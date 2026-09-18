package pipelinestore

import (
	"context"
	"fmt"
)

// portRef identifies a node input/output port.
type portRef struct {
	node string
	port string
}

// Validate performs full commit validation of a parsed contract using the
// read-only resolvers. It returns a non-nil *Error (with a stable Code) on the
// first violation and must be run BEFORE any persistence.
//
// It never mutates the contract, the resolvers, or any external state.
func Validate(ctx context.Context, c *PipelineContract, res Resolvers) error {
	// 1. Version gate: commit accepts ONLY the v1 identifier; fail closed otherwise.
	if c.CanonicalizationVersion != ContractVersionV1 {
		return newErr(CodeUnsupportedVersion, "unsupported canonicalization_version %q (only %q is supported)", c.CanonicalizationVersion, ContractVersionV1)
	}
	if c.SemanticDerivationVersion != ContractVersionV1 {
		return newErr(CodeUnsupportedVersion, "unsupported semantic_derivation_version %q (only %q is supported)", c.SemanticDerivationVersion, ContractVersionV1)
	}

	// 2. Nodes: identity, reserved-capability gate, pin resolution, parameters.
	decls := make(map[string]ToolFunctionDecl, len(c.Nodes))
	nodeSeen := make(map[string]struct{}, len(c.Nodes))
	for i := range c.Nodes {
		n := c.Nodes[i]
		if n.NodeID == "" {
			return newErr(CodeInvalidContract, "node[%d] has empty node_id", i)
		}
		if _, dup := nodeSeen[n.NodeID]; dup {
			return newErrDetail(CodeDuplicate, "node_id "+n.NodeID, "duplicate node id")
		}
		nodeSeen[n.NodeID] = struct{}{}

		// Reserved, inactive capability: PRESENT tool_profile_digest is a distinct
		// unsupported-capability class (TP-R1/TP-R2), never a generic unknown key
		// and never used to select "latest".
		if n.ToolProfileDigest != nil {
			return newErrDetail(CodeUnsupportedCapability, "TP-R1/TP-R2",
				"node %q sets reserved tool_profile_digest; ToolProfile pinning is not an active v1 capability", n.NodeID)
		}

		if n.ToolFunctionCASHash == "" {
			return newErr(CodeToolFunctionUnresolved, "node %q missing tool_function_cas_hash", n.NodeID)
		}
		decl, err := res.ToolFunction.ResolveToolFunction(ctx, n.ToolFunctionCASHash)
		if err != nil {
			return newErr(CodeToolFunctionUnresolved, "node %q: exact ToolFunction pin %q unresolvable: %v", n.NodeID, n.ToolFunctionCASHash, err)
		}
		decls[n.NodeID] = decl

		paramSeen := make(map[string]struct{}, len(n.FixedParameters))
		for _, p := range n.FixedParameters {
			if _, dup := paramSeen[p.Name]; dup {
				return newErrDetail(CodeDuplicate, "parameter "+p.Name, "node %q duplicate fixed parameter", n.NodeID)
			}
			paramSeen[p.Name] = struct{}{}
			pd, ok := decl.Parameters[p.Name]
			if !ok {
				return newErr(CodeParameterInvalid, "node %q fixed parameter %q is not declared by the pinned ToolFunction", n.NodeID, p.Name)
			}
			if !parameterValueValid(pd, p.Value) {
				return newErr(CodeParameterInvalid, "node %q fixed parameter %q has value invalid under the pinned ToolFunction declaration", n.NodeID, p.Name)
			}
		}
	}

	// providers tracks how many providers target each input port (direct edges +
	// reusable asset bindings + external input slots).
	providers := map[portRef]int{}

	// 3. Direct edges: dangling/duplicate checks + Q16 + cardinality.
	edgeSeen := map[DirectEdge]struct{}{}
	for _, e := range c.DirectEdges {
		if _, dup := edgeSeen[e]; dup {
			return newErrDetail(CodeDuplicate, "direct_edge", "duplicate direct edge %s.%s -> %s.%s", e.FromNodeID, e.FromOutputPort, e.ToNodeID, e.ToInputPort)
		}
		edgeSeen[e] = struct{}{}

		fromDecl, ok := decls[e.FromNodeID]
		if !ok {
			return newErr(CodeEdgeInvalid, "direct edge references unknown from_node_id %q", e.FromNodeID)
		}
		toDecl, ok := decls[e.ToNodeID]
		if !ok {
			return newErr(CodeEdgeInvalid, "direct edge references unknown to_node_id %q", e.ToNodeID)
		}
		out, ok := fromDecl.Outputs[e.FromOutputPort]
		if !ok {
			return newErr(CodeEdgeInvalid, "node %q has no output port %q", e.FromNodeID, e.FromOutputPort)
		}
		in, ok := toDecl.Inputs[e.ToInputPort]
		if !ok {
			return newErr(CodeEdgeInvalid, "node %q has no input port %q", e.ToNodeID, e.ToInputPort)
		}
		if err := checkCardinality(out.Cardinality, fmt.Sprintf("output %s.%s", e.FromNodeID, e.FromOutputPort)); err != nil {
			return err
		}
		if err := checkCardinality(in.Cardinality, fmt.Sprintf("input %s.%s", e.ToNodeID, e.ToInputPort)); err != nil {
			return err
		}
		if err := checkQ16(out.DataFormat, in.DataFormat, fmt.Sprintf("%s.%s -> %s.%s", e.FromNodeID, e.FromOutputPort, e.ToNodeID, e.ToInputPort)); err != nil {
			return err
		}
		providers[portRef{e.ToNodeID, e.ToInputPort}]++
	}

	// 4. Reusable asset bindings: dangling/duplicate + exact Sori verification +
	// Q16 + cardinality.
	bindSeen := map[ReusableAssetBinding]struct{}{}
	for _, bnd := range c.ReusableAssetBindings {
		if _, dup := bindSeen[bnd]; dup {
			return newErrDetail(CodeDuplicate, "reusable_asset_binding", "duplicate reusable asset binding into %s.%s", bnd.ToNodeID, bnd.ToInputPort)
		}
		bindSeen[bnd] = struct{}{}

		toDecl, ok := decls[bnd.ToNodeID]
		if !ok {
			return newErr(CodeEdgeInvalid, "reusable asset binding references unknown to_node_id %q", bnd.ToNodeID)
		}
		in, ok := toDecl.Inputs[bnd.ToInputPort]
		if !ok {
			return newErr(CodeEdgeInvalid, "node %q has no input port %q (reusable asset binding)", bnd.ToNodeID, bnd.ToInputPort)
		}
		member, err := res.Sori.VerifyAssetMember(ctx, bnd.AssetID, bnd.AssetRevisionID, bnd.MemberKey)
		if err != nil {
			return newErr(CodeAssetUnresolved, "reusable asset %q rev %q member %q unverifiable: %v", bnd.AssetID, bnd.AssetRevisionID, bnd.MemberKey, err)
		}
		// Gate the RESOLVED asset member's cardinality through the v1 capability
		// gate, not only the target input's: v1 supports ONLY SINGLE. A MULTIPLE/
		// COMPOSITE/SCATTER/UNSPECIFIED resolved member is out of profile and is
		// rejected before persistence (fail closed).
		if err := checkCardinality(member.Cardinality, fmt.Sprintf("asset %s rev %s member %s", bnd.AssetID, bnd.AssetRevisionID, bnd.MemberKey)); err != nil {
			return err
		}
		if err := checkCardinality(in.Cardinality, fmt.Sprintf("input %s.%s", bnd.ToNodeID, bnd.ToInputPort)); err != nil {
			return err
		}
		if err := checkQ16(member.DataFormat, in.DataFormat, fmt.Sprintf("asset %s -> %s.%s", bnd.AssetID, bnd.ToNodeID, bnd.ToInputPort)); err != nil {
			return err
		}
		providers[portRef{bnd.ToNodeID, bnd.ToInputPort}]++
	}

	// 5. External input slots: unique slot_id, single target per slot, valid target.
	slotTarget := map[string]portRef{}
	for _, s := range c.ExternalInputSlots {
		if s.SlotID == "" {
			return newErr(CodeSlotInvalid, "external input slot has empty slot_id")
		}
		target := portRef{s.ToNodeID, s.ToInputPort}
		if prev, ok := slotTarget[s.SlotID]; ok {
			if prev != target {
				return newErrDetail(CodeSlotInvalid, "slot_id "+s.SlotID, "slot_id reused for multiple targets")
			}
			return newErrDetail(CodeSlotInvalid, "slot_id "+s.SlotID, "duplicate slot_id")
		}
		slotTarget[s.SlotID] = target

		toDecl, ok := decls[s.ToNodeID]
		if !ok {
			return newErr(CodeEdgeInvalid, "external input slot references unknown to_node_id %q", s.ToNodeID)
		}
		in, ok := toDecl.Inputs[s.ToInputPort]
		if !ok {
			return newErr(CodeEdgeInvalid, "node %q has no input port %q (external input slot)", s.ToNodeID, s.ToInputPort)
		}
		if err := checkCardinality(in.Cardinality, fmt.Sprintf("input %s.%s", s.ToNodeID, s.ToInputPort)); err != nil {
			return err
		}
		providers[portRef{s.ToNodeID, s.ToInputPort}]++
	}

	// 6. Provider cardinality resolution.
	//    a) A SINGLE target input with >1 provider is a conflict.
	for ref, count := range providers {
		in := decls[ref.node].Inputs[ref.port]
		if in.Cardinality == CardinalitySingle && count > 1 {
			return newErr(CodeProviderConflict, "input %s.%s is SINGLE but has %d providers", ref.node, ref.port, count)
		}
	}
	//    b) A required input with zero providers is rejected.
	for nodeID, decl := range decls {
		for port, in := range decl.Inputs {
			if in.Required && providers[portRef{nodeID, port}] == 0 {
				return newErr(CodeProviderMissing, "required input %s.%s has zero providers", nodeID, port)
			}
		}
	}

	// 7. Cycle detection over direct edges (node dependency graph).
	if node, ok := detectCycle(c.Nodes, c.DirectEdges); ok {
		return newErrDetail(CodeGraphCycle, "node "+node, "direct edges form a dependency cycle")
	}

	return nil
}

// parameterValueValid reports whether value is acceptable under the declaration.
// No coercion is performed: with a restricted set the value must match a member
// exactly; otherwise any string is accepted verbatim.
func parameterValueValid(pd ParameterDecl, value string) bool {
	if len(pd.AllowedValues) == 0 {
		return true
	}
	for _, a := range pd.AllowedValues {
		if a == value {
			return true
		}
	}
	return false
}

// checkCardinality rejects any cardinality other than SINGLE (the only active v1
// capability). UNSPECIFIED and MULTIPLE/composite/scatter/fan-out are out of
// profile.
func checkCardinality(card Cardinality, where string) error {
	if card == CardinalitySingle {
		return nil
	}
	return newErr(CodeCardinalityUnsupported, "%s has cardinality %q; only SINGLE is supported in v1", where, string(card))
}

// checkQ16 enforces L0 data-format compatibility: both formats must be known and
// exactly equal. A differing format would require an implicit adapter/transform,
// which is rejected.
func checkQ16(fromFormat, toFormat, where string) error {
	if isUnknownFormat(fromFormat) || isUnknownFormat(toFormat) {
		return newErr(CodeQ16Mismatch, "%s: data format is UNKNOWN/UNSPECIFIED", where)
	}
	if fromFormat != toFormat {
		return newErr(CodeQ16Mismatch, "%s: data format %q vs %q would require an implicit adapter/transform", where, fromFormat, toFormat)
	}
	return nil
}

// detectCycle returns a node participating in a cycle, if any, over the directed
// graph induced by direct edges.
func detectCycle(nodes []Node, edges []DirectEdge) (string, bool) {
	adj := map[string][]string{}
	for _, n := range nodes {
		adj[n.NodeID] = nil
	}
	for _, e := range edges {
		adj[e.FromNodeID] = append(adj[e.FromNodeID], e.ToNodeID)
	}
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var visit func(string) (string, bool)
	visit = func(u string) (string, bool) {
		color[u] = gray
		for _, v := range adj[u] {
			switch color[v] {
			case gray:
				return v, true
			case white:
				if node, ok := visit(v); ok {
					return node, true
				}
			}
		}
		color[u] = black
		return "", false
	}
	for _, n := range nodes {
		if color[n.NodeID] == white {
			if node, ok := visit(n.NodeID); ok {
				return node, true
			}
		}
	}
	return "", false
}
