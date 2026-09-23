package lowering

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"time"

	ps "github.com/HeaInSeo/PipelineStore"
)

const (
	// CodeMissingFrozenInput is an empty required caller-frozen input.
	CodeMissingFrozenInput ps.Code = "MISSING_FROZEN_INPUT"
	// CodeRunnableUnresolved is a missing or non-exact runnable resolution
	// (image not pinned by sha256 digest) for a node's ToolFunction pin.
	CodeRunnableUnresolved ps.Code = "RUNNABLE_UNRESOLVED"
	// CodeAuthorizationUnknown is an absent or unrecognized Authorization decision.
	CodeAuthorizationUnknown ps.Code = "AUTHORIZATION_UNKNOWN"
	// CodeAuthorizationDenied is an explicit DENY Authorization decision.
	CodeAuthorizationDenied ps.Code = "AUTHORIZATION_DENIED"
	// CodeNotRepresentable is a contract feature this core cannot lower without
	// an undecided argv-rendering or physical-materialization contract.
	CodeNotRepresentable ps.Code = "NOT_REPRESENTABLE"
	// CodeSameSourceViolation is a RunSpec whose edges and artifact bindings do
	// not derive from one consistent source.
	CodeSameSourceViolation ps.Code = "SAME_SOURCE_VIOLATION"
)

// Decision is a frozen Authorization decision. The zero value is UNKNOWN and
// fails closed.
type Decision string

const (
	// DecisionUnknown is the zero value: no decision was supplied.
	DecisionUnknown Decision = ""
	// DecisionAllow permits lowering.
	DecisionAllow Decision = "ALLOW"
	// DecisionDeny forbids lowering.
	DecisionDeny Decision = "DENY"
)

// FrozenMetadata is Run metadata frozen by the caller. The core never reads the
// clock or generates a trace or requester identity; empty RequesterID and
// TraceID stay empty.
type FrozenMetadata struct {
	SubmittedAt time.Time
	RequesterID string
	TraceID     string
}

// Runnable is the exact runnable resolution of one ToolFunction pin.
type Runnable struct {
	// Image must be pinned by digest: <name>@sha256:<64 lowercase hex>.
	Image   string
	Command []string
	Args    []string
}

// Input is the complete frozen input of one lowering.
type Input struct {
	// RunID is the externally allocated RunID already attached to the
	// originating intent. It is never minted here.
	RunID string
	// Revision is the exact committed revision, as returned by an exact read.
	Revision ps.PipelineRevision
	// Metadata is the caller-frozen Run metadata.
	Metadata FrozenMetadata
	// ToolFunctions maps each exact tool_function_cas_hash to its declaration.
	ToolFunctions map[string]ps.ToolFunctionDecl
	// Runnables maps each exact tool_function_cas_hash to its runnable resolution.
	Runnables map[string]Runnable
	// Authorization is the frozen Authorization decision for this Run.
	Authorization Decision
}

var pinnedImage = regexp.MustCompile(`^[^@\s]+@sha256:[0-9a-f]{64}$`)

// Lower deterministically lowers in into a RunSpec. On any failure it returns a
// *ps.Error with a stable Code and a zero RunSpec. It never mutates in.
func Lower(in Input) (RunSpec, error) {
	if in.RunID == "" {
		return RunSpec{}, missing("RunID")
	}
	if in.Metadata.SubmittedAt.IsZero() {
		return RunSpec{}, missing("Metadata.SubmittedAt")
	}
	c, err := verifyRevision(in.Revision)
	if err != nil {
		return RunSpec{}, err
	}
	if err := checkProfile(c); err != nil {
		return RunSpec{}, err
	}
	switch in.Authorization {
	case DecisionAllow:
	case DecisionDeny:
		return RunSpec{}, newError(CodeAuthorizationDenied, "authorization denied for run %q", in.RunID)
	default:
		return RunSpec{}, newError(CodeAuthorizationUnknown, "authorization decision %q is not ALLOW or DENY", string(in.Authorization))
	}

	// Nodes in node_id order; each resolved against its exact pin.
	contractNodes := append([]ps.Node(nil), c.Nodes...)
	sort.Slice(contractNodes, func(i, j int) bool { return contractNodes[i].NodeID < contractNodes[j].NodeID })
	decls := make(map[string]ps.ToolFunctionDecl, len(contractNodes))
	nodes := make([]Node, 0, len(contractNodes))
	index := make(map[string]int, len(contractNodes))
	for _, n := range contractNodes {
		if n.NodeID == "" {
			return RunSpec{}, newError(ps.CodeInvalidContract, "node has empty node_id")
		}
		if _, dup := index[n.NodeID]; dup {
			return RunSpec{}, newError(ps.CodeDuplicate, "duplicate node_id %q", n.NodeID)
		}
		decl, ok := in.ToolFunctions[n.ToolFunctionCASHash]
		if !ok {
			return RunSpec{}, newError(ps.CodeToolFunctionUnresolved, "node %q: no frozen declaration for exact pin %q", n.NodeID, n.ToolFunctionCASHash)
		}
		run, ok := in.Runnables[n.ToolFunctionCASHash]
		if !ok {
			return RunSpec{}, newError(CodeRunnableUnresolved, "node %q: no runnable resolution for exact pin %q", n.NodeID, n.ToolFunctionCASHash)
		}
		if !pinnedImage.MatchString(run.Image) {
			return RunSpec{}, newError(CodeRunnableUnresolved, "node %q: image %q is not pinned by sha256 digest", n.NodeID, run.Image)
		}
		decls[n.NodeID] = decl
		index[n.NodeID] = len(nodes)
		nodes = append(nodes, Node{
			NodeID:  n.NodeID,
			Image:   run.Image,
			Command: cloneStrings(run.Command),
			Args:    cloneStrings(run.Args),
			Inputs:  sortedKeys(decl.Inputs),
			Outputs: sortedKeys(decl.Outputs),
		})
	}

	// Each DirectEdge is the single source of both the dependency edge and the
	// consumer binding.
	contractEdges := append([]ps.DirectEdge(nil), c.DirectEdges...)
	sort.Slice(contractEdges, func(i, j int) bool { return edgeLess(contractEdges[i], contractEdges[j]) })
	type port struct{ node, name string }
	bound := map[port]bool{}
	consumed := map[port]bool{}
	edgeSeen := map[[2]string]bool{}
	var edges [][]string
	for _, e := range contractEdges {
		fromDecl, ok := decls[e.FromNodeID]
		if !ok {
			return RunSpec{}, newError(ps.CodeEdgeInvalid, "edge references unknown from_node_id %q", e.FromNodeID)
		}
		toDecl, ok := decls[e.ToNodeID]
		if !ok {
			return RunSpec{}, newError(ps.CodeEdgeInvalid, "edge references unknown to_node_id %q", e.ToNodeID)
		}
		out, ok := fromDecl.Outputs[e.FromOutputPort]
		if !ok {
			return RunSpec{}, newError(ps.CodeEdgeInvalid, "node %q has no output port %q", e.FromNodeID, e.FromOutputPort)
		}
		inDecl, ok := toDecl.Inputs[e.ToInputPort]
		if !ok {
			return RunSpec{}, newError(ps.CodeEdgeInvalid, "node %q has no input port %q", e.ToNodeID, e.ToInputPort)
		}
		if out.Cardinality != ps.CardinalitySingle || inDecl.Cardinality != ps.CardinalitySingle {
			return RunSpec{}, newError(ps.CodeCardinalityUnsupported, "edge %s.%s -> %s.%s is not SINGLE to SINGLE", e.FromNodeID, e.FromOutputPort, e.ToNodeID, e.ToInputPort)
		}
		src := port{e.FromNodeID, e.FromOutputPort}
		if consumed[src] {
			return RunSpec{}, newError(CodeNotRepresentable, "output %s.%s fans out to more than one input", e.FromNodeID, e.FromOutputPort)
		}
		consumed[src] = true
		dst := port{e.ToNodeID, e.ToInputPort}
		if bound[dst] {
			return RunSpec{}, newError(ps.CodeProviderConflict, "input %s.%s has more than one provider", e.ToNodeID, e.ToInputPort)
		}
		bound[dst] = true

		consumer := &nodes[index[e.ToNodeID]]
		consumer.ArtifactBindings = append(consumer.ArtifactBindings, ArtifactBinding{
			BindingName:        e.ToInputPort,
			ChildInputName:     e.ToInputPort,
			ProducerNodeID:     e.FromNodeID,
			ProducerOutputName: e.FromOutputPort,
			Required:           inDecl.Required,
		})
		pair := [2]string{e.FromNodeID, e.ToNodeID}
		if !edgeSeen[pair] {
			edgeSeen[pair] = true
			edges = append(edges, []string{e.FromNodeID, e.ToNodeID})
		}
	}

	// Every required input must be bound by an edge.
	for _, n := range nodes {
		for _, name := range n.Inputs {
			if decls[n.NodeID].Inputs[name].Required && !bound[port{n.NodeID, name}] {
				return RunSpec{}, newError(ps.CodeProviderMissing, "required input %s.%s has no provider", n.NodeID, name)
			}
		}
	}

	spec := RunSpec{
		Run: RunMetadata{
			RunID:         in.RunID,
			SubmittedAt:   in.Metadata.SubmittedAt.Round(0),
			FailurePolicy: FailurePolicy{Mode: FailureModeFailFast},
			RequesterID:   in.Metadata.RequesterID,
			TraceID:       in.Metadata.TraceID,
		},
		Graph:    Graph{Nodes: nodes, Edges: edges},
		Defaults: Defaults{RetryPolicy: RetryPolicy{MaxAttempts: MaxAttemptsV1}},
	}
	if spec.Graph.Edges == nil {
		spec.Graph.Edges = [][]string{}
	}
	if err := CheckSpec(spec); err != nil {
		return RunSpec{}, err
	}
	return spec, nil
}

// verifyRevision fails closed unless rev is an exact v1 revision whose decoded
// contract re-canonicalizes to the stored canonical body and digest.
func verifyRevision(rev ps.PipelineRevision) (*ps.PipelineContract, error) {
	if rev.PipelineID == "" {
		return nil, missing("Revision.PipelineID")
	}
	if rev.RevisionID == "" {
		return nil, missing("Revision.RevisionID")
	}
	c := rev.Contract
	if c == nil {
		return nil, newError(ps.CodeIntegrity, "revision %q carries no decoded contract", rev.RevisionID)
	}
	for _, v := range []string{rev.SemanticDerivationVersion, rev.CanonicalizationVersion, c.SemanticDerivationVersion, c.CanonicalizationVersion} {
		if v != ps.ContractVersionV1 {
			return nil, newError(ps.CodeUnsupportedVersion, "revision %q has version %q (only %q is supported)", rev.RevisionID, v, ps.ContractVersionV1)
		}
	}
	canon := ps.Canonicalize(c)
	if !bytes.Equal(canon, rev.CanonicalBody) {
		return nil, newError(ps.CodeIntegrity, "revision %q decoded contract does not match its canonical body", rev.RevisionID)
	}
	if ps.DigestCanonical(canon) != rev.ContractDigest {
		return nil, newError(ps.CodeIntegrity, "revision %q canonical body does not match its contract digest", rev.RevisionID)
	}
	return c, nil
}

// checkProfile rejects contract features outside the first lowering slice.
func checkProfile(c *ps.PipelineContract) error {
	for _, n := range c.Nodes {
		if n.ToolProfileDigest != nil {
			return newError(ps.CodeUnsupportedCapability, "node %q sets reserved tool_profile_digest", n.NodeID)
		}
		if len(n.FixedParameters) > 0 {
			return newError(CodeNotRepresentable, "node %q fixed parameters need an argv rendering contract", n.NodeID)
		}
	}
	if len(c.ReusableAssetBindings) > 0 {
		return newError(CodeNotRepresentable, "reusable asset bindings need a physical materialization contract")
	}
	if len(c.ExternalInputSlots) > 0 {
		return newError(CodeNotRepresentable, "external input slots need a physical materialization contract")
	}
	return nil
}

func edgeLess(a, b ps.DirectEdge) bool {
	ka := [4]string{a.FromNodeID, a.FromOutputPort, a.ToNodeID, a.ToInputPort}
	kb := [4]string{b.FromNodeID, b.FromOutputPort, b.ToNodeID, b.ToInputPort}
	for i := range ka {
		if ka[i] != kb[i] {
			return ka[i] < kb[i]
		}
	}
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// cloneStrings returns a non-nil copy so the output never aliases the input and
// always encodes as a JSON array.
func cloneStrings(s []string) []string {
	return append([]string{}, s...)
}

func newError(code ps.Code, format string, args ...any) error {
	return &ps.Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func missing(field string) error {
	return newError(CodeMissingFrozenInput, "required frozen input %s is empty", field)
}
