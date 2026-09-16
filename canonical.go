package pipelinestore

import (
	"bytes"
	"encoding/json"
	"sort"
)

// Canonicalize renders the deterministic canonical JSON (canonical_json_v1) of a
// contract. It is the sole owner of PipelineStore canonicalization.
//
// Rules: canonical object-key order; no insignificant whitespace; arrays sorted
// by their canonical sort keys; parameter/provider strings preserved exactly with
// no Unicode normalization, case-folding, or trimming. The reserved (and, in v1,
// always-absent) tool_profile_digest key is never emitted.
//
// Canonicalize assumes structural well-formedness only; semantic validation is a
// separate concern (see Validate). It is a pure function of the contract value.
func Canonicalize(c *PipelineContract) []byte {
	var b bytes.Buffer
	b.WriteByte('{')

	writeStringField(&b, "semantic_derivation_version", c.SemanticDerivationVersion)
	b.WriteByte(',')
	writeStringField(&b, "canonicalization_version", c.CanonicalizationVersion)
	b.WriteByte(',')

	// nodes: sort by node_id.
	nodes := append([]Node(nil), c.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	b.WriteString(`"nodes":[`)
	for i := range nodes {
		if i > 0 {
			b.WriteByte(',')
		}
		writeNode(&b, nodes[i])
	}
	b.WriteString("],")

	// direct_edges: sort by (from_node_id, from_output_port, to_node_id, to_input_port).
	edges := append([]DirectEdge(nil), c.DirectEdges...)
	sort.Slice(edges, func(i, j int) bool {
		return lessKeys(
			[]string{edges[i].FromNodeID, edges[i].FromOutputPort, edges[i].ToNodeID, edges[i].ToInputPort},
			[]string{edges[j].FromNodeID, edges[j].FromOutputPort, edges[j].ToNodeID, edges[j].ToInputPort},
		)
	})
	b.WriteString(`"direct_edges":[`)
	for i := range edges {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		writeStringField(&b, "from_node_id", edges[i].FromNodeID)
		b.WriteByte(',')
		writeStringField(&b, "from_output_port", edges[i].FromOutputPort)
		b.WriteByte(',')
		writeStringField(&b, "to_node_id", edges[i].ToNodeID)
		b.WriteByte(',')
		writeStringField(&b, "to_input_port", edges[i].ToInputPort)
		b.WriteByte('}')
	}
	b.WriteString("],")

	// reusable_asset_bindings: sort by target (to_node_id, to_input_port) then
	// (asset_id, asset_revision_id, member_key).
	bindings := append([]ReusableAssetBinding(nil), c.ReusableAssetBindings...)
	sort.Slice(bindings, func(i, j int) bool {
		return lessKeys(
			[]string{bindings[i].ToNodeID, bindings[i].ToInputPort, bindings[i].AssetID, bindings[i].AssetRevisionID, bindings[i].MemberKey},
			[]string{bindings[j].ToNodeID, bindings[j].ToInputPort, bindings[j].AssetID, bindings[j].AssetRevisionID, bindings[j].MemberKey},
		)
	})
	b.WriteString(`"reusable_asset_bindings":[`)
	for i := range bindings {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		writeStringField(&b, "to_node_id", bindings[i].ToNodeID)
		b.WriteByte(',')
		writeStringField(&b, "to_input_port", bindings[i].ToInputPort)
		b.WriteByte(',')
		writeStringField(&b, "asset_id", bindings[i].AssetID)
		b.WriteByte(',')
		writeStringField(&b, "asset_revision_id", bindings[i].AssetRevisionID)
		b.WriteByte(',')
		writeStringField(&b, "member_key", bindings[i].MemberKey)
		b.WriteByte('}')
	}
	b.WriteString("],")

	// external_input_slots: sort by (slot_id, to_node_id, to_input_port).
	slots := append([]ExternalInputSlot(nil), c.ExternalInputSlots...)
	sort.Slice(slots, func(i, j int) bool {
		return lessKeys(
			[]string{slots[i].SlotID, slots[i].ToNodeID, slots[i].ToInputPort},
			[]string{slots[j].SlotID, slots[j].ToNodeID, slots[j].ToInputPort},
		)
	})
	b.WriteString(`"external_input_slots":[`)
	for i := range slots {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		writeStringField(&b, "slot_id", slots[i].SlotID)
		b.WriteByte(',')
		writeStringField(&b, "to_node_id", slots[i].ToNodeID)
		b.WriteByte(',')
		writeStringField(&b, "to_input_port", slots[i].ToInputPort)
		b.WriteByte('}')
	}
	b.WriteString("]")

	b.WriteByte('}')
	return b.Bytes()
}

func writeNode(b *bytes.Buffer, n Node) {
	b.WriteByte('{')
	writeStringField(b, "node_id", n.NodeID)
	b.WriteByte(',')
	writeStringField(b, "tool_function_cas_hash", n.ToolFunctionCASHash)
	b.WriteByte(',')
	// fixed_parameters: sort by name.
	params := append([]FixedParameter(nil), n.FixedParameters...)
	sort.Slice(params, func(i, j int) bool { return params[i].Name < params[j].Name })
	b.WriteString(`"fixed_parameters":[`)
	for i := range params {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		writeStringField(b, "name", params[i].Name)
		b.WriteByte(',')
		writeStringField(b, "value", params[i].Value)
		b.WriteByte('}')
	}
	b.WriteString("]")
	b.WriteByte('}')
}

// writeStringField writes `"key":<canonical-json-string>` with no whitespace.
func writeStringField(b *bytes.Buffer, key, value string) {
	b.Write(jsonString(key))
	b.WriteByte(':')
	b.Write(jsonString(value))
}

// jsonString encodes s as a JSON string with HTML escaping disabled so that the
// output is a faithful, deterministic encoding of the exact code points. Distinct
// byte sequences (NFC vs NFD) therefore encode to distinct output.
func jsonString(s string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	// Encode cannot fail for a Go string; ignore error deliberately.
	_ = enc.Encode(s)
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// lessKeys compares two equal-length string tuples lexicographically.
func lessKeys(a, b []string) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
