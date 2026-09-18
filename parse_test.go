package pipelinestore_test

import (
	"bytes"
	"strings"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
)

// T13: physical path / mount / PVC / locator fields cannot enter the v1 schema.
// They are unknown semantic-body keys and are rejected at parse.
func TestT13_PhysicalFieldsRejected(t *testing.T) {
	cases := map[string]string{
		"node mount_path": strings.Replace(validPipelineJSON,
			`"tool_function_cas_hash":"cas-b","fixed_parameters":[]`,
			`"tool_function_cas_hash":"cas-b","mount_path":"/data","fixed_parameters":[]`, 1),
		"node image_locator": strings.Replace(validPipelineJSON,
			`"tool_function_cas_hash":"cas-b","fixed_parameters":[]`,
			`"tool_function_cas_hash":"cas-b","image_locator":"harbor.local/x@sha256:ab","fixed_parameters":[]`, 1),
		"slot host_path": strings.Replace(validPipelineJSON,
			`"external_input_slots":[]`,
			`"external_input_slots":[{"slot_id":"s1","to_node_id":"b","to_input_port":"in","host_path":"/mnt/x"}]`, 1),
		"top-level pvc": strings.Replace(validPipelineJSON,
			`"external_input_slots":[]`,
			`"external_input_slots":[],"pvc_name":"vol0"`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ps.Parse([]byte(body))
			wantCode(t, err, ps.CodeUnknownKey)
		})
	}
}

// T24: unknown key or semantic null -> reject; and the reserved, PRESENT
// tool_profile_digest pin uses a DISTINCT unsupported-capability class (not the
// generic unknown-key class).
func TestT24_UnknownKeyNullAndReservedClass(t *testing.T) {
	// Unknown key.
	unknown := strings.Replace(validPipelineJSON,
		`"node_id":"b","tool_function_cas_hash":"cas-b"`,
		`"node_id":"b","surprise":true,"tool_function_cas_hash":"cas-b"`, 1)
	if _, err := ps.Parse([]byte(unknown)); ps.CodeOf(err) != ps.CodeUnknownKey {
		t.Fatalf("unknown key: expected UNKNOWN_KEY, got %v", err)
	}

	// Explicit semantic null (a field set to null).
	nullField := strings.Replace(validPipelineJSON,
		`"reusable_asset_bindings":[]`,
		`"reusable_asset_bindings":null`, 1)
	if _, err := ps.Parse([]byte(nullField)); ps.CodeOf(err) != ps.CodeSemanticNull {
		t.Fatalf("semantic null: expected SEMANTIC_NULL, got %v", err)
	}

	// Nested semantic null.
	nestedNull := strings.Replace(validPipelineJSON,
		`"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]`,
		`"node_id":"b","tool_function_cas_hash":null,"fixed_parameters":[]`, 1)
	if _, err := ps.Parse([]byte(nestedNull)); ps.CodeOf(err) != ps.CodeSemanticNull {
		t.Fatalf("nested semantic null: expected SEMANTIC_NULL, got %v", err)
	}

	// Reserved PRESENT tool_profile_digest -> distinct unsupported-capability class.
	// It parses (a known reserved key), then validation rejects it.
	withTP := strings.Replace(validPipelineJSON,
		`"node_id":"a","tool_function_cas_hash":"cas-a"`,
		`"node_id":"a","tool_function_cas_hash":"cas-a","tool_profile_digest":"sha256:abc"`, 1)
	if _, err := ps.Parse([]byte(withTP)); err != nil {
		t.Fatalf("reserved key should parse (known field), got parse error %v", err)
	}
	err := validateBody(t, withTP)
	wantCode(t, err, ps.CodeUnsupportedCapability)
	if !strings.Contains(err.Error(), "TP-R1/TP-R2") {
		t.Fatalf("expected TP-R1/TP-R2 detail, got %v", err)
	}
}

// Finding 3 regression: invalid UTF-8 in the contract bytes is rejected up front
// (fail closed) BEFORE decoding. Two DISTINCT malformed byte sequences must both
// be rejected — without the guard the JSON decoder would collapse each into the
// U+FFFD replacement character during string decoding, silently merging them and
// accepting the body.
func TestParse_InvalidUTF8Rejected(t *testing.T) {
	// A structurally valid contract whose node_id is a placeholder we replace
	// with raw invalid-UTF-8 bytes.
	base := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",` +
		`"nodes":[{"node_id":"XX","tool_function_cas_hash":"cas-a","fixed_parameters":[]}],` +
		`"direct_edges":[],"reusable_asset_bindings":[],"external_input_slots":[]}`

	// Two different malformed byte sequences (a lone 0xFF vs a lone 0x80). Both
	// would decode to the same replacement character if not rejected first.
	seq1 := bytes.Replace([]byte(base), []byte("XX"), []byte{'a', 0xff}, 1)
	seq2 := bytes.Replace([]byte(base), []byte("XX"), []byte{'a', 0x80}, 1)

	if _, err := ps.Parse(seq1); ps.CodeOf(err) != ps.CodeInvalidContract {
		t.Fatalf("seq1 (0xff): expected INVALID_CONTRACT, got %v", err)
	}
	if _, err := ps.Parse(seq2); ps.CodeOf(err) != ps.CodeInvalidContract {
		t.Fatalf("seq2 (0x80): expected INVALID_CONTRACT, got %v", err)
	}
}

// P2 regression: JSON *escaped* lone surrogates (e.g. \ud800 / \udc00) are
// rejected before decode. utf8.Valid on the raw bytes cannot catch these (the
// surrogate is an ASCII \u escape), and the strict JSON decoder would silently
// coerce each to U+FFFD — collapsing distinct malformed values. A valid
// high+low surrogate pair must still be accepted.
func TestParse_EscapedSurrogates(t *testing.T) {
	base := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",` +
		`"nodes":[{"node_id":"NODEID","tool_function_cas_hash":"cas-a","fixed_parameters":[]}],` +
		`"direct_edges":[],"reusable_asset_bindings":[],"external_input_slots":[]}`
	withNodeID := func(id string) string { return strings.Replace(base, "NODEID", id, 1) }

	// Lone high surrogate -> rejected.
	loneHigh := withNodeID(`\ud800`)
	if _, err := ps.Parse([]byte(loneHigh)); ps.CodeOf(err) != ps.CodeInvalidContract {
		t.Fatalf("lone high surrogate: expected INVALID_CONTRACT, got %v", err)
	}

	// Lone low surrogate -> rejected.
	loneLow := withNodeID(`\udc00`)
	if _, err := ps.Parse([]byte(loneLow)); ps.CodeOf(err) != ps.CodeInvalidContract {
		t.Fatalf("lone low surrogate: expected INVALID_CONTRACT, got %v", err)
	}

	// Two DISTINCT lone-surrogate values must both be rejected distinctly — they
	// must NOT collapse (to U+FFFD) into an accepted, converging body.
	if _, err := ps.Parse([]byte(loneHigh)); err == nil {
		t.Fatal("distinct lone high must be rejected, not collapsed")
	}
	errHigh := func() error { _, e := ps.Parse([]byte(loneHigh)); return e }()
	errLow := func() error { _, e := ps.Parse([]byte(loneLow)); return e }()
	if errHigh == nil || errLow == nil {
		t.Fatal("both distinct lone surrogates must be rejected")
	}
	if errHigh.Error() == errLow.Error() {
		t.Fatalf("distinct lone surrogates must not collapse to the same rejection: %q", errHigh.Error())
	}

	// Valid high+low surrogate pair (U+10000, escaped \ud800\udc00) -> accepted,
	// exercising the pairing-acceptance branch of the guard.
	pair := withNodeID(`\ud800\udc00`)
	c, err := ps.Parse([]byte(pair))
	if err != nil {
		t.Fatalf("valid surrogate pair must be accepted, got %v", err)
	}
	if len(c.Nodes) != 1 || c.Nodes[0].NodeID != "\U00010000" {
		t.Fatalf("valid surrogate pair must decode to U+10000, got %+v", c.Nodes)
	}
}

// Duplicate object keys are rejected (part of "duplicates REJECT").
func TestParse_DuplicateObjectKey(t *testing.T) {
	dup := strings.Replace(validPipelineJSON,
		`"canonicalization_version":"pipelinestore.pipeline-contract.v1"`,
		`"canonicalization_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1"`, 1)
	_, err := ps.Parse([]byte(dup))
	wantCode(t, err, ps.CodeDuplicate)
}
