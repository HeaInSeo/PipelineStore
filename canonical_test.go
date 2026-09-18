package pipelinestore_test

import (
	"strings"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
)

// T01: formatting / object-key-order invariance -> same digest.
func TestT01_FormattingAndKeyOrderInvariance(t *testing.T) {
	// Same content, different object-key order and whitespace.
	a := `{
      "canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "semantic_derivation_version":"pipelinestore.pipeline-contract.v1",
      "external_input_slots":[],
      "reusable_asset_bindings":[],
      "direct_edges":[{"to_input_port":"in","from_node_id":"a","to_node_id":"b","from_output_port":"out"}],
      "nodes":[
        {"fixed_parameters":[{"value":"4","name":"threads"}],"tool_function_cas_hash":"cas-a","node_id":"a"},
        {"tool_function_cas_hash":"cas-b","fixed_parameters":[],"node_id":"b"}
      ]
    }`
	b := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1","nodes":[{"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[{"name":"threads","value":"4"}]},{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}],"direct_edges":[{"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}],"reusable_asset_bindings":[],"external_input_slots":[]}`

	if mustDigest(t, a) != mustDigest(t, b) {
		t.Fatal("formatting/key-order variance changed the digest")
	}
}

// T02: node / edge / provider request-order invariance -> same digest.
func TestT02_ElementOrderInvariance(t *testing.T) {
	// Two producers into two distinct inputs, plus two slots; ordering scrambled.
	ordered := `{
      "semantic_derivation_version":"pipelinestore.pipeline-contract.v1",
      "canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[
        {"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]},
        {"node_id":"b","tool_function_cas_hash":"cas-io","fixed_parameters":[]},
        {"node_id":"c","tool_function_cas_hash":"cas-io","fixed_parameters":[]}
      ],
      "direct_edges":[
        {"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"},
        {"from_node_id":"b","from_output_port":"out","to_node_id":"c","to_input_port":"in"}
      ],
      "reusable_asset_bindings":[],
      "external_input_slots":[]
    }`
	scrambled := `{
      "semantic_derivation_version":"pipelinestore.pipeline-contract.v1",
      "canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[
        {"node_id":"c","tool_function_cas_hash":"cas-io","fixed_parameters":[]},
        {"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]},
        {"node_id":"b","tool_function_cas_hash":"cas-io","fixed_parameters":[]}
      ],
      "direct_edges":[
        {"from_node_id":"b","from_output_port":"out","to_node_id":"c","to_input_port":"in"},
        {"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}
      ],
      "reusable_asset_bindings":[],
      "external_input_slots":[]
    }`

	if mustDigest(t, ordered) != mustDigest(t, scrambled) {
		t.Fatal("element order variance changed the digest")
	}
}

// T03: a semantic change yields a different digest.
func TestT03_SemanticChangeDifferentDigest(t *testing.T) {
	base := mustDigest(t, validPipelineJSON)

	// Change a fixed-parameter value: semantic change.
	changed := strings.Replace(validPipelineJSON, `"value":"4"`, `"value":"2"`, 1)
	if mustDigest(t, changed) == base {
		t.Fatal("parameter value change did not change the digest")
	}

	// A reserved tool_profile_digest is NOT an active v1 capability: it must be
	// rejected at parse/validate (see T11), never able to alter the digest.
	withTP := strings.Replace(validPipelineJSON,
		`"tool_function_cas_hash":"cas-a","fixed_parameters":[{"name":"threads","value":"4"}]`,
		`"tool_function_cas_hash":"cas-a","tool_profile_digest":"sha256:deadbeef","fixed_parameters":[{"name":"threads","value":"4"}]`, 1)
	if err := validateBody(t, withTP); ps.CodeOf(err) != ps.CodeUnsupportedCapability {
		t.Fatalf("expected UNSUPPORTED_CAPABILITY for reserved tool_profile_digest, got %v", err)
	}
}

// T04 (content-digest half): same body under different PipelineID -> same content
// digest. (The distinct revision identity is asserted in the store tests.)
func TestT04_SameBodyContentDigest(t *testing.T) {
	d1 := mustDigest(t, validPipelineJSON)
	d2 := mustDigest(t, validPipelineJSON)
	if d1 != d2 {
		t.Fatal("identical bodies produced different content digests")
	}
}

// T18: presentation-like metadata cannot affect the digest, because there is no
// semantic-body surface for it: any such key is rejected as unknown.
func TestT18_NoPresentationSurface(t *testing.T) {
	withMeta := strings.Replace(validPipelineJSON,
		`"external_input_slots":[]`,
		`"external_input_slots":[],"display_label":"My Pipeline","color":"#fff"`, 1)
	if err := validateBody(t, withMeta); ps.CodeOf(err) != ps.CodeUnknownKey {
		t.Fatalf("expected UNKNOWN_KEY for presentation metadata, got %v", err)
	}
}

// T25: NFC vs NFD code-point-distinct strings are not normalized to equality.
func TestT25_NoUnicodeNormalization(t *testing.T) {
	// "café" with precomposed é (NFC) vs decomposed e + combining acute (NFD).
	nfc := "caf\u00e9"  // precomposed e-acute (NFC)
	nfd := "cafe\u0301" // e + combining acute (NFD)
	if nfc == nfd {
		t.Fatal("test setup error: NFC and NFD strings are byte-equal")
	}
	tmpl := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1","nodes":[{"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[{"name":"threads","value":"%s"}]}],"direct_edges":[],"reusable_asset_bindings":[],"external_input_slots":[]}`
	d1 := mustDigest(t, strings.Replace(tmpl, "%s", nfc, 1))
	d2 := mustDigest(t, strings.Replace(tmpl, "%s", nfd, 1))
	if d1 == d2 {
		t.Fatal("NFC and NFD forms were normalized to the same digest")
	}
}
