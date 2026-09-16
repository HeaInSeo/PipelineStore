package pipelinestore_test

import (
	"strings"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
)

// The baseline valid pipeline must validate cleanly (with tool_profile_digest
// ABSENT — which never selects a "latest" profile).
func TestValid_Baseline(t *testing.T) {
	if err := validateBody(t, validPipelineJSON); err != nil {
		t.Fatalf("baseline pipeline should be valid, got %v", err)
	}
}

// A valid pipeline whose single required input is satisfied by an external slot.
func TestValid_SlotProvider(t *testing.T) {
	body := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"s","tool_function_cas_hash":"cas-slot","fixed_parameters":[]}],
      "direct_edges":[],
      "reusable_asset_bindings":[],
      "external_input_slots":[{"slot_id":"in0","to_node_id":"s","to_input_port":"in"}]}`
	if err := validateBody(t, body); err != nil {
		t.Fatalf("slot-provided pipeline should be valid, got %v", err)
	}
}

// A valid pipeline whose required fasta input is satisfied by a reusable asset.
func TestValid_AssetBinding(t *testing.T) {
	body := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"r","tool_function_cas_hash":"cas-ref","fixed_parameters":[]}],
      "direct_edges":[],
      "reusable_asset_bindings":[{"to_node_id":"r","to_input_port":"ref","asset_id":"asset1","asset_revision_id":"rev1","member_key":"mem1"}],
      "external_input_slots":[]}`
	if err := validateBody(t, body); err != nil {
		t.Fatalf("asset-bound pipeline should be valid, got %v", err)
	}
}

// T07: duplicate node / provider / parameter -> reject.
func TestT07_Duplicates(t *testing.T) {
	dupNode := strings.Replace(validPipelineJSON,
		`{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}`,
		`{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]},{"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]}`, 1)
	wantCode(t, validateBody(t, dupNode), ps.CodeDuplicate)

	dupParam := strings.Replace(validPipelineJSON,
		`"fixed_parameters":[{"name":"threads","value":"4"}]`,
		`"fixed_parameters":[{"name":"threads","value":"4"},{"name":"threads","value":"2"}]`, 1)
	wantCode(t, validateBody(t, dupParam), ps.CodeDuplicate)

	dupEdge := strings.Replace(validPipelineJSON,
		`"direct_edges":[
    {"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}
  ]`,
		`"direct_edges":[{"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"},{"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}]`, 1)
	wantCode(t, validateBody(t, dupEdge), ps.CodeDuplicate)
}

// Empty node id -> reject.
func TestValidate_EmptyNodeID(t *testing.T) {
	body := strings.Replace(validPipelineJSON, `"node_id":"a"`, `"node_id":""`, 1)
	wantCode(t, validateBody(t, body), ps.CodeInvalidContract)
}

// T08: dangling edge / cycle -> reject.
func TestT08_DanglingAndCycle(t *testing.T) {
	dangling := strings.Replace(validPipelineJSON,
		`{"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}`,
		`{"from_node_id":"ghost","from_output_port":"out","to_node_id":"b","to_input_port":"in"}`, 1)
	wantCode(t, validateBody(t, dangling), ps.CodeEdgeInvalid)

	cycle := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"p","tool_function_cas_hash":"cas-io","fixed_parameters":[]},{"node_id":"q","tool_function_cas_hash":"cas-io","fixed_parameters":[]}],
      "direct_edges":[
        {"from_node_id":"p","from_output_port":"out","to_node_id":"q","to_input_port":"in"},
        {"from_node_id":"q","from_output_port":"out","to_node_id":"p","to_input_port":"in"}
      ],
      "reusable_asset_bindings":[],"external_input_slots":[]}`
	wantCode(t, validateBody(t, cycle), ps.CodeGraphCycle)
}

// T09: Q16 format mismatch or required UNKNOWN -> reject.
func TestT09_Q16(t *testing.T) {
	// fastq output feeding a fasta input: implicit adapter required.
	mismatch := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]},{"node_id":"r","tool_function_cas_hash":"cas-ref","fixed_parameters":[]}],
      "direct_edges":[{"from_node_id":"a","from_output_port":"out","to_node_id":"r","to_input_port":"ref"}],
      "reusable_asset_bindings":[],"external_input_slots":[]}`
	wantCode(t, validateBody(t, mismatch), ps.CodeQ16Mismatch)

	// UNKNOWN output format feeding a known input.
	unknown := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"u","tool_function_cas_hash":"cas-unkout","fixed_parameters":[]},{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}],
      "direct_edges":[{"from_node_id":"u","from_output_port":"out","to_node_id":"b","to_input_port":"in"}],
      "reusable_asset_bindings":[],"external_input_slots":[]}`
	wantCode(t, validateBody(t, unknown), ps.CodeQ16Mismatch)
}

// T10: exact ToolFunction pin unresolved -> reject.
func TestT10_ToolFunctionUnresolved(t *testing.T) {
	missing := strings.Replace(validPipelineJSON, `"tool_function_cas_hash":"cas-b"`, `"tool_function_cas_hash":"cas-does-not-exist"`, 1)
	wantCode(t, validateBody(t, missing), ps.CodeToolFunctionUnresolved)

	empty := strings.Replace(validPipelineJSON, `"tool_function_cas_hash":"cas-b"`, `"tool_function_cas_hash":""`, 1)
	wantCode(t, validateBody(t, empty), ps.CodeToolFunctionUnresolved)
}

// Parameter not declared / invalid value -> reject.
func TestValidate_Parameters(t *testing.T) {
	undeclared := strings.Replace(validPipelineJSON,
		`"fixed_parameters":[{"name":"threads","value":"4"}]`,
		`"fixed_parameters":[{"name":"nonesuch","value":"4"}]`, 1)
	wantCode(t, validateBody(t, undeclared), ps.CodeParameterInvalid)

	badValue := strings.Replace(validPipelineJSON,
		`"fixed_parameters":[{"name":"threads","value":"4"}]`,
		`"fixed_parameters":[{"name":"threads","value":"7"}]`, 1)
	wantCode(t, validateBody(t, badValue), ps.CodeParameterInvalid)
}

// T11: PRESENT tool_profile_digest -> UNSUPPORTED_CAPABILITY(TP-R1/TP-R2);
// ABSENT is accepted and never selects latest.
func TestT11_ReservedToolProfile(t *testing.T) {
	present := strings.Replace(validPipelineJSON,
		`"node_id":"a","tool_function_cas_hash":"cas-a"`,
		`"node_id":"a","tool_function_cas_hash":"cas-a","tool_profile_digest":"sha256:x"`, 1)
	err := validateBody(t, present)
	wantCode(t, err, ps.CodeUnsupportedCapability)
	if !strings.Contains(err.Error(), "TP-R1/TP-R2") {
		t.Fatalf("expected TP-R1/TP-R2 detail, got %v", err)
	}

	// Even an empty-string reserved key is PRESENT and therefore rejected.
	presentEmpty := strings.Replace(validPipelineJSON,
		`"node_id":"a","tool_function_cas_hash":"cas-a"`,
		`"node_id":"a","tool_function_cas_hash":"cas-a","tool_profile_digest":""`, 1)
	wantCode(t, validateBody(t, presentEmpty), ps.CodeUnsupportedCapability)

	// ABSENT: accepted (baseline is valid, no "latest" selection happens).
	if err := validateBody(t, validPipelineJSON); err != nil {
		t.Fatalf("absent tool_profile_digest should be valid, got %v", err)
	}
}

// T12: exact Sori revision / member unresolved -> reject.
func TestT12_SoriUnresolved(t *testing.T) {
	body := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"r","tool_function_cas_hash":"cas-ref","fixed_parameters":[]}],
      "direct_edges":[],
      "reusable_asset_bindings":[{"to_node_id":"r","to_input_port":"ref","asset_id":"asset1","asset_revision_id":"revX","member_key":"mem1"}],
      "external_input_slots":[]}`
	wantCode(t, validateBody(t, body), ps.CodeAssetUnresolved)
}

// T16: unsupported semantic-derivation basis is not silently reinterpreted.
func TestT16_UnsupportedSemanticDerivation(t *testing.T) {
	body := strings.Replace(validPipelineJSON,
		`"semantic_derivation_version":"pipelinestore.pipeline-contract.v1"`,
		`"semantic_derivation_version":"pipelinestore.pipeline-contract.v2"`, 1)
	wantCode(t, validateBody(t, body), ps.CodeUnsupportedVersion)
}

// T17: a legacy v0.2 document fails the v1 gate.
func TestT17_LegacyV02Rejected(t *testing.T) {
	// A v0.2-shaped body: different version string and legacy keys.
	legacy := `{"schema":"certified-pipeline/v0.2","pipeline_id":"p","steps":[]}`
	_, err := ps.Parse([]byte(legacy))
	if err == nil {
		t.Fatal("legacy v0.2 document must not parse as a v1 contract")
	}
	// It fails as an unknown key ("schema") under strict v1 parsing.
	if c := ps.CodeOf(err); c != ps.CodeUnknownKey {
		t.Fatalf("expected UNKNOWN_KEY for legacy doc, got %v", err)
	}
}

// T20: two direct edges into one SINGLE input -> reject.
func TestT20_SingleInputConflict(t *testing.T) {
	body := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[
        {"node_id":"a1","tool_function_cas_hash":"cas-a","fixed_parameters":[]},
        {"node_id":"a2","tool_function_cas_hash":"cas-a","fixed_parameters":[]},
        {"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}
      ],
      "direct_edges":[
        {"from_node_id":"a1","from_output_port":"out","to_node_id":"b","to_input_port":"in"},
        {"from_node_id":"a2","from_output_port":"out","to_node_id":"b","to_input_port":"in"}
      ],
      "reusable_asset_bindings":[],"external_input_slots":[]}`
	wantCode(t, validateBody(t, body), ps.CodeProviderConflict)
}

// T21: optional-but-bound input with Q16 mismatch -> reject.
func TestT21_OptionalButBoundQ16(t *testing.T) {
	// cas-opt's optional "opt" input is fasta; bind a bam asset member.
	body := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"o","tool_function_cas_hash":"cas-opt","fixed_parameters":[]}],
      "direct_edges":[],
      "reusable_asset_bindings":[{"to_node_id":"o","to_input_port":"opt","asset_id":"assetbad","asset_revision_id":"rev1","member_key":"mem1"}],
      "external_input_slots":[]}`
	wantCode(t, validateBody(t, body), ps.CodeQ16Mismatch)
}

// T22: MULTIPLE / out-of-profile cardinality -> reject.
func TestT22_CardinalityUnsupported(t *testing.T) {
	body := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]},{"node_id":"m","tool_function_cas_hash":"cas-multi","fixed_parameters":[]}],
      "direct_edges":[{"from_node_id":"a","from_output_port":"out","to_node_id":"m","to_input_port":"in"}],
      "reusable_asset_bindings":[],"external_input_slots":[]}`
	wantCode(t, validateBody(t, body), ps.CodeCardinalityUnsupported)
}

// Finding 1 regression: a reusable_asset_binding whose RESOLVED Sori member
// cardinality is non-SINGLE (MULTIPLE here) is rejected through the v1
// capability gate, even though the TARGET INPUT is SINGLE and the Q16 formats
// match. The SINGLE-member case still validates cleanly.
func TestReusableAssetMemberCardinalityGated(t *testing.T) {
	// Target input cas-ref/"ref" is fasta + SINGLE + required; asset member
	// "assetmulti" is fasta (Q16 ok) but resolved cardinality MULTIPLE.
	multi := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"r","tool_function_cas_hash":"cas-ref","fixed_parameters":[]}],
      "direct_edges":[],
      "reusable_asset_bindings":[{"to_node_id":"r","to_input_port":"ref","asset_id":"assetmulti","asset_revision_id":"rev1","member_key":"mem1"}],
      "external_input_slots":[]}`
	wantCode(t, validateBody(t, multi), ps.CodeCardinalityUnsupported)

	// The SINGLE-member equivalent (asset1) still passes.
	single := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"r","tool_function_cas_hash":"cas-ref","fixed_parameters":[]}],
      "direct_edges":[],
      "reusable_asset_bindings":[{"to_node_id":"r","to_input_port":"ref","asset_id":"asset1","asset_revision_id":"rev1","member_key":"mem1"}],
      "external_input_slots":[]}`
	if err := validateBody(t, single); err != nil {
		t.Fatalf("SINGLE resolved member cardinality should validate, got %v", err)
	}
}

// T23: duplicate slot_id or a slot reused for multiple targets -> reject.
func TestT23_SlotViolations(t *testing.T) {
	// Same slot_id reused for two different targets.
	reused := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"s","tool_function_cas_hash":"cas-slot","fixed_parameters":[]},{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}],
      "direct_edges":[],
      "reusable_asset_bindings":[],
      "external_input_slots":[
        {"slot_id":"dup","to_node_id":"s","to_input_port":"in"},
        {"slot_id":"dup","to_node_id":"b","to_input_port":"in"}
      ]}`
	wantCode(t, validateBody(t, reused), ps.CodeSlotInvalid)
}

// T26: unsupported canonicalization version on commit -> fail closed.
func TestT26_UnsupportedCanonicalizationVersion(t *testing.T) {
	body := strings.Replace(validPipelineJSON,
		`"canonicalization_version":"pipelinestore.pipeline-contract.v1"`,
		`"canonicalization_version":"pipelinestore.canon.v2"`, 1)
	wantCode(t, validateBody(t, body), ps.CodeUnsupportedVersion)
}

// A required input with zero providers -> reject.
func TestValidate_RequiredInputNoProvider(t *testing.T) {
	body := `{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}],
      "direct_edges":[],"reusable_asset_bindings":[],"external_input_slots":[]}`
	wantCode(t, validateBody(t, body), ps.CodeProviderMissing)
}
