package pipelinestore_test

import (
	"context"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
	"github.com/HeaInSeo/PipelineStore/fake"
)

// baseResolvers returns deterministic fakes covering every declaration the tests
// need. No network I/O of any kind.
func baseResolvers() ps.Resolvers {
	tf := fake.NewToolFunctionCatalog()
	// cas-a: a source producing fastq, with a bounded "threads" parameter.
	tf.Add("cas-a", ps.ToolFunctionDecl{
		Outputs:    map[string]ps.OutputPortDecl{"out": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle}},
		Parameters: map[string]ps.ParameterDecl{"threads": {AllowedValues: []string{"1", "2", "4"}}},
	})
	// cas-b: consumes fastq (required, SINGLE), produces bam.
	tf.Add("cas-b", ps.ToolFunctionDecl{
		Inputs:  map[string]ps.InputPortDecl{"in": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle, Required: true}},
		Outputs: map[string]ps.OutputPortDecl{"out": {DataFormat: "bam", Cardinality: ps.CardinalitySingle}},
	})
	// cas-ref: consumes a required fasta reference (for asset-binding tests).
	tf.Add("cas-ref", ps.ToolFunctionDecl{
		Inputs: map[string]ps.InputPortDecl{"ref": {DataFormat: "fasta", Cardinality: ps.CardinalitySingle, Required: true}},
	})
	// cas-multi: a required input declared MULTIPLE (out of v1 profile).
	tf.Add("cas-multi", ps.ToolFunctionDecl{
		Inputs: map[string]ps.InputPortDecl{"in": {DataFormat: "fastq", Cardinality: ps.CardinalityMultiple, Required: true}},
	})
	// cas-opt: an OPTIONAL fasta input (for optional-but-bound tests).
	tf.Add("cas-opt", ps.ToolFunctionDecl{
		Inputs: map[string]ps.InputPortDecl{"opt": {DataFormat: "fasta", Cardinality: ps.CardinalitySingle, Required: false}},
	})
	// cas-unkout: an output whose data format is UNKNOWN.
	tf.Add("cas-unkout", ps.ToolFunctionDecl{
		Outputs: map[string]ps.OutputPortDecl{"out": {DataFormat: "UNKNOWN", Cardinality: ps.CardinalitySingle}},
	})
	// cas-io: input + output both fastq (for building cycles); input not required.
	tf.Add("cas-io", ps.ToolFunctionDecl{
		Inputs:  map[string]ps.InputPortDecl{"in": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle, Required: false}},
		Outputs: map[string]ps.OutputPortDecl{"out": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle}},
	})
	// cas-slot: a required fastq input satisfied by an external slot.
	tf.Add("cas-slot", ps.ToolFunctionDecl{
		Inputs: map[string]ps.InputPortDecl{"in": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle, Required: true}},
	})

	sori := fake.NewSoriCatalog()
	sori.Add("asset1", "rev1", "mem1", ps.AssetMemberDecl{DataFormat: "fasta", Cardinality: ps.CardinalitySingle})
	// A member whose format does not match a fasta input (for Q16 tests).
	sori.Add("assetbad", "rev1", "mem1", ps.AssetMemberDecl{DataFormat: "bam", Cardinality: ps.CardinalitySingle})
	// A member whose data format matches the fasta "ref" input and whose target
	// input is SINGLE, but whose RESOLVED member cardinality is MULTIPLE (out of
	// v1 profile). Used to prove the resolved member cardinality is gated.
	sori.Add("assetmulti", "rev1", "mem1", ps.AssetMemberDecl{DataFormat: "fasta", Cardinality: ps.CardinalityMultiple})

	return ps.Resolvers{ToolFunction: tf, Sori: sori}
}

// validPipelineJSON is a minimal valid contract: node a (fastq source) -> node b.
const validPipelineJSON = `{
  "semantic_derivation_version":"pipelinestore.pipeline-contract.v1",
  "canonicalization_version":"pipelinestore.pipeline-contract.v1",
  "nodes":[
    {"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[{"name":"threads","value":"4"}]},
    {"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}
  ],
  "direct_edges":[
    {"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}
  ],
  "reusable_asset_bindings":[],
  "external_input_slots":[]
}`

// mustDigest parses and digests a contract body, failing the test on parse error.
func mustDigest(t *testing.T, body string) string {
	t.Helper()
	c, err := ps.Parse([]byte(body))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	return ps.Digest(c)
}

// validateBody parses then validates a contract body against the base resolvers.
func validateBody(t *testing.T, body string) error {
	t.Helper()
	c, err := ps.Parse([]byte(body))
	if err != nil {
		return err
	}
	return ps.Validate(context.Background(), c, baseResolvers())
}

// wantCode asserts that err is a *ps.Error carrying the expected code.
func wantCode(t *testing.T, err error, code ps.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", code)
	}
	if got := ps.CodeOf(err); got != code {
		t.Fatalf("expected code %s, got %s (err=%v)", code, got, err)
	}
}
