package lowering_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	ps "github.com/HeaInSeo/PipelineStore"
	"github.com/HeaInSeo/PipelineStore/fake"
	"github.com/HeaInSeo/PipelineStore/lowering"
)

const (
	digestHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	imageA    = "registry.example/tools/a@sha256:" + digestHex
	imageB    = "registry.example/tools/b@sha256:" + digestHex
)

// twoNodeJSON is the first-slice fixture: a.out (fastq, SINGLE) -> b.in.
const twoNodeJSON = `{
  "semantic_derivation_version":"pipelinestore.pipeline-contract.v1",
  "canonicalization_version":"pipelinestore.pipeline-contract.v1",
  "nodes":[
    {"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]},
    {"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}
  ],
  "direct_edges":[
    {"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}
  ],
  "reusable_asset_bindings":[],
  "external_input_slots":[]
}`

// twoNodePermutedJSON is twoNodeJSON with nodes listed in a different order.
const twoNodePermutedJSON = `{
  "semantic_derivation_version":"pipelinestore.pipeline-contract.v1",
  "canonicalization_version":"pipelinestore.pipeline-contract.v1",
  "nodes":[
    {"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]},
    {"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]}
  ],
  "direct_edges":[
    {"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}
  ],
  "reusable_asset_bindings":[],
  "external_input_slots":[]
}`

var submittedAt = time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)

func decls() map[string]ps.ToolFunctionDecl {
	return map[string]ps.ToolFunctionDecl{
		"cas-a": {
			Outputs: map[string]ps.OutputPortDecl{"out": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle}},
		},
		"cas-b": {
			Inputs:  map[string]ps.InputPortDecl{"in": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle, Required: true}},
			Outputs: map[string]ps.OutputPortDecl{"out": {DataFormat: "bam", Cardinality: ps.CardinalitySingle}},
		},
		"cas-a2": {
			Outputs: map[string]ps.OutputPortDecl{
				"o2": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle},
				"o1": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle},
			},
		},
		"cas-b2": {
			Inputs: map[string]ps.InputPortDecl{
				"i2": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle, Required: true},
				"i1": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle, Required: true},
			},
		},
	}
}

func runnables() map[string]lowering.Runnable {
	return map[string]lowering.Runnable{
		"cas-a":  {Image: imageA, Command: []string{"/bin/a"}},
		"cas-b":  {Image: imageB, Command: []string{"/bin/b"}, Args: []string{"--x"}},
		"cas-a2": {Image: imageA, Command: []string{"/bin/a2"}},
		"cas-b2": {Image: imageB, Command: []string{"/bin/b2"}},
	}
}

// revision builds the exact committed revision a store would return for body.
func revision(t *testing.T, body string) ps.PipelineRevision {
	t.Helper()
	p, err := ps.PrepareUnvalidated([]byte(body))
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	return ps.PipelineRevision{
		PipelineID:                "pipe-1",
		RevisionID:                "rev-1",
		ContractDigest:            p.Digest,
		SemanticDerivationVersion: ps.ContractVersionV1,
		CanonicalizationVersion:   ps.ContractVersionV1,
		CanonicalBody:             p.Canonical,
		Contract:                  p.Contract,
	}
}

// committedRevision is revision plus proof that body passes commit validation
// under the same frozen declarations.
func committedRevision(t *testing.T, body string) ps.PipelineRevision {
	t.Helper()
	rev := revision(t, body)
	tf := fake.NewToolFunctionCatalog()
	for cas, d := range decls() {
		tf.Add(cas, d)
	}
	res := ps.Resolvers{ToolFunction: tf, Sori: fake.NewSoriCatalog()}
	if err := ps.Validate(context.Background(), rev.Contract, res); err != nil {
		t.Fatalf("fixture is not commit-valid: %v", err)
	}
	return rev
}

func input(t *testing.T, body string) lowering.Input {
	t.Helper()
	meta := lowering.FrozenMetadata{SubmittedAt: submittedAt, RequesterID: "user-1", TraceID: "trace-1"}
	return lowering.Input{
		RunID:         "run-1",
		Revision:      committedRevision(t, body),
		Metadata:      meta,
		ToolFunctions: decls(),
		Runnables:     runnables(),
		Authorization: lowering.DecisionAllow,
	}
}

func mustLower(t *testing.T, in lowering.Input) lowering.RunSpec {
	t.Helper()
	spec, err := lowering.Lower(in)
	if err != nil {
		t.Fatalf("lower: %v", err)
	}
	return spec
}

func wantCode(t *testing.T, err error, code ps.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", code)
	}
	if got := ps.CodeOf(err); got != code {
		t.Fatalf("expected code %s, got %s (err=%v)", code, got, err)
	}
}

func TestLower_TwoNodeSingle(t *testing.T) {
	got := mustLower(t, input(t, twoNodeJSON))
	run := lowering.RunMetadata{
		RunID:         "run-1",
		SubmittedAt:   submittedAt,
		FailurePolicy: lowering.FailurePolicy{Mode: "fail-fast"},
		RequesterID:   "user-1",
		TraceID:       "trace-1",
	}
	bind := lowering.ArtifactBinding{BindingName: "in", ChildInputName: "in", ProducerNodeID: "a", ProducerOutputName: "out", Required: true}
	nodeA := lowering.Node{NodeID: "a", Image: imageA, Command: []string{"/bin/a"}, Args: []string{}, Outputs: []string{"out"}}
	nodeB := lowering.Node{NodeID: "b", Image: imageB, Command: []string{"/bin/b"}, Args: []string{"--x"}, Inputs: []string{"in"}, Outputs: []string{"out"}}
	nodeB.ArtifactBindings = []lowering.ArtifactBinding{bind}
	graph := lowering.Graph{Nodes: []lowering.Node{nodeA, nodeB}, Edges: [][]string{{"a", "b"}}}
	defaults := lowering.Defaults{RetryPolicy: lowering.RetryPolicy{MaxAttempts: 1}}
	want := lowering.RunSpec{Run: run, Graph: graph, Defaults: defaults}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lowered spec mismatch:\n got  %+v\n want %+v", got, want)
	}
}

// TestLower_GoldenJSON pins the exact JUMI ExecutableRunSpec wire encoding of
// the 2-node fixture, including the explicit PIPE-D2 fail-fast / maxAttempts=1.
func TestLower_GoldenJSON(t *testing.T) {
	spec := mustLower(t, input(t, twoNodeJSON))
	got, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want, err := os.ReadFile("testdata/two_node_single.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if !bytes.Equal(got, bytes.TrimSpace(want)) {
		t.Fatalf("golden mismatch:\n got  %s\n want %s", got, bytes.TrimSpace(want))
	}
}

func TestLower_Deterministic(t *testing.T) {
	first := mustLower(t, input(t, twoNodeJSON))
	second := mustLower(t, input(t, twoNodeJSON))
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same frozen input lowered differently")
	}
	permuted := mustLower(t, input(t, twoNodePermutedJSON))
	if !reflect.DeepEqual(first, permuted) {
		t.Fatalf("authoring order changed the lowering")
	}
}

// TestLower_NoAliasing proves caller mutation after lowering cannot change an
// already lowered spec, and mutating a spec cannot change a later lowering.
func TestLower_NoAliasing(t *testing.T) {
	in := input(t, twoNodeJSON)
	spec := mustLower(t, in)
	want := mustLower(t, input(t, twoNodeJSON))

	in.Runnables["cas-b"].Command[0] = "/bin/evil"
	in.Runnables["cas-b"].Args[0] = "--evil"
	in.ToolFunctions["cas-b"].Inputs["in2"] = ps.InputPortDecl{}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("caller mutation leaked into lowered spec")
	}

	spec.Graph.Nodes[1].Command[0] = "/bin/other"
	spec.Graph.Nodes[1].ArtifactBindings[0].ProducerNodeID = "x"
	again := mustLower(t, input(t, twoNodeJSON))
	if !reflect.DeepEqual(again, want) {
		t.Fatalf("spec mutation leaked into a later lowering")
	}
}

// TestLower_FrozenMetadataNeverGenerated proves SubmittedAt/TraceID/RequesterID
// come only from the caller: empty identities stay empty and the timestamp is
// exactly the frozen instant.
func TestLower_FrozenMetadataNeverGenerated(t *testing.T) {
	in := input(t, twoNodeJSON)
	in.Metadata = lowering.FrozenMetadata{SubmittedAt: time.Now()}
	spec := mustLower(t, in)
	if spec.Run.TraceID != "" || spec.Run.RequesterID != "" {
		t.Fatalf("core generated identity: trace=%q requester=%q", spec.Run.TraceID, spec.Run.RequesterID)
	}
	if !spec.Run.SubmittedAt.Equal(in.Metadata.SubmittedAt) {
		t.Fatalf("submittedAt %v differs from frozen %v", spec.Run.SubmittedAt, in.Metadata.SubmittedAt)
	}
	again := mustLower(t, in)
	if !reflect.DeepEqual(spec, again) {
		t.Fatalf("same frozen metadata lowered differently")
	}
	raw, err := json.Marshal(spec.Run)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"traceId", "requesterId"} {
		if _, ok := m[k]; ok {
			t.Fatalf("empty %s was emitted", k)
		}
	}
}

func TestLower_MultiplePortsBetweenSameNodes(t *testing.T) {
	body := `{
  "semantic_derivation_version":"pipelinestore.pipeline-contract.v1",
  "canonicalization_version":"pipelinestore.pipeline-contract.v1",
  "nodes":[
    {"node_id":"a","tool_function_cas_hash":"cas-a2","fixed_parameters":[]},
    {"node_id":"b","tool_function_cas_hash":"cas-b2","fixed_parameters":[]}
  ],
  "direct_edges":[
    {"from_node_id":"a","from_output_port":"o2","to_node_id":"b","to_input_port":"i2"},
    {"from_node_id":"a","from_output_port":"o1","to_node_id":"b","to_input_port":"i1"}
  ],
  "reusable_asset_bindings":[],
  "external_input_slots":[]
}`
	spec := mustLower(t, input(t, body))
	if want := [][]string{{"a", "b"}}; !reflect.DeepEqual(spec.Graph.Edges, want) {
		t.Fatalf("edges = %v, want %v", spec.Graph.Edges, want)
	}
	want := []lowering.ArtifactBinding{
		{BindingName: "i1", ChildInputName: "i1", ProducerNodeID: "a", ProducerOutputName: "o1", Required: true},
		{BindingName: "i2", ChildInputName: "i2", ProducerNodeID: "a", ProducerOutputName: "o2", Required: true},
	}
	if got := spec.Graph.Nodes[1].ArtifactBindings; !reflect.DeepEqual(got, want) {
		t.Fatalf("bindings = %+v, want %+v", got, want)
	}
	if got, want := spec.Graph.Nodes[0].Outputs, []string{"o1", "o2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("outputs = %v, want %v", got, want)
	}
}

func TestLower_FailClosed(t *testing.T) {
	cases := []struct {
		name string
		mut  func(in *lowering.Input)
		code ps.Code
	}{
		{"empty run id", func(in *lowering.Input) { in.RunID = "" }, lowering.CodeMissingFrozenInput},
		{"zero submittedAt", func(in *lowering.Input) { in.Metadata.SubmittedAt = time.Time{} }, lowering.CodeMissingFrozenInput},
		{"submittedAt year after 9999", func(in *lowering.Input) {
			in.Metadata.SubmittedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}, lowering.CodeInvalidFrozenInput},
		{"submittedAt negative year", func(in *lowering.Input) {
			in.Metadata.SubmittedAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)
		}, lowering.CodeInvalidFrozenInput},
		{"submittedAt offset beyond RFC 3339", func(in *lowering.Input) {
			in.Metadata.SubmittedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("", 24*3600))
		}, lowering.CodeInvalidFrozenInput},
		{"empty revision id", func(in *lowering.Input) { in.Revision.RevisionID = "" }, lowering.CodeMissingFrozenInput},
		{"empty pipeline id", func(in *lowering.Input) { in.Revision.PipelineID = "" }, lowering.CodeMissingFrozenInput},
		{"authorization unknown", func(in *lowering.Input) { in.Authorization = lowering.DecisionUnknown }, lowering.CodeAuthorizationUnknown},
		{"authorization unrecognized", func(in *lowering.Input) { in.Authorization = "MAYBE" }, lowering.CodeAuthorizationUnknown},
		{"authorization denied", func(in *lowering.Input) { in.Authorization = lowering.DecisionDeny }, lowering.CodeAuthorizationDenied},
		{"missing declaration", func(in *lowering.Input) { delete(in.ToolFunctions, "cas-b") }, ps.CodeToolFunctionUnresolved},
		{"missing runnable", func(in *lowering.Input) { delete(in.Runnables, "cas-a") }, lowering.CodeRunnableUnresolved},
		{"tag image", func(in *lowering.Input) {
			in.Runnables["cas-a"] = lowering.Runnable{Image: "registry.example/tools/a:latest"}
		}, lowering.CodeRunnableUnresolved},
		{"uppercase digest", func(in *lowering.Input) {
			in.Runnables["cas-a"] = lowering.Runnable{Image: "registry.example/tools/a@sha256:" + "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"}
		}, lowering.CodeRunnableUnresolved},
		{"tampered digest", func(in *lowering.Input) { in.Revision.ContractDigest = digestHex }, ps.CodeIntegrity},
		{"tampered canonical body", func(in *lowering.Input) {
			in.Revision.CanonicalBody = append(append([]byte(nil), in.Revision.CanonicalBody...), ' ')
		}, ps.CodeIntegrity},
		{"nil contract", func(in *lowering.Input) { in.Revision.Contract = nil }, ps.CodeIntegrity},
		{"unsupported version", func(in *lowering.Input) { in.Revision.CanonicalizationVersion = "v0" }, ps.CodeUnsupportedVersion},
		{"declaration drift: input missing", func(in *lowering.Input) {
			in.ToolFunctions["cas-b"] = ps.ToolFunctionDecl{Outputs: in.ToolFunctions["cas-b"].Outputs}
		}, ps.CodeEdgeInvalid},
		{"declaration drift: output missing", func(in *lowering.Input) {
			in.ToolFunctions["cas-a"] = ps.ToolFunctionDecl{}
		}, ps.CodeEdgeInvalid},
		{"multiple cardinality", func(in *lowering.Input) {
			d := in.ToolFunctions["cas-b"]
			d.Inputs = map[string]ps.InputPortDecl{"in": {DataFormat: "fastq", Cardinality: ps.CardinalityMultiple, Required: true}}
			in.ToolFunctions["cas-b"] = d
		}, ps.CodeCardinalityUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := input(t, twoNodeJSON)
			tc.mut(&in)
			spec, err := lowering.Lower(in)
			wantCode(t, err, tc.code)
			if !reflect.DeepEqual(spec, lowering.RunSpec{}) {
				t.Fatalf("failed lowering returned a non-zero spec: %+v", spec)
			}
		})
	}
}

func TestLower_ProfileGates(t *testing.T) {
	const head = `{
  "semantic_derivation_version":"pipelinestore.pipeline-contract.v1",
  "canonicalization_version":"pipelinestore.pipeline-contract.v1",`
	cases := []struct {
		name string
		body string
		code ps.Code
	}{
		{"fixed parameter", head + `
  "nodes":[
    {"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[{"name":"threads","value":"4"}]},
    {"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}
  ],
  "direct_edges":[{"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}],
  "reusable_asset_bindings":[],
  "external_input_slots":[]
}`, lowering.CodeNotRepresentable},
		{"reusable asset binding", head + `
  "nodes":[{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}],
  "direct_edges":[],
  "reusable_asset_bindings":[{"to_node_id":"b","to_input_port":"in","asset_id":"asset1","asset_revision_id":"rev1","member_key":"mem1"}],
  "external_input_slots":[]
}`, lowering.CodeNotRepresentable},
		{"external input slot", head + `
  "nodes":[{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}],
  "direct_edges":[],
  "reusable_asset_bindings":[],
  "external_input_slots":[{"slot_id":"s1","to_node_id":"b","to_input_port":"in"}]
}`, lowering.CodeNotRepresentable},
		{"fan-out", head + `
  "nodes":[
    {"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]},
    {"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]},
    {"node_id":"c","tool_function_cas_hash":"cas-b","fixed_parameters":[]}
  ],
  "direct_edges":[
    {"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"},
    {"from_node_id":"a","from_output_port":"out","to_node_id":"c","to_input_port":"in"}
  ],
  "reusable_asset_bindings":[],
  "external_input_slots":[]
}`, lowering.CodeNotRepresentable},
		{"two providers", head + `
  "nodes":[
    {"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[]},
    {"node_id":"a2","tool_function_cas_hash":"cas-a","fixed_parameters":[]},
    {"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}
  ],
  "direct_edges":[
    {"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"},
    {"from_node_id":"a2","from_output_port":"out","to_node_id":"b","to_input_port":"in"}
  ],
  "reusable_asset_bindings":[],
  "external_input_slots":[]
}`, ps.CodeProviderConflict},
		{"required input unbound", head + `
  "nodes":[{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}],
  "direct_edges":[],
  "reusable_asset_bindings":[],
  "external_input_slots":[]
}`, ps.CodeProviderMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// revision (not committedRevision): some bodies are deliberately not
			// commit-valid, and lowering must still fail closed on its own.
			in := input(t, twoNodeJSON)
			in.Revision = revision(t, tc.body)
			spec, err := lowering.Lower(in)
			wantCode(t, err, tc.code)
			if !reflect.DeepEqual(spec, lowering.RunSpec{}) {
				t.Fatalf("failed lowering returned a non-zero spec: %+v", spec)
			}
		})
	}
}

// TestCheckSpec_NegativeGoldens mutates a valid lowered spec into each
// inconsistency the JUMI validator would accept or that breaks v1 policy.
func TestCheckSpec_NegativeGoldens(t *testing.T) {
	binding := func(s *lowering.RunSpec) *lowering.ArtifactBinding { return &s.Graph.Nodes[1].ArtifactBindings[0] }
	cases := []struct {
		name string
		mut  func(s *lowering.RunSpec)
		code ps.Code
	}{
		{"unknown producer", func(s *lowering.RunSpec) { binding(s).ProducerNodeID = "c" }, lowering.CodeSameSourceViolation},
		{"undeclared producer output", func(s *lowering.RunSpec) { binding(s).ProducerOutputName = "nope" }, lowering.CodeSameSourceViolation},
		{"undeclared consumer input", func(s *lowering.RunSpec) {
			binding(s).BindingName = "zzz"
			binding(s).ChildInputName = "zzz"
		}, lowering.CodeSameSourceViolation},
		{"binding name differs from child input", func(s *lowering.RunSpec) { binding(s).BindingName = "other" }, lowering.CodeSameSourceViolation},
		{"binding without edge", func(s *lowering.RunSpec) { s.Graph.Edges = [][]string{} }, lowering.CodeSameSourceViolation},
		{"edge without binding", func(s *lowering.RunSpec) { s.Graph.Nodes[1].ArtifactBindings = nil }, lowering.CodeSameSourceViolation},
		{"reversed edge", func(s *lowering.RunSpec) { s.Graph.Edges = [][]string{{"b", "a"}} }, lowering.CodeSameSourceViolation},
		{"duplicate binding", func(s *lowering.RunSpec) {
			n := &s.Graph.Nodes[1]
			n.ArtifactBindings = append(n.ArtifactBindings, n.ArtifactBindings[0])
		}, lowering.CodeSameSourceViolation},
		{"empty binding name", func(s *lowering.RunSpec) {
			binding(s).BindingName = ""
			binding(s).ChildInputName = ""
		}, ps.CodeInvalidContract},
		{"failure mode omitted", func(s *lowering.RunSpec) { s.Run.FailurePolicy.Mode = "" }, ps.CodeInvalidContract},
		{"maxAttempts omitted", func(s *lowering.RunSpec) { s.Defaults.RetryPolicy.MaxAttempts = 0 }, ps.CodeInvalidContract},
		{"empty run id", func(s *lowering.RunSpec) { s.Run.RunID = "" }, lowering.CodeMissingFrozenInput},
		{"zero submittedAt", func(s *lowering.RunSpec) { s.Run.SubmittedAt = time.Time{} }, lowering.CodeMissingFrozenInput},
		{"unencodable submittedAt", func(s *lowering.RunSpec) {
			s.Run.SubmittedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}, lowering.CodeInvalidFrozenInput},
		{"no nodes", func(s *lowering.RunSpec) { s.Graph = lowering.Graph{} }, ps.CodeInvalidContract},
		{"duplicate node", func(s *lowering.RunSpec) { s.Graph.Nodes[1].NodeID = "a" }, ps.CodeDuplicate},
		{"unpinned image", func(s *lowering.RunSpec) { s.Graph.Nodes[0].Image = "a:latest" }, lowering.CodeRunnableUnresolved},
		{"duplicate edge", func(s *lowering.RunSpec) { s.Graph.Edges = append(s.Graph.Edges, []string{"a", "b"}) }, ps.CodeDuplicate},
		{"self loop", func(s *lowering.RunSpec) { s.Graph.Edges = append(s.Graph.Edges, []string{"a", "a"}) }, ps.CodeEdgeInvalid},
		{"dangling edge", func(s *lowering.RunSpec) { s.Graph.Edges = append(s.Graph.Edges, []string{"a", "z"}) }, ps.CodeEdgeInvalid},
		{"malformed edge", func(s *lowering.RunSpec) { s.Graph.Edges = append(s.Graph.Edges, []string{"a"}) }, ps.CodeEdgeInvalid},
		{"cycle", func(s *lowering.RunSpec) { s.Graph.Edges = append(s.Graph.Edges, []string{"b", "a"}) }, ps.CodeGraphCycle},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := mustLower(t, input(t, twoNodeJSON))
			if err := lowering.CheckSpec(spec); err != nil {
				t.Fatalf("baseline spec rejected: %v", err)
			}
			tc.mut(&spec)
			wantCode(t, lowering.CheckSpec(spec), tc.code)
		})
	}
}
