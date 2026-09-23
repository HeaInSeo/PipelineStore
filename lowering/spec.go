package lowering

import "time"

// The types below mirror the subset of JUMI pkg/spec (ExecutableRunSpec,
// RunMetadata, FailurePolicy, Graph, Node, ArtifactBinding, Defaults,
// RetryPolicy) that this core emits, with identical JSON field names. Fields
// that the core never sets are omitted rather than emitted empty.

// FailureModeFailFast is the PIPE-D2 v1 run failure policy mode.
const FailureModeFailFast = "fail-fast"

// MaxAttemptsV1 is the PIPE-D2 v1 default retry budget.
const MaxAttemptsV1 = 1

// RunSpec is the lowered executable Run specification.
type RunSpec struct {
	Run      RunMetadata `json:"run"`
	Graph    Graph       `json:"graph"`
	Defaults Defaults    `json:"defaults"`
}

// RunMetadata carries the caller-frozen Run identity and metadata.
type RunMetadata struct {
	RunID         string        `json:"runId"`
	SubmittedAt   time.Time     `json:"submittedAt"`
	FailurePolicy FailurePolicy `json:"failurePolicy"`
	RequesterID   string        `json:"requesterId,omitempty"`
	TraceID       string        `json:"traceId,omitempty"`
}

// FailurePolicy is the run-level failure policy.
type FailurePolicy struct {
	Mode string `json:"mode"`
}

// Graph is the node set plus [from, to] dependency edges.
type Graph struct {
	Nodes []Node     `json:"nodes"`
	Edges [][]string `json:"edges"`
}

// Node is one lowered executable node.
type Node struct {
	NodeID           string            `json:"nodeId"`
	Image            string            `json:"image"`
	Command          []string          `json:"command"`
	Args             []string          `json:"args"`
	Inputs           []string          `json:"inputs,omitempty"`
	Outputs          []string          `json:"outputs,omitempty"`
	ArtifactBindings []ArtifactBinding `json:"artifactBindings,omitempty"`
}

// ArtifactBinding binds a consumer input to a producer node output.
type ArtifactBinding struct {
	BindingName        string `json:"bindingName"`
	ChildInputName     string `json:"childInputName,omitempty"`
	ProducerNodeID     string `json:"producerNodeId"`
	ProducerOutputName string `json:"producerOutputName"`
	Required           bool   `json:"required,omitempty"`
}

// Defaults carries run-wide node defaults.
type Defaults struct {
	RetryPolicy RetryPolicy `json:"retryPolicy"`
}

// RetryPolicy is the retry budget.
type RetryPolicy struct {
	MaxAttempts int `json:"maxAttempts"`
}
