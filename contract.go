package pipelinestore

// ContractVersionV1 is the single supported contract identifier for PIPE-I1.
//
// It is the value required in both persisted version fields
// (semantic_derivation_version and canonicalization_version) and is the domain
// tag used by the pipeline-contract digest. Commit accepts ONLY this identifier;
// any other schema/canonicalization/semantic-derivation version fails closed and
// is never silently reinterpreted.
const ContractVersionV1 = "pipelinestore.pipeline-contract.v1"

// PipelineContract is the versioned v1 semantic body authored elsewhere and
// committed to PipelineStore. It contains ONLY logical authoring semantics.
//
// It deliberately has no field for a physical path, image locator, mount,
// PVC/HostPath, materialization URI, K8s placement, Tori endpoint/path, or JUMI
// Run/Attempt identity. Such keys cannot enter the v1 schema and are rejected as
// unknown semantic-body keys.
type PipelineContract struct {
	// SemanticDerivationVersion is the semantic-derivation basis version. Persisted
	// on every record; must equal ContractVersionV1 for commit to accept.
	SemanticDerivationVersion string `json:"semantic_derivation_version"`
	// CanonicalizationVersion selects the canonicalization/digest algorithm.
	// Persisted on every record; must equal ContractVersionV1 for commit to accept.
	CanonicalizationVersion string `json:"canonicalization_version"`

	Nodes                 []Node                 `json:"nodes"`
	DirectEdges           []DirectEdge           `json:"direct_edges"`
	ReusableAssetBindings []ReusableAssetBinding `json:"reusable_asset_bindings"`
	ExternalInputSlots    []ExternalInputSlot    `json:"external_input_slots"`
}

// Node is a pipeline node pinned to an exact runnable ToolFunction.
type Node struct {
	NodeID string `json:"node_id"`
	// ToolFunctionCASHash is the exact content-addressed pin of the runnable
	// ToolFunction. It is never alias-resolved or case-folded.
	ToolFunctionCASHash string `json:"tool_function_cas_hash"`
	// ToolProfileDigest is RESERVED and inactive in v1 (TP-R1/TP-R2). It is a
	// *string so that PRESENT (non-nil) is distinguishable from ABSENT (nil):
	// PRESENT is rejected with CodeUnsupportedCapability; ABSENT is the only
	// accepted state and never selects a "latest" profile.
	ToolProfileDigest *string          `json:"tool_profile_digest,omitempty"`
	FixedParameters   []FixedParameter `json:"fixed_parameters"`
}

// FixedParameter is an exact fixed parameter binding for a node. Value strings
// are preserved exactly (no coercion, no Unicode normalization).
type FixedParameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// DirectEdge is an exact from-node/output-port -> to-node/input-port dependency.
type DirectEdge struct {
	FromNodeID     string `json:"from_node_id"`
	FromOutputPort string `json:"from_output_port"`
	ToNodeID       string `json:"to_node_id"`
	ToInputPort    string `json:"to_input_port"`
}

// ReusableAssetBinding binds a target node input to an exact Sori asset member.
type ReusableAssetBinding struct {
	ToNodeID        string `json:"to_node_id"`
	ToInputPort     string `json:"to_input_port"`
	AssetID         string `json:"asset_id"`
	AssetRevisionID string `json:"asset_revision_id"`
	MemberKey       string `json:"member_key"`
}

// ExternalInputSlot is a logical external input bound at run time. It carries no
// concrete path/mount/provider locator; such fields cannot enter the schema.
type ExternalInputSlot struct {
	SlotID      string `json:"slot_id"`
	ToNodeID    string `json:"to_node_id"`
	ToInputPort string `json:"to_input_port"`
}
