// Package pipelinestore implements the PipelineStore PIPE-I1 semantic core:
// the versioned v1 PipelineContract model, strict validation and
// canonicalization owned by PipelineStore, the pipeline-contract digest, and
// the immutable revision identity/equality primitives.
//
// Storage topology lives behind the Store interface (see store.go); the durable
// SQLite-backed implementation is in the sqlitestore subpackage. Revision
// semantics (identity, digest, canonicalization, validation) are independent of
// the storage backend.
//
// Scope boundary (PIPE-I1): this package owns ToolFunction-pinned pipeline
// authoring semantics only. It contains no physical placement, mount, image
// locator, materialization URI, K8s, Tori, Sori mutation, or JUMI Run/Attempt
// concepts. The tool_profile_digest key is RESERVED and inactive in v1: when
// present it is rejected with a distinct unsupported-capability error class
// (TP-R1/TP-R2), never silently accepted and never used to select "latest".
package pipelinestore
