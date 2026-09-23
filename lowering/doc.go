// Package lowering implements the PIPE-I2 pure deterministic lowering core: it
// turns one exact committed PipelineRevision plus caller-frozen typed inputs
// into an executable Run specification in the JUMI ExecutableRunSpec wire
// shape.
//
// Lower is a pure function of its Input. It performs no I/O, reads no clock,
// generates no random or trace identity, allocates no RunID, and never invents
// an upstream Attempt or artifact identity. The same frozen Input always yields
// the same RunSpec. The output never aliases caller-owned slices or maps.
//
// PIPE-D2 v1 execution semantics are emitted explicitly and never left to a
// JUMI fallback: run.failurePolicy.mode is "fail-fast" and
// defaults.retryPolicy.maxAttempts is 1.
//
// Every DirectEdge is lowered from one source of truth into both a
// Graph.Edges dependency and the consumer's ArtifactBinding (BindingName and
// ChildInputName are the consumer input port name). CheckSpec re-verifies that
// same-source invariant on the output independently of JUMI's validator, which
// does not check it.
//
// Fail-closed profile (first slice): acyclic graphs of SINGLE-to-SINGLE direct
// produced-artifact edges between pre-built runnables, with no fan-out of an
// output and at most one provider per input. A missing or
// UNKNOWN required fact (RunID, SubmittedAt, exact ToolFunction declaration,
// exact runnable resolution, Authorization decision, revision integrity) is
// rejected. Fixed parameters, reusable asset bindings and external input slots
// need an argv rendering or physical materialization contract that is not yet
// decided, so they are rejected as NOT_REPRESENTABLE rather than guessed.
//
// Scope boundary: no public DTO or wire freeze, no network or transport
// adapter, no RunID allocator, no Run submission, no physical materialization,
// and no Authorization or eligibility provider. The Authorization decision is
// a caller-supplied frozen fact; a fixture ALLOW is not production evidence.
// The RunSpec types mirror JUMI pkg/spec field names and JSON tags but do not
// import JUMI; they are internal and not a frozen public wire format.
package lowering
