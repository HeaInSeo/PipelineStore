// Package intent implements the PIPE-I0 pre-Run RunGenerationIntent core:
// automatic uniqueness over (AutoRunPolicyID, InputBindingSubjectIdentity),
// frozen policy/pipeline revisions, explicit operation idempotency, and
// attach-only assignment of an externally allocated RunID.
//
// An intent always refers to an exact committed PipelineRevision coordinate
// (PipelineID, PipelineRevisionID) issued by PIPE-I1 and confirmed through an
// exact read before any intent state is written. This package never recomputes
// contract digests, derives revision identities, or resolves "latest".
//
// Scope boundary (PIPE-I0): no RunID allocation, no queue, no network or
// process adapter, and no call into JUMI, PolicyScheduler, Tori,
// Kubernetes/Kueue, or Authorization. Identities are opaque strings and the
// error codes and result fields are internal; neither is a frozen public wire
// format. MemoryStore is the reference/test implementation of the atomic Store
// contract and is not evidence of production durability; a durable adapter is
// a separate storage gate.
//
// PIPE-I3 adds the automatic admission local core on the same Store: a
// serialized per-policy lifecycle log (ACTIVATE/DISABLE/REENABLE/RETIRE) whose
// activation entries carry the epoch's frontier, an admission gate
// (Service.AdmitAutomatic) that records a new automatic intent only for an
// ACTIVE policy, an occurrence class of exactly NEW, and a caller-provided
// opaque PublicationPosition strictly after the frontier, and an owner-scoped
// blocker set that holds materialization. Disable holds unassigned intents,
// never cancels, and is not undone by re-enable. Publication order is never
// inferred from time, events, delivery, or listing order; unknown or
// incomparable facts are not admitted. Scope boundary (PIPE-I3): no Tori
// adapter or wire format, no Run submit, Campaign, GC, or Authorization
// provider, no multiple-policy conflict handling, and no submit-equality
// authority (jumi.submit-intent.v1 is JUMI-owned). Service.CreateAutomatic
// remains the ungated PIPE-I0 primitive and is not an automatic admission path.
package intent
