package intent

import (
	"fmt"

	ps "github.com/HeaInSeo/PipelineStore"
)

// ID is the opaque identity of a RunGenerationIntent, allocated by the Store.
type ID string

// RunID is a Run identity allocated by an external authority. This package only
// attaches it to an existing intent; it never mints one.
type RunID string

// Origin distinguishes the automatic and explicit request families.
type Origin string

const (
	// OriginAutomatic intents are unique per (AutoRunPolicyID, InputBindingSubjectIdentity).
	OriginAutomatic Origin = "AUTOMATIC"
	// OriginExplicit intents are unique per explicit OperationID.
	OriginExplicit Origin = "EXPLICIT"
)

const (
	// CodeMissingCoordinate is an empty required semantic coordinate.
	CodeMissingCoordinate ps.Code = "MISSING_COORDINATE"
	// CodeRunIDConflict is an attempt to attach a RunID different from the one
	// already attached to the intent.
	CodeRunIDConflict ps.Code = "RUN_ID_CONFLICT"
)

// PipelineRevisionRef is the exact committed PipelineRevision coordinate issued
// by PIPE-I1. Both parts are opaque.
type PipelineRevisionRef struct {
	PipelineID string
	RevisionID ps.PipelineRevisionID
}

// Intent is a pre-Run RunGenerationIntent. Every field is a value, so copies
// handed to or returned from the Store never alias stored state.
type Intent struct {
	ID     ID
	Origin Origin

	// AutoRunPolicyID and AutoRunPolicyRevision are set for automatic intents.
	// The revision is frozen at first commit.
	AutoRunPolicyID       string
	AutoRunPolicyRevision string

	// OperationID is set for explicit intents.
	OperationID string

	InputBindingSubjectIdentity string

	// PipelineRevision is frozen at first commit.
	PipelineRevision PipelineRevisionRef

	// RunID is empty until an externally assigned RunID is attached.
	RunID RunID
}

// AutomaticRequest is one automatic evaluation of a policy against a subject.
type AutomaticRequest struct {
	AutoRunPolicyID             string
	AutoRunPolicyRevision       string
	InputBindingSubjectIdentity string
	PipelineRevision            PipelineRevisionRef
}

// ExplicitRequest is an explicit request identified by its OperationID.
type ExplicitRequest struct {
	OperationID                 string
	InputBindingSubjectIdentity string
	PipelineRevision            PipelineRevisionRef
}

// Divergence reports that an automatic reevaluation of an existing uniqueness
// domain observed revisions other than the frozen ones. The frozen intent is
// returned unchanged; no second intent is created.
type Divergence struct {
	PolicyRevisionChanged    bool
	PipelineRevisionChanged  bool
	ObservedPolicyRevision   string
	ObservedPipelineRevision PipelineRevisionRef
}

// CreateResult is the outcome of a create call.
type CreateResult struct {
	Intent Intent
	// Created is true only when this call recorded a new intent.
	Created bool
	// Divergence is non-nil only for an automatic replay whose observed
	// revisions differ from the frozen ones.
	Divergence *Divergence
}

func newError(code ps.Code, format string, args ...any) error {
	return &ps.Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func missing(field string) error {
	return newError(CodeMissingCoordinate, "required coordinate %s is empty", field)
}

func (r PipelineRevisionRef) validate() error {
	if r.PipelineID == "" {
		return missing("PipelineRevision.PipelineID")
	}
	if r.RevisionID == "" {
		return missing("PipelineRevision.RevisionID")
	}
	return nil
}

func (r AutomaticRequest) validate() error {
	if r.AutoRunPolicyID == "" {
		return missing("AutoRunPolicyID")
	}
	if r.AutoRunPolicyRevision == "" {
		return missing("AutoRunPolicyRevision")
	}
	if r.InputBindingSubjectIdentity == "" {
		return missing("InputBindingSubjectIdentity")
	}
	return r.PipelineRevision.validate()
}

func (r ExplicitRequest) validate() error {
	if r.OperationID == "" {
		return missing("OperationID")
	}
	if r.InputBindingSubjectIdentity == "" {
		return missing("InputBindingSubjectIdentity")
	}
	return r.PipelineRevision.validate()
}

// sameExplicitSemantics reports whether a stored explicit intent carries the
// same immutable semantics as a new draft. RunID is excluded: it is attached
// after creation and is never part of the request identity.
func sameExplicitSemantics(stored, draft Intent) bool {
	return stored.Origin == draft.Origin &&
		stored.InputBindingSubjectIdentity == draft.InputBindingSubjectIdentity &&
		stored.PipelineRevision == draft.PipelineRevision
}
