package intent

import (
	"context"
	"errors"
	"fmt"

	ps "github.com/HeaInSeo/PipelineStore"
)

// RevisionReader is the narrow exact-read view of committed PipelineRevisions
// that intent creation needs. ps.Store satisfies it.
type RevisionReader interface {
	GetRevision(ctx context.Context, pipelineID string, revisionID ps.PipelineRevisionID) (*ps.PipelineRevision, error)
}

var _ RevisionReader = ps.Store(nil)

// Service applies the PIPE-I0 intent semantics on top of a Store.
type Service struct {
	store     Store
	revisions RevisionReader
}

// NewService returns a Service. Both dependencies are required.
func NewService(store Store, revisions RevisionReader) (*Service, error) {
	if store == nil {
		return nil, errors.New("intent: NewService requires a Store")
	}
	if revisions == nil {
		return nil, errors.New("intent: NewService requires a RevisionReader")
	}
	return &Service{store: store, revisions: revisions}, nil
}

// requireCommitted confirms ref names an existing committed revision by exact
// read. Any read failure fails closed before intent state is touched.
func (s *Service) requireCommitted(ctx context.Context, ref PipelineRevisionRef) error {
	rev, err := s.revisions.GetRevision(ctx, ref.PipelineID, ref.RevisionID)
	if err != nil {
		return fmt.Errorf("intent: pipeline revision (%s, %s) is not a readable committed revision: %w",
			ref.PipelineID, ref.RevisionID, err)
	}
	if rev == nil || rev.PipelineID != ref.PipelineID || rev.RevisionID != ref.RevisionID {
		return newError(ps.CodeNotFound, "pipeline revision (%s, %s) exact read returned a different or empty revision",
			ref.PipelineID, ref.RevisionID)
	}
	return nil
}

// CreateAutomatic records the automatic intent for the request's uniqueness
// domain (AutoRunPolicyID, InputBindingSubjectIdentity), or returns the one
// already recorded. A replay never creates a second intent and never rewrites
// the frozen policy or pipeline revision; a replay that observed different
// revisions is reported through CreateResult.Divergence.
//
// A replay is resolved from the store before the revision read, so it neither
// depends on the revision reader being available nor on the observed revision
// being committed; only a call that may create an intent reads the revision.
func (s *Service) CreateAutomatic(ctx context.Context, req AutomaticRequest) (CreateResult, error) {
	if err := req.validate(); err != nil {
		return CreateResult{}, err
	}
	existing, found, err := s.store.LookupAutomatic(ctx, req.AutoRunPolicyID, req.InputBindingSubjectIdentity)
	if err != nil {
		return CreateResult{}, err
	}
	if found {
		return automaticReplay(existing, req), nil
	}
	if err := s.requireCommitted(ctx, req.PipelineRevision); err != nil {
		return CreateResult{}, err
	}
	stored, created, err := s.store.CreateAutomatic(ctx, Intent{
		Origin:                      OriginAutomatic,
		AutoRunPolicyID:             req.AutoRunPolicyID,
		AutoRunPolicyRevision:       req.AutoRunPolicyRevision,
		InputBindingSubjectIdentity: req.InputBindingSubjectIdentity,
		PipelineRevision:            req.PipelineRevision,
	})
	if err != nil {
		return CreateResult{}, err
	}
	if !created {
		// Lost a race to a concurrent creator after the lookup.
		return automaticReplay(stored, req), nil
	}
	return CreateResult{Intent: stored, Created: true}, nil
}

// automaticReplay returns the frozen intent for a reevaluation of its domain,
// reporting any revision the reevaluation observed that differs from it.
func automaticReplay(stored Intent, req AutomaticRequest) CreateResult {
	res := CreateResult{Intent: stored}
	d := Divergence{
		PolicyRevisionChanged:    stored.AutoRunPolicyRevision != req.AutoRunPolicyRevision,
		PipelineRevisionChanged:  stored.PipelineRevision != req.PipelineRevision,
		ObservedPolicyRevision:   req.AutoRunPolicyRevision,
		ObservedPipelineRevision: req.PipelineRevision,
	}
	if d.PolicyRevisionChanged || d.PipelineRevisionChanged {
		res.Divergence = &d
	}
	return res
}

// CreateExplicit records the explicit intent for the request's OperationID, or
// returns the one already recorded when the semantics match. The same
// OperationID with different semantics fails with ps.CodeOperationConflict and
// writes nothing. As with CreateAutomatic, an existing OperationID is resolved
// before the revision read.
func (s *Service) CreateExplicit(ctx context.Context, req ExplicitRequest) (CreateResult, error) {
	if err := req.validate(); err != nil {
		return CreateResult{}, err
	}
	draft := Intent{
		Origin:                      OriginExplicit,
		OperationID:                 req.OperationID,
		InputBindingSubjectIdentity: req.InputBindingSubjectIdentity,
		PipelineRevision:            req.PipelineRevision,
	}
	existing, found, err := s.store.LookupExplicit(ctx, req.OperationID)
	if err != nil {
		return CreateResult{}, err
	}
	if found {
		return explicitReplay(existing, draft)
	}
	if err := s.requireCommitted(ctx, req.PipelineRevision); err != nil {
		return CreateResult{}, err
	}
	stored, created, err := s.store.CreateExplicit(ctx, draft)
	if err != nil {
		return CreateResult{}, err
	}
	if !created {
		// Lost a race to a concurrent creator after the lookup.
		return explicitReplay(stored, draft)
	}
	return CreateResult{Intent: stored, Created: true}, nil
}

// explicitReplay converges on the intent already bound to the draft's
// OperationID, or fails with ps.CodeOperationConflict if its semantics differ.
func explicitReplay(stored, draft Intent) (CreateResult, error) {
	if !sameExplicitSemantics(stored, draft) {
		return CreateResult{}, newError(ps.CodeOperationConflict,
			"operation %q is already bound to intent %q with different semantics", draft.OperationID, stored.ID)
	}
	return CreateResult{Intent: stored}, nil
}

// AttachRunID attaches an externally assigned RunID to an existing intent.
// Re-attaching the same RunID converges. A different RunID, or a RunID already
// attached to another intent, fails with CodeRunIDConflict and changes nothing.
func (s *Service) AttachRunID(ctx context.Context, id ID, run RunID) (Intent, error) {
	if id == "" {
		return Intent{}, missing("intent ID")
	}
	if run == "" {
		return Intent{}, missing("RunID")
	}
	return s.store.AttachRunID(ctx, id, run)
}

// Get returns the intent by exact id.
func (s *Service) Get(ctx context.Context, id ID) (Intent, error) {
	if id == "" {
		return Intent{}, missing("intent ID")
	}
	return s.store.Get(ctx, id)
}
