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
func (s *Service) CreateAutomatic(ctx context.Context, req AutomaticRequest) (CreateResult, error) {
	if err := req.validate(); err != nil {
		return CreateResult{}, err
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
	res := CreateResult{Intent: stored, Created: created}
	if !created {
		d := Divergence{
			PolicyRevisionChanged:    stored.AutoRunPolicyRevision != req.AutoRunPolicyRevision,
			PipelineRevisionChanged:  stored.PipelineRevision != req.PipelineRevision,
			ObservedPolicyRevision:   req.AutoRunPolicyRevision,
			ObservedPipelineRevision: req.PipelineRevision,
		}
		if d.PolicyRevisionChanged || d.PipelineRevisionChanged {
			res.Divergence = &d
		}
	}
	return res, nil
}

// CreateExplicit records the explicit intent for the request's OperationID, or
// returns the one already recorded when the semantics match. The same
// OperationID with different semantics fails with ps.CodeOperationConflict and
// writes nothing.
func (s *Service) CreateExplicit(ctx context.Context, req ExplicitRequest) (CreateResult, error) {
	if err := req.validate(); err != nil {
		return CreateResult{}, err
	}
	if err := s.requireCommitted(ctx, req.PipelineRevision); err != nil {
		return CreateResult{}, err
	}
	draft := Intent{
		Origin:                      OriginExplicit,
		OperationID:                 req.OperationID,
		InputBindingSubjectIdentity: req.InputBindingSubjectIdentity,
		PipelineRevision:            req.PipelineRevision,
	}
	stored, created, err := s.store.CreateExplicit(ctx, draft)
	if err != nil {
		return CreateResult{}, err
	}
	if !created && !sameExplicitSemantics(stored, draft) {
		return CreateResult{}, newError(ps.CodeOperationConflict,
			"operation %q is already bound to intent %q with different semantics", req.OperationID, stored.ID)
	}
	return CreateResult{Intent: stored, Created: created}, nil
}

// AttachRunID attaches an externally assigned RunID to an existing intent.
// Re-attaching the same RunID converges; a different RunID fails with
// CodeRunIDConflict and leaves the existing assignment unchanged.
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
