package pipelinestore

import "context"

// PipelineRevisionID is the opaque, immutable identity of a committed revision.
//
// It is allocated and owned exclusively by PipelineStore. It is NOT derived from
// the timestamp, caller, path, stableRef, contract digest, or any request input;
// callers must treat it as opaque.
type PipelineRevisionID string

// PipelineRevision is an immutable committed revision of a pipeline contract.
type PipelineRevision struct {
	PipelineID                string
	RevisionID                PipelineRevisionID
	ContractDigest            string
	SemanticDerivationVersion string
	CanonicalizationVersion   string
	// CanonicalBody is the exact canonical_json_v1 bytes the digest was computed
	// over. It is the durable source of truth for the revision body.
	CanonicalBody []byte
	// Contract is the decoded contract (recomputed on exact read; digest-verified).
	Contract *PipelineContract
}

// CommitRequest is a durable, idempotent commit request.
type CommitRequest struct {
	// OperationID is the idempotency key. The same OperationID with the same
	// PipelineID and body returns the same revision; with a different immutable
	// body it is a conflict with zero partial mutation.
	OperationID string
	// PipelineID scopes the revision. The same body under a different PipelineID
	// is allowed and yields a distinct revision identity.
	PipelineID string
	// Contract is the raw v1 pipeline-contract JSON body.
	Contract []byte
}

// CommitResult is the outcome of a commit.
type CommitResult struct {
	Revision *PipelineRevision
	// Created is true when a new revision was minted; false when an existing
	// revision was returned (idempotent replay or content convergence).
	Created bool
}

// Store is the storage-topology boundary for revision semantics. Implementations
// must provide durable, crash-safe, atomic commits and exact reads.
type Store interface {
	// Commit validates and durably commits the contract, honoring idempotency and
	// content convergence. A new RevisionID is not externally observable before the
	// backend's durable commit boundary succeeds.
	Commit(ctx context.Context, req CommitRequest) (CommitResult, error)
	// GetRevision returns the immutable revision identified exactly by
	// (pipelineID, revisionID). It recomputes the digest under the stored
	// canonicalization version and fails closed on any mismatch.
	GetRevision(ctx context.Context, pipelineID string, revisionID PipelineRevisionID) (*PipelineRevision, error)
	// Close releases the underlying resources.
	Close() error
}

// Prepared is the result of parse+validate+canonicalize+digest for a request.
type Prepared struct {
	Contract  *PipelineContract
	Canonical []byte
	Digest    string
}

// Prepare runs the full pre-persistence pipeline: strict parse, semantic
// validation, canonicalization, and digest. Any failure is returned before the
// caller performs any persistence. Store implementations call Prepare before
// touching durable state.
func Prepare(ctx context.Context, body []byte, res Resolvers) (*Prepared, error) {
	c, err := Parse(body)
	if err != nil {
		return nil, err
	}
	if err := Validate(ctx, c, res); err != nil {
		return nil, err
	}
	canon := Canonicalize(c)
	// Re-parse the canonical form so the stored/returned Contract is exactly what
	// the digest covers, and confirm the canonicalization is idempotent.
	cc, err := Parse(canon)
	if err != nil {
		return nil, newErr(CodeInvalidContract, "canonical form failed re-parse: %v", err)
	}
	return &Prepared{Contract: cc, Canonical: canon, Digest: DigestCanonical(canon)}, nil
}
