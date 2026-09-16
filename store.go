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

// PrepareUnvalidated runs the resolver-INDEPENDENT portion of Prepare: strict
// parse, canonicalization, and digest. It performs NO semantic validation and
// calls NO resolvers.
//
// It exists so a durable store can compute the canonical body digest — the
// operation-replay and content-convergence identity — WITHOUT invoking the
// external resolvers. Idempotent replay and OperationID conflict detection are
// therefore decided on canonical-equivalent body semantics, never on raw
// request bytes, and never depend on external resolver availability. Genuinely
// new operations still go through full Prepare (with validation) before minting.
func PrepareUnvalidated(body []byte) (*Prepared, error) {
	c, err := Parse(body)
	if err != nil {
		return nil, err
	}
	// Fail closed on any PRESENT reserved capability BEFORE the canonical
	// identity is derived. The canonical form intentionally omits
	// tool_profile_digest, so without this a body carrying it would produce the
	// same replay/convergence identity as one without it and could
	// success-short-circuit before the full validation that rejects it. This is a
	// pure structural check (no resolver), so it runs on the replay, convergence,
	// and new-mint paths alike.
	if err := rejectReservedCapabilities(c); err != nil {
		return nil, err
	}
	return prepareCanonical(c)
}

// rejectReservedCapabilities fails closed on any known-but-inactive reserved
// capability that must not participate in the resolver-independent replay /
// convergence identity. In v1 the only such capability is tool_profile_digest
// (TP-R1/TP-R2): PRESENT (a non-nil pointer) is rejected — both an empty-string
// and a non-empty value — while ABSENT (nil) is the sole accepted state and is
// the only one that proceeds into the canonical identity.
func rejectReservedCapabilities(c *PipelineContract) error {
	for i := range c.Nodes {
		if c.Nodes[i].ToolProfileDigest != nil {
			return newErrDetail(CodeUnsupportedCapability, "TP-R1/TP-R2",
				"node %q sets reserved tool_profile_digest; ToolProfile pinning is not an active v1 capability", c.Nodes[i].NodeID)
		}
	}
	return nil
}

// Prepare runs the full pre-persistence pipeline: strict parse, semantic
// validation, canonicalization, and digest. Any failure is returned before the
// caller performs any persistence. Store implementations call Prepare before
// touching durable state (for genuinely new operations).
func Prepare(ctx context.Context, body []byte, res Resolvers) (*Prepared, error) {
	c, err := Parse(body)
	if err != nil {
		return nil, err
	}
	if err := Validate(ctx, c, res); err != nil {
		return nil, err
	}
	return prepareCanonical(c)
}

// prepareCanonical canonicalizes a parsed contract, re-parses the canonical form
// so the stored/returned Contract is exactly what the digest covers (and to
// confirm canonicalization is idempotent), and computes the digest. It is the
// shared, resolver-independent tail of Prepare and PrepareUnvalidated.
func prepareCanonical(c *PipelineContract) (*Prepared, error) {
	canon := Canonicalize(c)
	cc, err := Parse(canon)
	if err != nil {
		return nil, newErr(CodeInvalidContract, "canonical form failed re-parse: %v", err)
	}
	return &Prepared{Contract: cc, Canonical: canon, Digest: DigestCanonical(canon)}, nil
}
