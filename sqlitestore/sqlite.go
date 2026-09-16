// Package sqlitestore is the durable, crash-safe SQLite-backed implementation of
// the PipelineStore Store interface. It uses the pure-Go modernc.org/sqlite
// driver so the build is cgo-free.
//
// A single revision commit atomically transitions three tables — the revision
// record, the operation ledger (idempotency), and the digest index (content
// convergence) — inside one transaction. Success is never acknowledged before
// the transaction's durable commit boundary, and a freshly allocated RevisionID
// is never externally observable before that boundary succeeds.
package sqlitestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	ps "github.com/HeaInSeo/PipelineStore"
)

// Store is a durable SQLite-backed Store.
type Store struct {
	db  *sql.DB
	res ps.Resolvers

	// faultBeforeCommit, when non-nil and returning a non-nil error, aborts the
	// transaction just before its durable commit boundary. It exists to let tests
	// inject a crash/fault at the most dangerous point; it is never set in
	// production.
	faultBeforeCommit func() error
}

const schema = `
CREATE TABLE IF NOT EXISTS revisions (
	pipeline_id                 TEXT NOT NULL,
	revision_id                 TEXT NOT NULL,
	contract_digest             TEXT NOT NULL,
	semantic_derivation_version TEXT NOT NULL,
	canonicalization_version    TEXT NOT NULL,
	canonical_body              BLOB NOT NULL,
	PRIMARY KEY (pipeline_id, revision_id)
);
CREATE TABLE IF NOT EXISTS operations (
	operation_id        TEXT NOT NULL PRIMARY KEY,
	pipeline_id         TEXT NOT NULL,
	revision_id         TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS digest_index (
	pipeline_id              TEXT NOT NULL,
	canonicalization_version TEXT NOT NULL,
	contract_digest          TEXT NOT NULL,
	revision_id              TEXT NOT NULL,
	PRIMARY KEY (pipeline_id, canonicalization_version, contract_digest)
);
`

// Open opens (creating if needed) a durable store at dbPath using the given
// read-only resolvers for validation.
func Open(dbPath string, res ps.Resolvers) (*Store, error) {
	if res.ToolFunction == nil || res.Sori == nil {
		return nil, fmt.Errorf("sqlitestore: both ToolFunction and Sori resolvers are required")
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: open: %w", err)
	}
	// Serialize access: SQLite is single-writer, and a single connection keeps
	// commit semantics and the fault-injection point unambiguous.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlitestore: init schema: %w", err)
	}
	return &Store{db: db, res: res}, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error { return s.db.Close() }

// requestFingerprint is the immutable-body identity for an operation:
// SHA256(pipeline_id \0 contract_digest). Same operation_id + same fingerprint is
// an idempotent replay; a different fingerprint under the same operation_id is a
// conflict.
func requestFingerprint(pipelineID, digest string) string {
	h := sha256.New()
	h.Write([]byte(pipelineID))
	h.Write([]byte{0})
	h.Write([]byte(digest))
	return hex.EncodeToString(h.Sum(nil))
}

// Commit implements ps.Store.
func (s *Store) Commit(ctx context.Context, req ps.CommitRequest) (ps.CommitResult, error) {
	if req.OperationID == "" {
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: empty operation_id")
	}
	if req.PipelineID == "" {
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: empty pipeline_id")
	}

	// Resolver-INDEPENDENT parse + canonicalization + digest BEFORE any
	// persistence and BEFORE any external resolver call. This derives the
	// operation-replay / content-convergence identity from canonical-equivalent
	// body semantics (not raw request bytes) so that a retry of an
	// already-committed operation can be replayed even when the external
	// resolvers are now unavailable or the catalog has drifted. Full resolver
	// validation is deferred to genuinely new operations (step 3). Any parse/
	// canonicalization failure returns here with zero mutation.
	pre, err := ps.PrepareUnvalidated(req.Contract)
	if err != nil {
		return ps.CommitResult{}, err
	}
	fingerprint := requestFingerprint(req.PipelineID, pre.Digest)

	// Begin the transaction for the atomic three-table transition (revision +
	// operation ledger + digest index). modernc.org/sqlite opens a deferred
	// transaction here; writes are fully serialized by SetMaxOpenConns(1), and
	// the operations/digest_index primary keys are hard duplicate guards, so a
	// deferred begin is sufficient for atomicity and idempotency.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 1. Idempotency ledger: same operation_id?
	var (
		existingRevID string
		existingFP    string
	)
	err = tx.QueryRowContext(ctx,
		`SELECT revision_id, request_fingerprint FROM operations WHERE operation_id = ?`,
		req.OperationID,
	).Scan(&existingRevID, &existingFP)
	switch {
	case err == nil:
		if existingFP != fingerprint {
			// Same operation_id, different immutable body: conflict, zero mutation.
			return ps.CommitResult{}, &ps.Error{
				Code: ps.CodeOperationConflict,
				Msg:  fmt.Sprintf("operation_id %q already committed a different body", req.OperationID),
			}
		}
		rev, rerr := loadRevisionTx(ctx, tx, req.PipelineID, existingRevID)
		if rerr != nil {
			return ps.CommitResult{}, rerr
		}
		if cerr := tx.Commit(); cerr != nil {
			return ps.CommitResult{}, fmt.Errorf("sqlitestore: commit(replay): %w", cerr)
		}
		committed = true
		return ps.CommitResult{Revision: rev, Created: false}, nil
	case errors.Is(err, sql.ErrNoRows):
		// New operation; continue.
	default:
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: operation lookup: %w", err)
	}

	// 2. Content convergence: same PipelineID + canon version + digest already
	// present? Return the existing revision and record this operation against it.
	var convergedRevID string
	err = tx.QueryRowContext(ctx,
		`SELECT revision_id FROM digest_index WHERE pipeline_id = ? AND canonicalization_version = ? AND contract_digest = ?`,
		req.PipelineID, pre.Contract.CanonicalizationVersion, pre.Digest,
	).Scan(&convergedRevID)
	switch {
	case err == nil:
		if _, ierr := tx.ExecContext(ctx,
			`INSERT INTO operations (operation_id, pipeline_id, revision_id, request_fingerprint) VALUES (?, ?, ?, ?)`,
			req.OperationID, req.PipelineID, convergedRevID, fingerprint,
		); ierr != nil {
			return ps.CommitResult{}, fmt.Errorf("sqlitestore: record converged operation: %w", ierr)
		}
		rev, rerr := loadRevisionTx(ctx, tx, req.PipelineID, convergedRevID)
		if rerr != nil {
			return ps.CommitResult{}, rerr
		}
		if s.faultBeforeCommit != nil {
			if ferr := s.faultBeforeCommit(); ferr != nil {
				return ps.CommitResult{}, fmt.Errorf("sqlitestore: injected fault before commit: %w", ferr)
			}
		}
		if cerr := tx.Commit(); cerr != nil {
			return ps.CommitResult{}, fmt.Errorf("sqlitestore: commit(converge): %w", cerr)
		}
		committed = true
		return ps.CommitResult{Revision: rev, Created: false}, nil
	case errors.Is(err, sql.ErrNoRows):
		// No convergence; mint a new revision.
	default:
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: digest index lookup: %w", err)
	}

	// 3. Genuinely new operation (neither an idempotent replay nor a content
	// convergence): NOW run the full resolver validation before minting. This is
	// the only path that calls the external resolvers. A validation failure
	// returns here with zero mutation (the transaction is rolled back by the
	// deferred guard). prep.Digest is identical to pre.Digest (same
	// canonicalization), so the fingerprint recorded below stays consistent.
	prep, err := ps.Prepare(ctx, req.Contract, s.res)
	if err != nil {
		return ps.CommitResult{}, err
	}

	// Mint a fresh, opaque RevisionID and stage the atomic three-table write.
	revID := uuid.NewString()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO revisions (pipeline_id, revision_id, contract_digest, semantic_derivation_version, canonicalization_version, canonical_body)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		req.PipelineID, revID, prep.Digest,
		prep.Contract.SemanticDerivationVersion, prep.Contract.CanonicalizationVersion, prep.Canonical,
	); err != nil {
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: insert revision: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO operations (operation_id, pipeline_id, revision_id, request_fingerprint) VALUES (?, ?, ?, ?)`,
		req.OperationID, req.PipelineID, revID, fingerprint,
	); err != nil {
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: insert operation: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO digest_index (pipeline_id, canonicalization_version, contract_digest, revision_id) VALUES (?, ?, ?, ?)`,
		req.PipelineID, prep.Contract.CanonicalizationVersion, prep.Digest, revID,
	); err != nil {
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: insert digest index: %w", err)
	}

	// Fault-injection point: a crash here must leave NO revision/operation/index
	// row and NO externally observable RevisionID.
	if s.faultBeforeCommit != nil {
		if ferr := s.faultBeforeCommit(); ferr != nil {
			return ps.CommitResult{}, fmt.Errorf("sqlitestore: injected fault before commit: %w", ferr)
		}
	}

	if err := tx.Commit(); err != nil {
		return ps.CommitResult{}, fmt.Errorf("sqlitestore: commit: %w", err)
	}
	committed = true

	return ps.CommitResult{
		Revision: &ps.PipelineRevision{
			PipelineID:                req.PipelineID,
			RevisionID:                ps.PipelineRevisionID(revID),
			ContractDigest:            prep.Digest,
			SemanticDerivationVersion: prep.Contract.SemanticDerivationVersion,
			CanonicalizationVersion:   prep.Contract.CanonicalizationVersion,
			CanonicalBody:             append([]byte(nil), prep.Canonical...),
			Contract:                  prep.Contract,
		},
		Created: true,
	}, nil
}

// GetRevision implements ps.Store. It fails closed on any integrity mismatch.
func (s *Store) GetRevision(ctx context.Context, pipelineID string, revisionID ps.PipelineRevisionID) (*ps.PipelineRevision, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: begin(read): %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	return loadRevisionTx(ctx, tx, pipelineID, string(revisionID))
}

// loadRevisionTx reads a revision within tx, recomputes the digest under the
// stored canonicalization version, and fails closed on version or digest
// mismatch (stored-body corruption, stored-digest mismatch, or unsupported
// stored canon version).
func loadRevisionTx(ctx context.Context, tx *sql.Tx, pipelineID, revisionID string) (*ps.PipelineRevision, error) {
	var (
		digest   string
		semVer   string
		canonVer string
		body     []byte
	)
	err := tx.QueryRowContext(ctx,
		`SELECT contract_digest, semantic_derivation_version, canonicalization_version, canonical_body
		 FROM revisions WHERE pipeline_id = ? AND revision_id = ?`,
		pipelineID, revisionID,
	).Scan(&digest, &semVer, &canonVer, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &ps.Error{Code: ps.CodeNotFound, Msg: fmt.Sprintf("no revision %q for pipeline %q", revisionID, pipelineID)}
	}
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: read revision: %w", err)
	}

	// Fail closed on an unsupported stored canonicalization version rather than
	// silently reinterpreting the body under a different basis.
	if canonVer != ps.ContractVersionV1 {
		return nil, &ps.Error{Code: ps.CodeIntegrity, Msg: fmt.Sprintf("stored canonicalization_version %q is not supported", canonVer)}
	}
	// Recompute the digest under the stored canon version and verify.
	if recomputed := ps.DigestCanonical(body); recomputed != digest {
		return nil, &ps.Error{Code: ps.CodeIntegrity, Msg: "stored body/digest integrity check failed"}
	}
	contract, err := ps.Parse(body)
	if err != nil {
		return nil, &ps.Error{Code: ps.CodeIntegrity, Msg: fmt.Sprintf("stored body failed strict re-parse: %v", err)}
	}
	// The redundant version columns must agree with the digest-protected body;
	// fail closed on any drift (the body is authoritative and integrity-checked
	// above, the columns are convenience indexes). This mirrors the
	// canonicalization_version gate and keeps §8 "fail closed on any mismatch"
	// symmetric across both stored version fields.
	if semVer != contract.SemanticDerivationVersion {
		return nil, &ps.Error{Code: ps.CodeIntegrity, Msg: "stored semantic_derivation_version disagrees with digest-protected body"}
	}
	if canonVer != contract.CanonicalizationVersion {
		return nil, &ps.Error{Code: ps.CodeIntegrity, Msg: "stored canonicalization_version disagrees with digest-protected body"}
	}

	return &ps.PipelineRevision{
		PipelineID:                pipelineID,
		RevisionID:                ps.PipelineRevisionID(revisionID),
		ContractDigest:            digest,
		SemanticDerivationVersion: semVer,
		CanonicalizationVersion:   canonVer,
		CanonicalBody:             body,
		Contract:                  contract,
	}, nil
}
