package intent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	ps "github.com/HeaInSeo/PipelineStore"
)

// SQLiteStore is the J1 durable Store for intents, automatic uniqueness, the
// explicit operation ledger, RunID attachment and blockers, on an SQLite
// database reached through database/sql. It lives in this package because
// Store's raw writes are unexported; it does not register or import a driver
// (sqlitestore.Store.IntentStore wires it onto the existing PipelineStore
// SQLite file).
//
// Every write runs in its own BEGIN IMMEDIATE transaction, so concurrent
// writers on the same file (including separate handles or processes) are
// serialized by SQLite and each call commits all of its writes or none of them.
// The automatic key, explicit OperationID and attached RunID are additionally
// guarded by unique indexes.
//
// Scope boundary (J1): the policy lifecycle log is NOT persisted.
// PolicyTransition carries a caller-supplied opaque PublicationPosition whose
// durable encoding is still open, so GetPolicy, PolicyLog, GetPolicyMembership,
// AppendPolicyTransition and createAutomaticAdmitted fail closed with
// ps.CodeUnsupportedCapability instead of reporting a missing policy. As a
// consequence Service.AdmitAutomatic does not admit on this store.
type SQLiteStore struct {
	db *sql.DB

	// fault, when set by tests, is called at each staged step of a write and
	// aborts the write (rolled back) when it returns an error.
	fault func(step string) error
}

var _ Store = (*SQLiteStore)(nil)

// sqliteBusyTimeout is how long a call waits for another writer's lock (ms).
const sqliteBusyTimeout = 5000

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS intents (
	intent_id                      TEXT NOT NULL PRIMARY KEY,
	origin                         TEXT NOT NULL,
	auto_run_policy_id             TEXT NOT NULL,
	auto_run_policy_revision       TEXT NOT NULL,
	operation_id                   TEXT NOT NULL,
	input_binding_subject_identity TEXT NOT NULL,
	pipeline_id                    TEXT NOT NULL,
	pipeline_revision_id           TEXT NOT NULL,
	run_id                         TEXT NOT NULL,
	admission_epoch                INTEGER NOT NULL,
	blocker_policy_disable         TEXT NOT NULL,
	blocker_campaign_pause         TEXT NOT NULL,
	blocker_authorization          TEXT NOT NULL,
	blocker_materialization_prereq TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS intents_auto_key ON intents (auto_run_policy_id, input_binding_subject_identity)
	WHERE origin = 'AUTOMATIC';
CREATE UNIQUE INDEX IF NOT EXISTS intents_operation_id ON intents (operation_id) WHERE origin = 'EXPLICIT';
CREATE UNIQUE INDEX IF NOT EXISTS intents_run_id ON intents (run_id) WHERE run_id <> '';
`

const intentColumns = `intent_id, origin, auto_run_policy_id, auto_run_policy_revision, operation_id,
	input_binding_subject_identity, pipeline_id, pipeline_revision_id, run_id, admission_epoch,
	blocker_policy_disable, blocker_campaign_pause, blocker_authorization, blocker_materialization_prereq`

// NewSQLiteStore creates the intent tables in db if needed and returns the
// store. db must be an SQLite database; the caller owns and closes it.
func NewSQLiteStore(ctx context.Context, db *sql.DB) (*SQLiteStore, error) {
	if db == nil {
		return nil, errors.New("intent: NewSQLiteStore requires a database")
	}
	s := &SQLiteStore{db: db}
	if err := s.write(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, sqliteSchema)
		return err
	}); err != nil {
		return nil, fmt.Errorf("intent: init sqlite schema: %w", err)
	}
	return s, nil
}

func (s *SQLiteStore) inject(step string) error {
	if s.fault == nil {
		return nil
	}
	return s.fault(step)
}

// conn returns a pooled connection with the busy timeout applied, so a call
// waits for a concurrent writer instead of failing with SQLITE_BUSY.
func (s *SQLiteStore) conn(ctx context.Context) (*sql.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", sqliteBusyTimeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// write runs fn inside one BEGIN IMMEDIATE transaction and commits only if fn
// succeeds; any error rolls every write of the call back.
func (s *SQLiteStore) write(ctx context.Context, fn func(conn *sql.Conn) error) (err error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

// read runs fn on a connection outside an explicit transaction; each query is
// a single atomic statement.
func (s *SQLiteStore) read(ctx context.Context, fn func(conn *sql.Conn) error) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return fn(conn)
}

// queryIntent returns the single intent selected by where/args, with
// found=false when there is none.
func queryIntent(ctx context.Context, conn *sql.Conn, where string, args ...any) (Intent, bool, error) {
	var (
		in    Intent
		epoch int64
	)
	err := conn.QueryRowContext(ctx, "SELECT "+intentColumns+" FROM intents WHERE "+where, args...).Scan(
		&in.ID, &in.Origin, &in.AutoRunPolicyID, &in.AutoRunPolicyRevision, &in.OperationID,
		&in.InputBindingSubjectIdentity, &in.PipelineRevision.PipelineID, &in.PipelineRevision.RevisionID, &in.RunID, &epoch,
		&in.Blockers.PolicyDisable, &in.Blockers.CampaignPause, &in.Blockers.Authorization, &in.Blockers.MaterializationPrereq,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Intent{}, false, nil
	}
	if err != nil {
		return Intent{}, false, fmt.Errorf("intent: read intent: %w", err)
	}
	if epoch < 0 {
		return Intent{}, false, newError(ps.CodeIntegrity, "intent %q has negative admission epoch %d", in.ID, epoch)
	}
	in.AdmissionEpoch = uint64(epoch)
	return in, true, nil
}

func insertIntent(ctx context.Context, conn *sql.Conn, in Intent) error {
	if in.AdmissionEpoch > 1<<63-1 {
		return newError(ps.CodeInvalidContract, "admission epoch %d does not fit the store", in.AdmissionEpoch)
	}
	_, err := conn.ExecContext(ctx, "INSERT INTO intents ("+intentColumns+") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		string(in.ID), string(in.Origin), in.AutoRunPolicyID, in.AutoRunPolicyRevision, in.OperationID,
		in.InputBindingSubjectIdentity, in.PipelineRevision.PipelineID, string(in.PipelineRevision.RevisionID), string(in.RunID),
		int64(in.AdmissionEpoch),
		string(in.Blockers.PolicyDisable), string(in.Blockers.CampaignPause), string(in.Blockers.Authorization),
		string(in.Blockers.MaterializationPrereq))
	if err != nil {
		return fmt.Errorf("intent: insert intent: %w", err)
	}
	return nil
}

const (
	whereAuto     = "origin = 'AUTOMATIC' AND auto_run_policy_id = ? AND input_binding_subject_identity = ?"
	whereExplicit = "origin = 'EXPLICIT' AND operation_id = ?"
	whereID       = "intent_id = ?"
	whereRunID    = "run_id = ?"
)

// LookupAutomatic implements Store.
func (s *SQLiteStore) LookupAutomatic(ctx context.Context, policyID, subject string) (stored Intent, found bool, err error) {
	err = s.read(ctx, func(conn *sql.Conn) error {
		stored, found, err = queryIntent(ctx, conn, whereAuto, policyID, subject)
		return err
	})
	return stored, found, err
}

// LookupExplicit implements Store.
func (s *SQLiteStore) LookupExplicit(ctx context.Context, operationID string) (stored Intent, found bool, err error) {
	err = s.read(ctx, func(conn *sql.Conn) error {
		stored, found, err = queryIntent(ctx, conn, whereExplicit, operationID)
		return err
	})
	return stored, found, err
}

// createAutomatic implements Store.
func (s *SQLiteStore) createAutomatic(ctx context.Context, draft Intent) (stored Intent, created bool, err error) {
	err = s.write(ctx, func(conn *sql.Conn) error {
		existing, found, err := queryIntent(ctx, conn, whereAuto, draft.AutoRunPolicyID, draft.InputBindingSubjectIdentity)
		if err != nil {
			return err
		}
		if found {
			stored = existing
			return nil
		}
		draft.ID = ID(uuid.NewString())
		if err := s.inject(stepIndexStaged); err != nil {
			return err
		}
		if err := insertIntent(ctx, conn, draft); err != nil {
			return err
		}
		if err := s.inject(stepIntentStaged); err != nil {
			return err
		}
		stored, created = draft, true
		return nil
	})
	if err != nil {
		return Intent{}, false, err
	}
	return stored, created, nil
}

// createExplicit implements Store.
func (s *SQLiteStore) createExplicit(ctx context.Context, draft Intent) (stored Intent, created bool, err error) {
	if err := checkExplicitDraft(draft); err != nil {
		return Intent{}, false, err
	}
	err = s.write(ctx, func(conn *sql.Conn) error {
		existing, found, err := queryIntent(ctx, conn, whereExplicit, draft.OperationID)
		if err != nil {
			return err
		}
		if found {
			stored = existing
			return nil
		}
		draft.ID = ID(uuid.NewString())
		if err := s.inject(stepLedgerStaged); err != nil {
			return err
		}
		if err := insertIntent(ctx, conn, draft); err != nil {
			return err
		}
		if err := s.inject(stepIntentStaged); err != nil {
			return err
		}
		stored, created = draft, true
		return nil
	})
	if err != nil {
		return Intent{}, false, err
	}
	return stored, created, nil
}

// AttachRunID implements Store.
func (s *SQLiteStore) AttachRunID(ctx context.Context, id ID, run RunID) (out Intent, err error) {
	err = s.write(ctx, func(conn *sql.Conn) error {
		in, found, err := queryIntent(ctx, conn, whereID, string(id))
		if err != nil {
			return err
		}
		if !found {
			return newError(ps.CodeNotFound, "intent %q not found", id)
		}
		if in.RunID == run {
			out = in
			return nil
		}
		if in.RunID != "" {
			return newError(CodeRunIDConflict, "intent %q already has RunID %q; refusing %q", id, in.RunID, run)
		}
		owner, taken, err := queryIntent(ctx, conn, whereRunID, string(run))
		if err != nil {
			return err
		}
		if taken {
			return newError(CodeRunIDConflict, "RunID %q is already attached to intent %q; refusing intent %q", run, owner.ID, id)
		}
		if err := s.inject(stepRunIndexStaged); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, "UPDATE intents SET run_id = ? WHERE intent_id = ?", string(run), string(id)); err != nil {
			return fmt.Errorf("intent: attach RunID: %w", err)
		}
		if err := s.inject(stepIntentStaged); err != nil {
			return err
		}
		in.RunID = run
		out = in
		return nil
	})
	if err != nil {
		return Intent{}, err
	}
	return out, nil
}

// Get implements Store.
func (s *SQLiteStore) Get(ctx context.Context, id ID) (out Intent, err error) {
	err = s.read(ctx, func(conn *sql.Conn) error {
		in, found, err := queryIntent(ctx, conn, whereID, string(id))
		if err != nil {
			return err
		}
		if !found {
			return newError(ps.CodeNotFound, "intent %q not found", id)
		}
		out = in
		return nil
	})
	if err != nil {
		return Intent{}, err
	}
	return out, nil
}

// blockerColumns maps each blocker owner to its column; it mirrors Blockers.slot.
var blockerColumns = map[BlockerOwner]string{
	BlockerPolicyDisable:         "blocker_policy_disable",
	BlockerCampaignPause:         "blocker_campaign_pause",
	BlockerAuthorization:         "blocker_authorization",
	BlockerMaterializationPrereq: "blocker_materialization_prereq",
}

// UpdateBlocker implements Store.
func (s *SQLiteStore) UpdateBlocker(ctx context.Context, id ID, owner BlockerOwner, state BlockerState) (out Intent, err error) {
	err = s.write(ctx, func(conn *sql.Conn) error {
		in, found, err := queryIntent(ctx, conn, whereID, string(id))
		if err != nil {
			return err
		}
		if !found {
			return newError(ps.CodeNotFound, "intent %q not found", id)
		}
		slot := in.Blockers.slot(owner)
		column, known := blockerColumns[owner]
		if slot == nil || !known {
			return newError(CodeInvalidTransition, "unknown blocker owner %q", owner)
		}
		if *slot == state {
			out = in
			return nil
		}
		if err := s.inject(stepBlockerStaged); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, "UPDATE intents SET "+column+" = ? WHERE intent_id = ?", string(state), string(id)); err != nil {
			return fmt.Errorf("intent: update blocker: %w", err)
		}
		*slot = state
		out = in
		return nil
	})
	if err != nil {
		return Intent{}, err
	}
	return out, nil
}

// errPolicyLogNotPersisted is returned by every policy-lifecycle method.
func errPolicyLogNotPersisted(policyID string) error {
	return newError(ps.CodeUnsupportedCapability,
		"policy %q: the J1 SQLite intent store does not persist the policy lifecycle log (PublicationPosition/frontier encoding is open)",
		policyID)
}

// GetPolicy implements Store; it always fails closed (see SQLiteStore).
func (s *SQLiteStore) GetPolicy(_ context.Context, policyID string) (PolicyRecord, bool, error) {
	return PolicyRecord{}, false, errPolicyLogNotPersisted(policyID)
}

// PolicyLog implements Store; it always fails closed (see SQLiteStore).
func (s *SQLiteStore) PolicyLog(_ context.Context, policyID string) ([]PolicyTransition, error) {
	return nil, errPolicyLogNotPersisted(policyID)
}

// GetPolicyMembership implements Store; it always fails closed (see SQLiteStore).
func (s *SQLiteStore) GetPolicyMembership(_ context.Context, policyID string) (PolicyRecord, PolicyMembership, bool, error) {
	return PolicyRecord{}, PolicyMembership{}, false, errPolicyLogNotPersisted(policyID)
}

// AppendPolicyTransition implements Store; it always fails closed and writes
// nothing (see SQLiteStore).
func (s *SQLiteStore) AppendPolicyTransition(_ context.Context, policyID string, _ uint64, _ PolicyTransition, _ bool) (PolicyRecord, PolicyMembership, error) {
	return PolicyRecord{}, PolicyMembership{}, errPolicyLogNotPersisted(policyID)
}

// createAutomaticAdmitted implements Store; it always fails closed and writes
// nothing, because the admission check needs the policy record (see SQLiteStore).
func (s *SQLiteStore) createAutomaticAdmitted(_ context.Context, draft Intent, _ uint64) (Intent, bool, error) {
	return Intent{}, false, errPolicyLogNotPersisted(draft.AutoRunPolicyID)
}
