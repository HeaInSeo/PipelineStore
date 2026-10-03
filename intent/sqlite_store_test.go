package intent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"

	ps "github.com/HeaInSeo/PipelineStore"
)

func sqlitePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "pipelinestore.db")
}

// openSQLite opens one handle on path, configured like sqlitestore.Open (a
// single connection per handle).
func openSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newSQLiteStore(t *testing.T, db *sql.DB) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(context.Background(), db)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	return s
}

// reopenSQLite closes db and returns a store on a new handle to the same file,
// as a restarted process would see it.
func reopenSQLite(t *testing.T, db *sql.DB, path string) *SQLiteStore {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return newSQLiteStore(t, openSQLite(t, path))
}

// A duplicate automatic trigger and an explicit replay after a restart return
// the intent recorded before it, through the public Service paths.
func TestSQLiteStore_ReplayAfterReopenReturnsSameIntent(t *testing.T) {
	ctx := context.Background()
	path := sqlitePath(t)
	db := openSQLite(t, path)
	s := newSQLiteStore(t, db)
	auto := mustCreateAutomatic(t, s, "subject-1")
	svc, err := NewService(s, newCommitted(rev1, rev2))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	explicit, err := svc.CreateExplicit(ctx, explicitReq())
	if err != nil || !explicit.Created {
		t.Fatalf("CreateExplicit: %+v err=%v", explicit, err)
	}

	r := reopenSQLite(t, db, path)
	// The revision reader is now empty: a replay must be resolved from the store.
	svc, err = NewService(r, newCommitted())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	replay, err := svc.createAutomatic(ctx, autoReq())
	if err != nil || replay.Created || replay.Intent != auto || replay.Divergence != nil {
		t.Fatalf("automatic replay after reopen: %+v err=%v, want frozen %+v", replay, err, auto)
	}
	again, err := svc.CreateExplicit(ctx, explicitReq())
	if err != nil || again.Created || again.Intent != explicit.Intent {
		t.Fatalf("explicit replay after reopen: %+v err=%v, want %+v", again, err, explicit.Intent)
	}
	changed := explicitReq()
	changed.InputBindingSubjectIdentity = "subject-2"
	if _, err := svc.CreateExplicit(ctx, changed); ps.CodeOf(err) != ps.CodeOperationConflict {
		t.Fatalf("changed explicit semantics after reopen: err=%v, want %s", err, ps.CodeOperationConflict)
	}
}

// RunID attachment, its conflicts and blockers survive a restart.
func TestSQLiteStore_RunIDAndBlockersSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := sqlitePath(t)
	db := openSQLite(t, path)
	s := newSQLiteStore(t, db)
	a := mustCreateAutomatic(t, s, "subject-a")
	b := mustCreateAutomatic(t, s, "subject-b")
	if _, err := s.AttachRunID(ctx, a.ID, "run-1"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	held, err := s.UpdateBlocker(ctx, b.ID, BlockerMaterializationPrereq, BlockerBlocked)
	if err != nil {
		t.Fatalf("block: %v", err)
	}

	r := reopenSQLite(t, db, path)
	if _, err := r.AttachRunID(ctx, a.ID, "run-2"); ps.CodeOf(err) != CodeRunIDConflict {
		t.Fatalf("different RunID after reopen: err=%v, want %s", err, CodeRunIDConflict)
	}
	if _, err := r.AttachRunID(ctx, b.ID, "run-1"); ps.CodeOf(err) != CodeRunIDConflict {
		t.Fatalf("RunID owned by another intent after reopen: err=%v, want %s", err, CodeRunIDConflict)
	}
	if got, err := r.AttachRunID(ctx, a.ID, "run-1"); err != nil || got.RunID != "run-1" {
		t.Fatalf("same RunID after reopen: %+v err=%v", got, err)
	}
	if got := mustGetIntent(t, r, b.ID); got != held {
		t.Fatalf("blockers after reopen: %+v want %+v", got, held)
	}
}

// Concurrent automatic creates for one uniqueness domain, spread over two
// independent handles on the same file, record exactly one intent.
func TestSQLiteStore_ConcurrentCreateAcrossHandlesOneWinner(t *testing.T) {
	path := sqlitePath(t)
	stores := []*SQLiteStore{newSQLiteStore(t, openSQLite(t, path)), newSQLiteStore(t, openSQLite(t, path))}
	const n = 16
	results := make([]Intent, n)
	created := make([]bool, n)
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], created[i], errs[i] = stores[i%2].createAutomatic(context.Background(), automaticDraft("subject-1"))
		}(i)
	}
	close(start)
	wg.Wait()
	winners := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if created[i] {
			winners++
		}
		if results[i] != results[0] {
			t.Fatalf("caller %d diverged: %+v vs %+v", i, results[i], results[0])
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
	var rows int
	if err := openSQLite(t, path).QueryRow("SELECT COUNT(*) FROM intents").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("intent rows = %d err=%v, want 1", rows, err)
	}
}

// The policy lifecycle log is not persisted in J1: every policy method fails
// closed with CodeUnsupportedCapability and writes nothing, and the public
// admission gate surfaces that error instead of a NotAdmitted verdict.
func TestSQLiteStore_PolicyLogFailsClosed(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteStore(t, openSQLite(t, sqlitePath(t)))
	unsupported := func(name string, err error) {
		t.Helper()
		if ps.CodeOf(err) != ps.CodeUnsupportedCapability {
			t.Fatalf("%s: err=%v, want %s", name, err, ps.CodeUnsupportedCapability)
		}
	}
	_, _, err := s.GetPolicy(ctx, "policy-1")
	unsupported("GetPolicy", err)
	_, err = s.PolicyLog(ctx, "policy-1")
	unsupported("PolicyLog", err)
	_, _, _, err = s.GetPolicyMembership(ctx, "policy-1")
	unsupported("GetPolicyMembership", err)
	_, _, err = s.AppendPolicyTransition(ctx, "policy-1", 0, PolicyTransition{Kind: TransitionActivate}, true)
	unsupported("AppendPolicyTransition", err)
	_, _, err = s.createAutomaticAdmitted(ctx, automaticDraft("subject-1"), 1)
	unsupported("createAutomaticAdmitted", err)

	svc, err := NewService(s, newCommitted(rev1))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	res, err := svc.AdmitAutomatic(ctx, autoReq(), PublicationFacts{Class: OccurrenceNew})
	unsupported("AdmitAutomatic", err)
	if res.Decision != "" {
		t.Fatalf("AdmitAutomatic decision = %q, want none", res.Decision)
	}
	if _, found, err := s.LookupAutomatic(ctx, "policy-1", "subject-1"); err != nil || found {
		t.Fatalf("fail-closed admission wrote an intent: found=%v err=%v", found, err)
	}
}

// dbSnapshot lists every schema object of db with its SQL and, for tables, its
// row count, so a refused open can be checked for zero mutation.
func dbSnapshot(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT type, name, coalesce(sql, '') FROM sqlite_master ORDER BY type, name`)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var snap, tables []string
	for rows.Next() {
		var typ, name, ddl string
		if err := rows.Scan(&typ, &name, &ddl); err != nil {
			t.Fatalf("scan schema: %v", err)
		}
		snap = append(snap, typ+" "+name+" "+ddl)
		if typ == "table" {
			tables = append(tables, name)
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close schema rows: %v", err)
	}
	for _, name := range tables {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM "` + name + `"`).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", name, err)
		}
		snap = append(snap, fmt.Sprintf("rows %s=%d", name, n))
	}
	var version sql.NullInt64
	if err := db.QueryRow(`SELECT version FROM intent_schema_version`).Scan(&version); err == nil {
		snap = append(snap, fmt.Sprintf("intent_schema_version=%d", version.Int64))
	}
	return snap
}

// rebuildIntents returns statements that recreate the intents table and its
// indexes from schema, keeping the stored rows.
func rebuildIntents(schema string) []string {
	return []string{
		"DROP INDEX intents_auto_key",
		"DROP INDEX intents_operation_id",
		"DROP INDEX intents_run_id",
		"ALTER TABLE intents RENAME TO intents_old",
		schema,
		"INSERT INTO intents SELECT * FROM intents_old",
		"DROP TABLE intents_old",
	}
}

// A newer, unknown or missing version record, intent tables without a version
// record, or a version record without its tables all fail the open with an
// explicit code and leave the database exactly as it was: nothing is dropped,
// recreated, migrated or stamped.
func TestSQLiteStore_OpenRefusesUnsupportedSchema(t *testing.T) {
	cases := []struct {
		name  string
		setup []string
		code  ps.Code
	}{
		{"newer-version", []string{"UPDATE intent_schema_version SET version = 2"}, ps.CodeUnsupportedVersion},
		{"unknown-version", []string{"UPDATE intent_schema_version SET version = 0"}, ps.CodeUnsupportedVersion},
		{"missing-version-record", []string{"DELETE FROM intent_schema_version"}, ps.CodeIntegrity},
		{"unversioned-intent-tables", []string{"DROP TABLE intent_schema_version"}, ps.CodeUnsupportedVersion},
		{"version-without-tables", []string{"DROP TABLE intents"}, ps.CodeIntegrity},
		{"version-with-partial-indexes", []string{"DROP INDEX intents_run_id"}, ps.CodeIntegrity},
		{"nonunique-index", []string{
			"DROP INDEX intents_run_id",
			"CREATE INDEX intents_run_id ON intents (run_id) WHERE run_id <> ''",
		}, ps.CodeIntegrity},
		{"non-partial-index", []string{
			"DROP INDEX intents_operation_id",
			"CREATE UNIQUE INDEX intents_operation_id ON intents (operation_id)",
		}, ps.CodeIntegrity},
		{"index-wrong-columns", []string{
			"DROP INDEX intents_auto_key",
			"CREATE UNIQUE INDEX intents_auto_key ON intents (auto_run_policy_id) WHERE origin = 'AUTOMATIC'",
		}, ps.CodeIntegrity},
		{"index-replaced-by-view", []string{
			"DROP INDEX intents_run_id",
			"CREATE VIEW intents_run_id AS SELECT run_id FROM intents",
		}, ps.CodeIntegrity},
		{"index-on-other-table", []string{
			"DROP INDEX intents_run_id",
			"CREATE TABLE intents_other (run_id TEXT NOT NULL)",
			"CREATE UNIQUE INDEX intents_run_id ON intents_other (run_id) WHERE run_id <> ''",
		}, ps.CodeIntegrity},
		{"table-replaced-by-view", []string{
			"ALTER TABLE intents RENAME TO intents_renamed",
			"CREATE VIEW intents AS SELECT * FROM intents_renamed",
		}, ps.CodeIntegrity},
		{"table-extra-column", []string{"ALTER TABLE intents ADD COLUMN extra TEXT NOT NULL DEFAULT ''"}, ps.CodeIntegrity},
		// Extra uniqueness would refuse a second valid intent sharing the subject
		// under another policy or operation.
		{"table-unique-constraint", rebuildIntents(strings.Replace(sqliteSchema,
			"blocker_materialization_prereq TEXT NOT NULL\n)",
			"blocker_materialization_prereq TEXT NOT NULL,\n\tUNIQUE(input_binding_subject_identity)\n)", 1)), ps.CodeIntegrity},
		{"column-unique-constraint", rebuildIntents(strings.Replace(sqliteSchema,
			"input_binding_subject_identity TEXT NOT NULL", "input_binding_subject_identity TEXT NOT NULL UNIQUE", 1)), ps.CodeIntegrity},
		{"extra-unique-index", []string{
			"CREATE UNIQUE INDEX intents_subject ON intents (input_binding_subject_identity)",
		}, ps.CodeIntegrity},
		// A CHECK constraint would refuse a valid intent with a raw constraint
		// error on a later write; the stored row still satisfies it.
		{"column-check-constraint", rebuildIntents(strings.Replace(sqliteSchema,
			"input_binding_subject_identity TEXT NOT NULL", "input_binding_subject_identity TEXT NOT NULL CHECK (input_binding_subject_identity <> 'subject-2')", 1)), ps.CodeIntegrity},
		{"table-check-constraint", rebuildIntents(strings.Replace(sqliteSchema,
			"blocker_materialization_prereq TEXT NOT NULL\n)",
			"blocker_materialization_prereq TEXT NOT NULL,\n\tCONSTRAINT narrow Check(length(operation_id) < 4)\n)", 1)), ps.CodeIntegrity},
		// A trigger would refuse a valid intent with a raw error on a later write,
		// or rewrite what is stored; the stored row passes through it untouched.
		{"narrowing-trigger", []string{
			"CREATE TRIGGER narrowed BEFORE INSERT ON intents WHEN NEW.input_binding_subject_identity = 'subject-2' BEGIN SELECT RAISE(ABORT, 'narrowed'); END",
		}, ps.CodeIntegrity},
		{"rewriting-trigger", []string{
			"CREATE TRIGGER rewritten AFTER INSERT ON intents BEGIN UPDATE intents SET auto_run_policy_id = 'other' WHERE intent_id = NEW.intent_id; END",
		}, ps.CodeIntegrity},
		// Table names are case-insensitive: a trigger declared on "INTENTS" is on intents.
		{"trigger-on-uppercase-table-name", []string{
			`CREATE TRIGGER shouted BEFORE UPDATE ON "INTENTS" BEGIN SELECT RAISE(ABORT, 'narrowed'); END`,
		}, ps.CodeIntegrity},
		{"index-wrong-predicate", []string{
			"DROP INDEX intents_run_id",
			"CREATE UNIQUE INDEX intents_run_id ON intents (run_id) WHERE run_id IS NOT NULL",
		}, ps.CodeIntegrity},
		{"index-predicate-wrong-literal", []string{
			"DROP INDEX intents_operation_id",
			"CREATE UNIQUE INDEX intents_operation_id ON intents (operation_id) WHERE origin = 'explicit'",
		}, ps.CodeIntegrity},
		// A quoted identifier named '' is not the empty string literal.
		{"index-predicate-quoted-identifier-lookalike", []string{
			"DROP INDEX intents_run_id",
			`CREATE UNIQUE INDEX intents_run_id ON intents (run_id) WHERE run_id <> "''"`,
		}, ps.CodeIntegrity},
		// A comment only separates tokens: the clause after it still counts.
		{"index-predicate-clause-after-comment", []string{
			"DROP INDEX intents_operation_id",
			"CREATE UNIQUE INDEX intents_operation_id ON intents (operation_id) WHERE origin = 'EXPLICIT' /* explicit */ AND run_id <> ''",
		}, ps.CodeIntegrity},
		{"column-wrong-type", rebuildIntents(strings.Replace(sqliteSchema,
			"operation_id                   TEXT NOT NULL", "operation_id                   INTEGER NOT NULL", 1)), ps.CodeIntegrity},
		// A non-BINARY key collation lets distinct opaque IDs collide.
		{"index-nocase-collation", []string{
			"DROP INDEX intents_operation_id",
			"CREATE UNIQUE INDEX intents_operation_id ON intents (operation_id COLLATE NOCASE) WHERE origin = 'EXPLICIT'",
		}, ps.CodeIntegrity},
		{"index-rtrim-collation", []string{
			"DROP INDEX intents_run_id",
			"CREATE UNIQUE INDEX intents_run_id ON intents (run_id COLLATE RTRIM) WHERE run_id <> ''",
		}, ps.CodeIntegrity},
		{"index-second-key-nocase-collation", []string{
			"DROP INDEX intents_auto_key",
			"CREATE UNIQUE INDEX intents_auto_key ON intents (auto_run_policy_id, input_binding_subject_identity COLLATE NOCASE) WHERE origin = 'AUTOMATIC'",
		}, ps.CodeIntegrity},
		// The index inherits a collation declared on the column.
		{"column-nocase-collation", rebuildIntents(strings.Replace(sqliteSchema,
			"operation_id                   TEXT NOT NULL", "operation_id                   TEXT COLLATE NOCASE NOT NULL", 1)), ps.CodeIntegrity},
		// The primary key index inherits intent_id's collation.
		{"intent-id-nocase-collation", rebuildIntents(strings.Replace(sqliteSchema,
			"intent_id                      TEXT NOT NULL PRIMARY KEY", "intent_id                      TEXT COLLATE NOCASE NOT NULL PRIMARY KEY", 1)), ps.CodeIntegrity},
		{"index-expression-key", []string{
			"DROP INDEX intents_operation_id",
			"CREATE UNIQUE INDEX intents_operation_id ON intents (lower(operation_id)) WHERE origin = 'EXPLICIT'",
		}, ps.CodeIntegrity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := sqlitePath(t)
			db := openSQLite(t, path)
			mustCreateAutomatic(t, newSQLiteStore(t, db), "subject-1")
			for _, stmt := range tc.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			before := dbSnapshot(t, db)
			s, err := NewSQLiteStore(ctx, db)
			if ps.CodeOf(err) != tc.code || s != nil {
				t.Fatalf("NewSQLiteStore: store=%v err=%v, want %s", s, err, tc.code)
			}
			if after := dbSnapshot(t, db); !reflect.DeepEqual(after, before) {
				t.Fatalf("refused open changed the database\nbefore=%q\nafter=%q", before, after)
			}
		})
	}
}

// A failure at any step of schema initialization fails the open and leaves no
// intent object behind; a later open then initializes cleanly.
func TestSQLiteStore_InitFailureLeavesNoSchema(t *testing.T) {
	for _, step := range []string{"schema-version-table", "schema-tables", "schema-version-record"} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			db := openSQLite(t, sqlitePath(t))
			boom := errors.New("injected init fault")
			s, err := newSQLiteStoreWithFault(ctx, db, func(got string) error {
				if got == step {
					return boom
				}
				return nil
			})
			if !errors.Is(err, boom) || s != nil {
				t.Fatalf("open with fault at %s: store=%v err=%v, want %v", step, s, err, boom)
			}
			if snap := dbSnapshot(t, db); len(snap) != 0 {
				t.Fatalf("failed init left schema behind: %q", snap)
			}
			mustCreateAutomatic(t, newSQLiteStore(t, db), "subject-1")
		})
	}
}

// Opening the current version again, on the same or a new handle, keeps the
// stored intents and the single version record.
func TestSQLiteStore_ReopenCurrentVersionKeepsData(t *testing.T) {
	ctx := context.Background()
	path := sqlitePath(t)
	db := openSQLite(t, path)
	in := mustCreateAutomatic(t, newSQLiteStore(t, db), "subject-1")
	before := dbSnapshot(t, db)
	newSQLiteStore(t, db)
	r := reopenSQLite(t, db, path)
	got, found, err := r.LookupAutomatic(ctx, in.AutoRunPolicyID, "subject-1")
	if err != nil || !found || got != in {
		t.Fatalf("intent after reopen: %+v found=%v err=%v, want %+v", got, found, err, in)
	}
	if after := dbSnapshot(t, r.db); !reflect.DeepEqual(after, before) {
		t.Fatalf("reopen changed the database\nbefore=%q\nafter=%q", before, after)
	}
}

// Schema verification reads SQLite metadata, and compares only index
// predicates token by token, never the whole stored SQL text: a version-1
// database whose objects are equivalent but written differently (type case,
// identifier quoting and case, operator spelling, parentheses, whitespace and
// comments) still opens, unchanged.
func TestSQLiteStore_OpenAcceptsEquivalentSchemaText(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, sqlitePath(t))
	in := mustCreateAutomatic(t, newSQLiteStore(t, db), "subject-1")
	stmts := rebuildIntents(strings.NewReplacer("TEXT NOT NULL", "text not null", "INTEGER NOT NULL", "Integer NOT NULL").Replace(sqliteSchema))
	stmts = append(stmts,
		"DROP INDEX intents_run_id",
		"create unique index intents_run_id on intents(run_id) where run_id/* a */!=-- b\n''",
		"DROP INDEX intents_operation_id",
		"CREATE UNIQUE INDEX \"intents_operation_id\" ON intents ([operation_id]) WHERE ( (\"ORIGIN\"=='EXPLICIT') ) -- explicit ledger",
		"DROP INDEX intents_auto_key",
		"CREATE UNIQUE INDEX intents_auto_key ON intents (auto_run_policy_id, input_binding_subject_identity) /* automatic */ WHERE `origin` = 'AUTOMATIC'",
	)
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	before := dbSnapshot(t, db)
	s, err := NewSQLiteStore(ctx, db)
	if err != nil {
		t.Fatalf("NewSQLiteStore on an equivalent schema: %v", err)
	}
	got, found, err := s.LookupAutomatic(ctx, in.AutoRunPolicyID, "subject-1")
	if err != nil || !found || got != in {
		t.Fatalf("intent after open: %+v found=%v err=%v, want %+v", got, found, err, in)
	}
	if after := dbSnapshot(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("open changed the database\nbefore=%q\nafter=%q", before, after)
	}
}

// Only a CHECK keyword is a CHECK constraint: a column default whose string
// literal spells "check" narrows nothing, so the open accepts it unchanged.
func TestSQLiteStore_OpenAcceptsCheckLiteral(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, sqlitePath(t))
	in := mustCreateAutomatic(t, newSQLiteStore(t, db), "subject-1")
	for _, stmt := range rebuildIntents(strings.Replace(sqliteSchema,
		"blocker_authorization          TEXT NOT NULL", "blocker_authorization          TEXT NOT NULL DEFAULT 'check'", 1)) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	before := dbSnapshot(t, db)
	s, err := NewSQLiteStore(ctx, db)
	if err != nil {
		t.Fatalf("NewSQLiteStore with a 'check' literal: %v", err)
	}
	if got := mustGetIntent(t, s, in.ID); got != in {
		t.Fatalf("intent after open: %+v, want %+v", got, in)
	}
	if after := dbSnapshot(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("open changed the database\nbefore=%q\nafter=%q", before, after)
	}
}

// failingIndexRows yields one well-formed index row and then stops with an
// iteration error, as *sql.Rows does when reading a later row fails.
type failingIndexRows struct {
	served bool
	err    error
}

func (r *failingIndexRows) Next() bool {
	if r.served {
		return false
	}
	r.served = true
	return true
}

func (r *failingIndexRows) Scan(dest ...any) error {
	*dest[0].(*string) = "intents_auto_key"
	*dest[1].(*int) = 1
	*dest[2].(*int) = 1
	*dest[3].(*string) = "c"
	return nil
}

func (r *failingIndexRows) Err() error   { return r.err }
func (r *failingIndexRows) Close() error { return nil }

// An iteration error after some rows fails the index list read instead of
// returning the rows read so far as the whole list.
func TestReadIndexList_IterationErrorFailsClosed(t *testing.T) {
	boom := errors.New("injected iteration error")
	indexes, err := readIndexList(&failingIndexRows{err: boom})
	if !errors.Is(err, boom) || indexes != nil {
		t.Fatalf("readIndexList = %v, %v; want nil, %v", indexes, err, boom)
	}
	indexes, err = readIndexList(&failingIndexRows{})
	if err != nil || len(indexes) != 1 || indexes["intents_auto_key"] != (indexMeta{unique: true, partial: true, origin: "c"}) {
		t.Fatalf("readIndexList without error = %v, %v; want the one row", indexes, err)
	}
}

// An operation_id index that compares case-insensitively would collapse the
// distinct explicit IDs "op" and "OP", so the open refuses it and changes
// nothing. The same index spelled with an explicit COLLATE BINARY opens and
// keeps both IDs.
func TestSQLiteStore_OperationIDIndexMustBeBinary(t *testing.T) {
	cases := []struct {
		name  string
		index string
		opens bool
	}{
		{"nocase", "CREATE UNIQUE INDEX intents_operation_id ON intents (operation_id COLLATE NOCASE) WHERE origin = 'EXPLICIT'", false},
		{"explicit-binary", "CREATE UNIQUE INDEX intents_operation_id ON intents (operation_id COLLATE binary) WHERE origin = 'EXPLICIT'", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := openSQLite(t, sqlitePath(t))
			if _, created, err := newSQLiteStore(t, db).createExplicit(ctx, explicitDraft("op")); err != nil || !created {
				t.Fatalf("create op: created=%v err=%v", created, err)
			}
			for _, stmt := range []string{"DROP INDEX intents_operation_id", tc.index} {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			before := dbSnapshot(t, db)
			s, err := NewSQLiteStore(ctx, db)
			if !tc.opens {
				if ps.CodeOf(err) != ps.CodeIntegrity || s != nil {
					detail := ""
					if s != nil {
						_, _, cerr := s.createExplicit(ctx, explicitDraft("OP"))
						detail = fmt.Sprintf("; creating OP then gives %v", cerr)
					}
					t.Fatalf("NewSQLiteStore: store=%v err=%v, want %s%s", s, err, ps.CodeIntegrity, detail)
				}
				if after := dbSnapshot(t, db); !reflect.DeepEqual(after, before) {
					t.Fatalf("refused open changed the database\nbefore=%q\nafter=%q", before, after)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewSQLiteStore with a BINARY index: %v", err)
			}
			if after := dbSnapshot(t, db); !reflect.DeepEqual(after, before) {
				t.Fatalf("open changed the database\nbefore=%q\nafter=%q", before, after)
			}
			if _, created, err := s.createExplicit(ctx, explicitDraft("OP")); err != nil || !created {
				t.Fatalf("create OP next to op: created=%v err=%v", created, err)
			}
			for _, op := range []string{"op", "OP"} {
				got, found, err := s.LookupExplicit(ctx, op)
				if err != nil || !found || got.OperationID != op {
					t.Fatalf("LookupExplicit(%q) = %+v found=%v err=%v", op, got, found, err)
				}
			}
		})
	}
}

// Identity columns declared NOCASE, with every required index explicitly
// BINARY, pass schema verification. The identity predicates pin BINARY, so
// "op"/"OP", "subject-1"/"SUBJECT-1" and "run"/"RUN" stay distinct identities
// instead of replaying or conflicting with the stored one.
func TestSQLiteStore_NocaseIdentityColumnsDoNotCollapse(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, sqlitePath(t))
	s := newSQLiteStore(t, db)
	explicit, created, err := s.createExplicit(ctx, explicitDraft("op"))
	if err != nil || !created {
		t.Fatalf("create op: created=%v err=%v", created, err)
	}
	auto := mustCreateAutomatic(t, s, "subject-1")
	if _, err := s.AttachRunID(ctx, auto.ID, "run"); err != nil {
		t.Fatalf("attach run: %v", err)
	}
	schema := strings.NewReplacer(
		"operation_id                   TEXT NOT NULL", "operation_id                   TEXT COLLATE NOCASE NOT NULL",
		"auto_run_policy_id             TEXT NOT NULL", "auto_run_policy_id             TEXT COLLATE NOCASE NOT NULL",
		"input_binding_subject_identity TEXT NOT NULL", "input_binding_subject_identity TEXT COLLATE NOCASE NOT NULL",
		"run_id                         TEXT NOT NULL", "run_id                         TEXT COLLATE NOCASE NOT NULL",
		"(auto_run_policy_id, input_binding_subject_identity)", "(auto_run_policy_id COLLATE BINARY, input_binding_subject_identity COLLATE BINARY)",
		"(operation_id)", "(operation_id COLLATE BINARY)",
		"(run_id)", "(run_id COLLATE BINARY)",
	).Replace(sqliteSchema)
	for _, stmt := range rebuildIntents(schema) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	before := dbSnapshot(t, db)
	s, err = NewSQLiteStore(ctx, db)
	if err != nil {
		t.Fatalf("NewSQLiteStore with NOCASE columns and BINARY indexes: %v", err)
	}
	if after := dbSnapshot(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("open changed the database\nbefore=%q\nafter=%q", before, after)
	}

	if got, found, err := s.LookupExplicit(ctx, "OP"); err != nil || found {
		t.Fatalf("LookupExplicit(OP) before create = %+v found=%v err=%v, want not found", got, found, err)
	}
	upper, created, err := s.createExplicit(ctx, explicitDraft("OP"))
	if err != nil || !created || upper.ID == explicit.ID || upper.OperationID != "OP" {
		t.Fatalf("create OP next to op: %+v created=%v err=%v", upper, created, err)
	}
	for _, want := range []Intent{explicit, upper} {
		if got, found, err := s.LookupExplicit(ctx, want.OperationID); err != nil || !found || got.ID != want.ID {
			t.Fatalf("LookupExplicit(%q) = %+v found=%v err=%v, want %s", want.OperationID, got, found, err, want.ID)
		}
	}

	if got, found, err := s.LookupAutomatic(ctx, auto.AutoRunPolicyID, "SUBJECT-1"); err != nil || found {
		t.Fatalf("LookupAutomatic(SUBJECT-1) before create = %+v found=%v err=%v, want not found", got, found, err)
	}
	autoUpper, created, err := s.createAutomatic(ctx, automaticDraft("SUBJECT-1"))
	if err != nil || !created || autoUpper.ID == auto.ID {
		t.Fatalf("create SUBJECT-1 next to subject-1: %+v created=%v err=%v", autoUpper, created, err)
	}

	// Same subject, policy ID differing only in case: only the policy predicate
	// can tell the two keys apart.
	if got, found, err := s.LookupAutomatic(ctx, "POLICY-1", "subject-1"); err != nil || found {
		t.Fatalf("LookupAutomatic(POLICY-1) before create = %+v found=%v err=%v, want not found", got, found, err)
	}
	policyDraft := automaticDraft("subject-1")
	policyDraft.AutoRunPolicyID = "POLICY-1"
	policyUpper, created, err := s.createAutomatic(ctx, policyDraft)
	if err != nil || !created || policyUpper.ID == auto.ID || policyUpper.AutoRunPolicyID != "POLICY-1" {
		t.Fatalf("create POLICY-1 next to policy-1: %+v created=%v err=%v", policyUpper, created, err)
	}
	for _, want := range []Intent{auto, policyUpper} {
		if got, found, err := s.LookupAutomatic(ctx, want.AutoRunPolicyID, "subject-1"); err != nil || !found || got.ID != want.ID {
			t.Fatalf("LookupAutomatic(%q) = %+v found=%v err=%v, want %s", want.AutoRunPolicyID, got, found, err, want.ID)
		}
	}

	if _, err := s.AttachRunID(ctx, autoUpper.ID, "RUN"); err != nil {
		t.Fatalf("attach RUN next to run: %v", err)
	}
	if got := mustGetIntent(t, s, auto.ID); got.RunID != "run" {
		t.Fatalf("intent %s RunID = %q, want run", auto.ID, got.RunID)
	}
	if got := mustGetIntent(t, s, autoUpper.ID); got.RunID != "RUN" {
		t.Fatalf("intent %s RunID = %q, want RUN", autoUpper.ID, got.RunID)
	}
}

// An intent_id column declared NOCASE whose primary key is explicitly BINARY
// passes schema verification. Get and AttachRunID pin BINARY on intent_id, so a
// case variant of a stored ID is not found and the stored intent is unchanged.
func TestSQLiteStore_NocaseIntentIDColumnWithBinaryPrimaryKey(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, sqlitePath(t))
	in := mustCreateAutomatic(t, newSQLiteStore(t, db), "subject-1")
	const id ID = "intent-lower"
	if _, err := db.Exec("UPDATE intents SET intent_id = ? WHERE intent_id = ?", string(id), string(in.ID)); err != nil {
		t.Fatalf("set intent_id: %v", err)
	}
	schema := strings.NewReplacer(
		"intent_id                      TEXT NOT NULL PRIMARY KEY,", "intent_id                      TEXT COLLATE NOCASE NOT NULL,",
		"blocker_materialization_prereq TEXT NOT NULL\n", "blocker_materialization_prereq TEXT NOT NULL,\n\tPRIMARY KEY (intent_id COLLATE BINARY)\n",
	).Replace(sqliteSchema)
	for _, stmt := range rebuildIntents(schema) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	before := dbSnapshot(t, db)
	s, err := NewSQLiteStore(ctx, db)
	if err != nil {
		t.Fatalf("NewSQLiteStore with a NOCASE intent_id and a BINARY primary key: %v", err)
	}
	if after := dbSnapshot(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("open changed the database\nbefore=%q\nafter=%q", before, after)
	}

	const upper ID = "INTENT-LOWER"
	if got, err := s.Get(ctx, upper); ps.CodeOf(err) != ps.CodeNotFound {
		t.Fatalf("Get(%s) = %+v err=%v, want %s", upper, got, err, ps.CodeNotFound)
	}
	if got, err := s.AttachRunID(ctx, upper, "run"); ps.CodeOf(err) != ps.CodeNotFound {
		t.Fatalf("AttachRunID(%s) = %+v err=%v, want %s", upper, got, err, ps.CodeNotFound)
	}
	if got := mustGetIntent(t, s, id); got.ID != id || got.RunID != "" {
		t.Fatalf("stored intent after case-variant calls = %+v, want ID %s without RunID", got, id)
	}
	if after := dbSnapshot(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("case-variant calls changed the database\nbefore=%q\nafter=%q", before, after)
	}
}

func TestNewSQLiteStore_RequiresDatabase(t *testing.T) {
	if _, err := NewSQLiteStore(context.Background(), nil); err == nil {
		t.Fatal("NewSQLiteStore(nil) succeeded")
	}
}
