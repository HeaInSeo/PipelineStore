package intent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
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

func TestNewSQLiteStore_RequiresDatabase(t *testing.T) {
	if _, err := NewSQLiteStore(context.Background(), nil); err == nil {
		t.Fatal("NewSQLiteStore(nil) succeeded")
	}
}
