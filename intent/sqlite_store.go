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

// intentSchemaVersion is the intent table layout this build reads and writes.
// It is recorded in its own intent_schema_version table rather than PRAGMA
// user_version, because the database file is shared with the revision store.
const intentSchemaVersion = 1

const sqliteVersionTable = `
CREATE TABLE intent_schema_version (
	singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
	version   INTEGER NOT NULL
)`

const sqliteSchema = `
CREATE TABLE intents (
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
CREATE UNIQUE INDEX intents_auto_key ON intents (auto_run_policy_id, input_binding_subject_identity)
	WHERE origin = 'AUTOMATIC';
CREATE UNIQUE INDEX intents_operation_id ON intents (operation_id) WHERE origin = 'EXPLICIT';
CREATE UNIQUE INDEX intents_run_id ON intents (run_id) WHERE run_id <> '';
`

// intentObjects are the schema objects sqliteSchema creates.
const intentObjects = `'intents', 'intents_auto_key', 'intents_operation_id', 'intents_run_id'`

const intentColumns = `intent_id, origin, auto_run_policy_id, auto_run_policy_revision, operation_id,
	input_binding_subject_identity, pipeline_id, pipeline_revision_id, run_id, admission_epoch,
	blocker_policy_disable, blocker_campaign_pause, blocker_authorization, blocker_materialization_prereq`

// NewSQLiteStore opens the intent store on db and returns it. db must be an
// SQLite database; the caller owns and closes it.
//
// Open safety: a database with no intent objects is initialized at
// intentSchemaVersion. A database recorded at intentSchemaVersion is used as
// is. Anything else fails with ps.CodeUnsupportedVersion or ps.CodeIntegrity:
// a newer or unknown version, intent tables without a version record, or a
// version record without its tables. Existing intent tables are never dropped,
// recreated or migrated. Initialization runs in one transaction, so a failed
// open leaves the database unchanged.
func NewSQLiteStore(ctx context.Context, db *sql.DB) (*SQLiteStore, error) {
	return newSQLiteStoreWithFault(ctx, db, nil)
}

// newSQLiteStoreWithFault is NewSQLiteStore with fault active during schema
// initialization (tests).
func newSQLiteStoreWithFault(ctx context.Context, db *sql.DB, fault func(step string) error) (*SQLiteStore, error) {
	if db == nil {
		return nil, errors.New("intent: NewSQLiteStore requires a database")
	}
	s := &SQLiteStore{db: db, fault: fault}
	if err := s.write(ctx, func(conn *sql.Conn) error { return s.initSchema(ctx, conn) }); err != nil {
		return nil, fmt.Errorf("intent: init sqlite schema: %w", err)
	}
	s.fault = nil
	return s, nil
}

// initSchema verifies the recorded intent schema version, or creates the
// schema on a database that has no intent objects yet.
func (s *SQLiteStore) initSchema(ctx context.Context, conn *sql.Conn) error {
	var versioned, objects int
	if err := conn.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'intent_schema_version'`).Scan(&versioned); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE name IN (`+intentObjects+`)`).Scan(&objects); err != nil {
		return err
	}
	if versioned == 0 {
		if objects != 0 {
			return newError(ps.CodeUnsupportedVersion, "intent tables exist without a schema version; refusing to adopt them")
		}
		return s.createSchema(ctx, conn)
	}
	var version int64
	err := conn.QueryRowContext(ctx, `SELECT version FROM intent_schema_version WHERE singleton = 1`).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return newError(ps.CodeIntegrity, "intent schema version table has no version record")
	}
	if err != nil {
		return err
	}
	if version != intentSchemaVersion {
		return newError(ps.CodeUnsupportedVersion, "intent schema version %d is not supported (this build supports %d)", version, intentSchemaVersion)
	}
	if objects != 4 {
		return newError(ps.CodeIntegrity, "intent schema version %d is recorded but %d of 4 intent objects exist", version, objects)
	}
	return verifySchemaObjects(ctx, conn)
}

// intentIndexes are the unique partial indexes sqliteSchema creates on
// intents, with their key columns in order and their WHERE predicate.
var intentIndexes = []struct {
	name    string
	columns []string
	where   string
}{
	{"intents_auto_key", []string{"auto_run_policy_id", "input_binding_subject_identity"}, "origin = 'AUTOMATIC'"},
	{"intents_operation_id", []string{"operation_id"}, "origin = 'EXPLICIT'"},
	{"intents_run_id", []string{"run_id"}, "run_id <> ''"},
}

// verifySchemaObjects checks that the intent objects of a version-1 database
// are what sqliteSchema creates, using SQLite's schema metadata rather than
// the stored SQL text: intents is a table with exactly the intent columns
// (declared types as in sqliteSchema, all NOT NULL, intent_id the primary
// key, its index keyed with BINARY collation), and each required index is a
// unique partial index on intents over its key columns, each compared with
// BINARY collation (see verifyBinaryKeys), whose WHERE predicate is the
// expected one. Apart from the primary key and those indexes, intents has no
// unique index or UNIQUE constraint, and it has no CHECK constraint (see
// verifyNoCheckConstraints). Other column collations are not read; the identity predicates
// pin BINARY instead (see whereID). SQLite exposes the
// predicate only inside the stored CREATE INDEX text, so the predicate alone is
// compared token by token (see indexPredicate); the rest of the text is not. A
// mismatch fails with ps.CodeIntegrity; nothing is repaired.
func verifySchemaObjects(ctx context.Context, conn *sql.Conn) error {
	objectOn := func(name, wantType string) error {
		var typ, tbl string
		if err := conn.QueryRowContext(ctx,
			`SELECT type, tbl_name FROM sqlite_master WHERE name = ?`, name).Scan(&typ, &tbl); err != nil {
			return err
		}
		if typ != wantType || tbl != "intents" {
			return newError(ps.CodeIntegrity, "intent schema object %q is a %s on %q, want a %s on \"intents\"", name, typ, tbl, wantType)
		}
		return nil
	}
	if err := objectOn("intents", "table"); err != nil {
		return err
	}
	if err := verifyIntentColumns(ctx, conn); err != nil {
		return err
	}
	if err := verifyNoCheckConstraints(ctx, conn); err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, `SELECT name, "unique", partial, origin FROM pragma_index_list('intents')`)
	if err != nil {
		return err
	}
	indexes, err := readIndexList(rows)
	if err != nil {
		return err
	}
	// Any other uniqueness on intents (a UNIQUE column or table constraint, or
	// an extra unique index) would refuse writes the store contract accepts.
	allowedUnique := map[string]bool{}
	for _, want := range intentIndexes {
		allowedUnique[want.name] = true
	}
	for name, meta := range indexes {
		if meta.unique && meta.origin != "pk" && !allowedUnique[name] {
			return newError(ps.CodeIntegrity, "intents has unexpected unique index %q (origin %q)", name, meta.origin)
		}
	}
	for _, want := range intentIndexes {
		if err := objectOn(want.name, "index"); err != nil {
			return err
		}
		meta := indexes[want.name]
		if !meta.unique || !meta.partial {
			return newError(ps.CodeIntegrity, "intent index %q is unique=%v partial=%v, want a unique partial index", want.name, meta.unique, meta.partial)
		}
		// An expression key has no column name; it reads as "" and never matches.
		cols, err := queryStrings(ctx, conn, `SELECT coalesce(name, '') FROM pragma_index_info(?) ORDER BY seqno`, want.name)
		if err != nil {
			return err
		}
		if !equalStrings(cols, want.columns) {
			return newError(ps.CodeIntegrity, "intent index %q covers %v, want %v", want.name, cols, want.columns)
		}
		if err := verifyBinaryKeys(ctx, conn, want.name); err != nil {
			return err
		}
		var ddl string
		if err := conn.QueryRowContext(ctx,
			`SELECT coalesce(sql, '') FROM sqlite_master WHERE name = ?`, want.name).Scan(&ddl); err != nil {
			return err
		}
		got, err := indexPredicate(ddl)
		if err != nil {
			return newError(ps.CodeIntegrity, "intent index %q: %v", want.name, err)
		}
		expected, err := sqlTokens(want.where)
		if err != nil {
			return err
		}
		if !equalTokens(got, expected) {
			return newError(ps.CodeIntegrity, "intent index %q has predicate %v, want %q", want.name, tokenTexts(got), want.where)
		}
	}
	// The primary key's index inherits intent_id's declared collation.
	pks, err := queryStrings(ctx, conn, `SELECT name FROM pragma_index_list('intents') WHERE origin = 'pk'`)
	if err != nil {
		return err
	}
	if len(pks) != 1 {
		return newError(ps.CodeIntegrity, "intents has %d primary key indexes, want 1", len(pks))
	}
	return verifyBinaryKeys(ctx, conn, pks[0])
}

// indexMeta is one row of pragma_index_list('intents').
type indexMeta struct {
	unique, partial bool
	origin          string
}

// indexRows is the part of *sql.Rows that readIndexList uses.
type indexRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// readIndexList reads (name, unique, partial, origin) rows into a map keyed by
// index name and closes rows. An error that ends the iteration early fails the
// read, so a partial list is never taken for the whole one.
func readIndexList(rows indexRows) (map[string]indexMeta, error) {
	defer func() { _ = rows.Close() }()
	indexes := map[string]indexMeta{}
	for rows.Next() {
		var (
			name, origin    string
			unique, partial int
		)
		if err := rows.Scan(&name, &unique, &partial, &origin); err != nil {
			return nil, err
		}
		indexes[name] = indexMeta{unique: unique == 1, partial: partial == 1, origin: origin}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return indexes, nil
}

// verifyNoCheckConstraints checks that the stored CREATE TABLE text of intents
// has no CHECK constraint, on a column or on the table. sqliteSchema declares
// none, and SQLite exposes CHECK constraints only inside that text, so any
// CHECK keyword token there (a quoted identifier or a string literal spelling
// "check" is not one) would narrow the rows the store contract accepts and
// fail a later write with a raw constraint error.
func verifyNoCheckConstraints(ctx context.Context, conn *sql.Conn) error {
	var ddl string
	if err := conn.QueryRowContext(ctx,
		`SELECT coalesce(sql, '') FROM sqlite_master WHERE type = 'table' AND name = 'intents'`).Scan(&ddl); err != nil {
		return err
	}
	toks, err := sqlTokens(ddl)
	if err != nil {
		return newError(ps.CodeIntegrity, "intents table: %v", err)
	}
	for _, t := range toks {
		if t.word && t.text == "check" {
			return newError(ps.CodeIntegrity, "intents has an unexpected CHECK constraint")
		}
	}
	return nil
}

// sqlToken is one token of SQL text. Keywords and identifiers (quoted or not)
// are case-folded and unquoted, string literals keep their exact quoted text,
// and the operator spellings "!=" and "==" become "<>" and "=". word marks an
// unquoted keyword or identifier, literal a string literal and punct an
// operator or punctuation. Token class is part of identity: a quoted
// identifier whose name spells a string literal never equals that literal.
type sqlToken struct {
	text    string
	word    bool
	literal bool
	punct   bool
}

// sqlTokens splits SQL text into tokens, dropping whitespace and comments, so
// that two spellings of the same expression compare equal. Case folding is
// ASCII-only, as for SQLite identifiers.
func sqlTokens(text string) ([]sqlToken, error) {
	var out []sqlToken
	for i := 0; i < len(text); {
		c := text[i]
		next := byte(0)
		if i+1 < len(text) {
			next = text[i+1]
		}
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && next == '-':
			for i < len(text) && text[i] != '\n' {
				i++
			}
		case c == '/' && next == '*':
			end := i + 2
			for end+1 < len(text) && (text[end] != '*' || text[end+1] != '/') {
				end++
			}
			if end+1 >= len(text) {
				return nil, errors.New("unterminated comment in schema SQL")
			}
			i = end + 2
		case c == '\'':
			end, err := quotedEnd(text, i, '\'')
			if err != nil {
				return nil, err
			}
			out = append(out, sqlToken{text: text[i:end], literal: true})
			i = end
		case c == '"' || c == '`' || c == '[':
			closer := c
			if c == '[' {
				closer = ']'
			}
			end, err := quotedEnd(text, i, closer)
			if err != nil {
				return nil, err
			}
			var name []byte
			for j := i + 1; j < end-1; j++ {
				name = append(name, text[j])
				if closer != ']' && text[j] == closer {
					j++ // doubled closer is one escaped closer
				}
			}
			out = append(out, sqlToken{text: lowerASCII(string(name))})
			i = end
		case isWordByte(c):
			j := i
			for j < len(text) && isWordByte(text[j]) {
				j++
			}
			out = append(out, sqlToken{text: lowerASCII(text[i:j]), word: true})
			i = j
		default:
			op, width := text[i:i+1], 1
			switch two := string([]byte{c, next}); two {
			case "<>", "<=", ">=", "||", "<<", ">>":
				op, width = two, 2
			case "!=":
				op, width = "<>", 2
			case "==":
				op, width = "=", 2
			}
			out = append(out, sqlToken{text: op, punct: true})
			i += width
		}
	}
	return out, nil
}

// quotedEnd returns the index just past the quoted run that starts at
// text[start], where a doubled closer inside the run is an escaped closer
// (except for [...], which has no escape).
func quotedEnd(text string, start int, closer byte) (int, error) {
	for i := start + 1; i < len(text); i++ {
		if text[i] != closer {
			continue
		}
		if closer != ']' && i+1 < len(text) && text[i+1] == closer {
			i++
			continue
		}
		return i + 1, nil
	}
	return 0, errors.New("unterminated quoted text in schema SQL")
}

// lowerASCII folds ASCII upper-case letters to lower case.
func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

func isWordByte(c byte) bool {
	return c == '_' || c == '$' || c >= 0x80 ||
		('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// indexPredicate returns the tokens of the WHERE predicate of a CREATE INDEX
// statement, without parentheses that enclose the whole predicate.
func indexPredicate(ddl string) ([]sqlToken, error) {
	toks, err := sqlTokens(ddl)
	if err != nil {
		return nil, err
	}
	depth := 0
	for i, t := range toks {
		switch {
		case t.punct && t.text == "(":
			depth++
		case t.punct && t.text == ")":
			depth--
		case depth == 0 && t.word && t.text == "where":
			return stripOuterParens(toks[i+1:]), nil
		}
	}
	return nil, errors.New("index has no WHERE predicate")
}

// stripOuterParens removes parentheses that enclose all of toks.
func stripOuterParens(toks []sqlToken) []sqlToken {
	for len(toks) >= 2 && toks[0] == (sqlToken{text: "(", punct: true}) && toks[len(toks)-1] == (sqlToken{text: ")", punct: true}) {
		depth := 0
		enclosing := true
		for i, t := range toks {
			switch {
			case t.punct && t.text == "(":
				depth++
			case t.punct && t.text == ")":
				depth--
			}
			if depth == 0 && i < len(toks)-1 {
				enclosing = false
				break
			}
		}
		if !enclosing {
			return toks
		}
		toks = toks[1 : len(toks)-1]
	}
	return toks
}

func equalTokens(a, b []sqlToken) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].text != b[i].text || a[i].literal != b[i].literal || a[i].punct != b[i].punct {
			return false
		}
	}
	return true
}

func tokenTexts(toks []sqlToken) []string {
	out := make([]string, len(toks))
	for i, t := range toks {
		out[i] = t.text
	}
	return out
}

// intentColumnNames are the intents columns in table order (intentColumns as a
// list).
var intentColumnNames = []string{
	"intent_id", "origin", "auto_run_policy_id", "auto_run_policy_revision", "operation_id",
	"input_binding_subject_identity", "pipeline_id", "pipeline_revision_id", "run_id", "admission_epoch",
	"blocker_policy_disable", "blocker_campaign_pause", "blocker_authorization", "blocker_materialization_prereq",
}

// intentColumnType is the type sqliteSchema declares for an intents column.
// The declared type sets the column's affinity, so a different one (say
// INTEGER for operation_id) would change how stored and indexed values compare.
func intentColumnType(name string) string {
	if name == "admission_epoch" {
		return "integer"
	}
	return "text"
}

// verifyIntentColumns checks the intents table has exactly the intent columns
// in order, each with its declared type (ASCII case-insensitive) and NOT NULL,
// with intent_id as the only primary key column.
func verifyIntentColumns(ctx context.Context, conn *sql.Conn) error {
	want := intentColumnNames
	rows, err := conn.QueryContext(ctx, `SELECT name, type, "notnull", pk FROM pragma_table_info('intents') ORDER BY cid`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var (
			name, typ   string
			notNull, pk int
		)
		if err := rows.Scan(&name, &typ, &notNull, &pk); err != nil {
			return err
		}
		if notNull != 1 || (pk != 0) != (name == "intent_id") {
			return newError(ps.CodeIntegrity, "intents column %q has notnull=%d pk=%d", name, notNull, pk)
		}
		if lowerASCII(typ) != intentColumnType(name) {
			return newError(ps.CodeIntegrity, "intents column %q is declared %q, want %q", name, typ, intentColumnType(name))
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !equalStrings(got, want) {
		return newError(ps.CodeIntegrity, "intents columns are %v, want %v", got, want)
	}
	return nil
}

// verifyBinaryKeys checks that every key column of index compares with the
// BINARY collation. Intent identities are opaque byte strings: NOCASE, RTRIM or
// any other collation, declared on the index key or inherited from the column,
// would make the unique index treat distinct IDs such as "op" and "OP" as one.
func verifyBinaryKeys(ctx context.Context, conn *sql.Conn, index string) error {
	colls, err := queryStrings(ctx, conn,
		`SELECT coalesce(coll, '') FROM pragma_index_xinfo(?) WHERE key = 1 ORDER BY seqno`, index)
	if err != nil {
		return err
	}
	for _, coll := range colls {
		if lowerASCII(coll) != "binary" {
			return newError(ps.CodeIntegrity, "intent index %q has key collations %v, want BINARY", index, colls)
		}
	}
	return nil
}

func queryStrings(ctx context.Context, conn *sql.Conn, query string, args ...any) ([]string, error) {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *SQLiteStore) createSchema(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, sqliteVersionTable); err != nil {
		return err
	}
	if err := s.inject("schema-version-table"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, sqliteSchema); err != nil {
		return err
	}
	if err := s.inject("schema-tables"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO intent_schema_version (singleton, version) VALUES (1, ?)`, intentSchemaVersion); err != nil {
		return err
	}
	return s.inject("schema-version-record")
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

// Identity predicates pin BINARY: an equality uses the column's declared
// collation unless one operand names another, and verifySchemaObjects checks
// the index keys, not every column declaration. A column declared NOCASE must
// still never match "OP" to a stored "op".
const (
	whereAuto     = "origin = 'AUTOMATIC' AND auto_run_policy_id COLLATE BINARY = ? AND input_binding_subject_identity COLLATE BINARY = ?"
	whereExplicit = "origin = 'EXPLICIT' AND operation_id COLLATE BINARY = ?"
	whereID       = "intent_id COLLATE BINARY = ?"
	whereRunID    = "run_id COLLATE BINARY = ?"
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
		if _, err := conn.ExecContext(ctx, "UPDATE intents SET run_id = ? WHERE "+whereID, string(run), string(id)); err != nil {
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
		if _, err := conn.ExecContext(ctx, "UPDATE intents SET "+column+" = ? WHERE "+whereID, string(state), string(id)); err != nil {
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
