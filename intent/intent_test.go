package intent

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"maps"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
)

// ── fixtures ─────────────────────────────────────────────────────────────────

var (
	rev1 = PipelineRevisionRef{PipelineID: "pipe-a", RevisionID: "rev-1"}
	rev2 = PipelineRevisionRef{PipelineID: "pipe-a", RevisionID: "rev-2"}
)

// committed is an exact-read RevisionReader over a fixed set of committed
// revisions.
type committed struct {
	mu   sync.Mutex
	revs map[PipelineRevisionRef]bool
}

func newCommitted(refs ...PipelineRevisionRef) *committed {
	c := &committed{revs: map[PipelineRevisionRef]bool{}}
	for _, r := range refs {
		c.revs[r] = true
	}
	return c
}

func (c *committed) GetRevision(_ context.Context, pipelineID string, revisionID ps.PipelineRevisionID) (*ps.PipelineRevision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.revs[PipelineRevisionRef{PipelineID: pipelineID, RevisionID: revisionID}] {
		return nil, &ps.Error{Code: ps.CodeNotFound, Msg: "no such committed revision"}
	}
	return &ps.PipelineRevision{PipelineID: pipelineID, RevisionID: revisionID}, nil
}

func newTestService(t *testing.T) (*Service, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	svc, err := NewService(store, newCommitted(rev1, rev2))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, store
}

func autoReq() AutomaticRequest {
	return AutomaticRequest{
		AutoRunPolicyID:             "policy-1",
		AutoRunPolicyRevision:       "policy-rev-1",
		InputBindingSubjectIdentity: "subject-1",
		PipelineRevision:            rev1,
	}
}

func explicitReq() ExplicitRequest {
	return ExplicitRequest{
		OperationID:                 "op-1",
		InputBindingSubjectIdentity: "subject-1",
		PipelineRevision:            rev1,
	}
}

type storeState struct {
	intents map[ID]Intent
	auto    map[autoKey]ID
	ops     map[string]ID
}

func snapshot(m *MemoryStore) storeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return storeState{intents: maps.Clone(m.intents), auto: maps.Clone(m.auto), ops: maps.Clone(m.ops)}
}

func assertUnchanged(t *testing.T, m *MemoryStore, before storeState) {
	t.Helper()
	if after := snapshot(m); !reflect.DeepEqual(before, after) {
		t.Fatalf("store mutated:\nbefore %+v\nafter  %+v", before, after)
	}
}

func assertCode(t *testing.T, err error, want ps.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s error, got nil", want)
	}
	if got := ps.CodeOf(err); got != want {
		t.Fatalf("error code = %q, want %q (err: %v)", got, want, err)
	}
}

// ── 1. repeated automatic evaluation → exactly one intent ────────────────────

func TestAutomatic_RepeatedAndConcurrent_ExactlyOneIntent(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	first, err := svc.CreateAutomatic(ctx, autoReq())
	if err != nil {
		t.Fatalf("first CreateAutomatic: %v", err)
	}
	if !first.Created {
		t.Fatal("first evaluation must create the intent")
	}

	for i := range 1000 {
		res, err := svc.CreateAutomatic(ctx, autoReq())
		if err != nil {
			t.Fatalf("sequential replay %d: %v", i, err)
		}
		if res.Created || res.Intent.ID != first.Intent.ID || res.Divergence != nil {
			t.Fatalf("sequential replay %d = %+v, want existing %q without divergence", i, res, first.Intent.ID)
		}
	}

	// Concurrent evaluations of a fresh subject: exactly one creator wins.
	req := autoReq()
	req.InputBindingSubjectIdentity = "subject-concurrent"
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := map[ID]int{}
	created := 0
	var errs []error
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := svc.CreateAutomatic(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			ids[res.Intent.ID]++
			if res.Created {
				created++
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent evaluations failed: %v", errs[0])
	}
	if created != 1 || len(ids) != 1 {
		t.Fatalf("concurrent evaluations: created=%d distinct ids=%d, want 1 and 1", created, len(ids))
	}

	st := snapshot(store)
	if len(st.intents) != 2 || len(st.auto) != 2 || len(st.ops) != 0 {
		t.Fatalf("store holds intents=%d auto=%d ops=%d, want 2/2/0", len(st.intents), len(st.auto), len(st.ops))
	}
}

// ── 2./3. revision changes never create or rewrite ───────────────────────────

func TestAutomatic_PolicyRevisionChange_NoNewIntentNoRewrite(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	first, err := svc.CreateAutomatic(ctx, autoReq())
	if err != nil {
		t.Fatalf("CreateAutomatic: %v", err)
	}
	before := snapshot(store)

	later := autoReq()
	later.AutoRunPolicyRevision = "policy-rev-2"
	res, err := svc.CreateAutomatic(ctx, later)
	if err != nil {
		t.Fatalf("reevaluation: %v", err)
	}
	if res.Created || res.Intent != first.Intent {
		t.Fatalf("reevaluation returned %+v, want the unchanged frozen intent %+v", res.Intent, first.Intent)
	}
	if res.Divergence == nil || !res.Divergence.PolicyRevisionChanged || res.Divergence.PipelineRevisionChanged ||
		res.Divergence.ObservedPolicyRevision != "policy-rev-2" {
		t.Fatalf("divergence = %+v, want policy-revision-only divergence observing policy-rev-2", res.Divergence)
	}
	assertUnchanged(t, store, before)
}

func TestAutomatic_PipelineRevisionChange_NoNewIntentNoRewrite(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	first, err := svc.CreateAutomatic(ctx, autoReq())
	if err != nil {
		t.Fatalf("CreateAutomatic: %v", err)
	}
	before := snapshot(store)

	later := autoReq()
	later.PipelineRevision = rev2
	res, err := svc.CreateAutomatic(ctx, later)
	if err != nil {
		t.Fatalf("reevaluation: %v", err)
	}
	if res.Created || res.Intent != first.Intent || res.Intent.PipelineRevision != rev1 {
		t.Fatalf("reevaluation returned %+v, want the unchanged frozen intent on rev1", res.Intent)
	}
	if res.Divergence == nil || !res.Divergence.PipelineRevisionChanged || res.Divergence.PolicyRevisionChanged ||
		res.Divergence.ObservedPipelineRevision != rev2 {
		t.Fatalf("divergence = %+v, want pipeline-revision-only divergence observing rev2", res.Divergence)
	}
	assertUnchanged(t, store, before)

	// Positive control: a different subject under the same policy is a new domain.
	other := autoReq()
	other.InputBindingSubjectIdentity = "subject-2"
	res, err = svc.CreateAutomatic(ctx, other)
	if err != nil || !res.Created || res.Intent.ID == first.Intent.ID {
		t.Fatalf("different subject: res=%+v err=%v, want a new intent", res, err)
	}
}

// ── 4./5. explicit operation idempotency ─────────────────────────────────────

func TestExplicit_SameOperationSameSemantics_Converges(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	first, err := svc.CreateExplicit(ctx, explicitReq())
	if err != nil || !first.Created {
		t.Fatalf("first CreateExplicit: res=%+v err=%v", first, err)
	}
	before := snapshot(store)
	for i := range 10 {
		res, err := svc.CreateExplicit(ctx, explicitReq())
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if res.Created || res.Intent != first.Intent {
			t.Fatalf("replay %d = %+v, want existing %+v", i, res, first.Intent)
		}
	}
	assertUnchanged(t, store, before)
}

func TestExplicit_SameOperationChangedSemantics_ConflictZeroMutation(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	if _, err := svc.CreateExplicit(ctx, explicitReq()); err != nil {
		t.Fatalf("CreateExplicit: %v", err)
	}
	before := snapshot(store)

	changedSubject := explicitReq()
	changedSubject.InputBindingSubjectIdentity = "subject-other"
	changedRevision := explicitReq()
	changedRevision.PipelineRevision = rev2

	for name, req := range map[string]ExplicitRequest{"subject": changedSubject, "revision": changedRevision} {
		t.Run(name, func(t *testing.T) {
			_, err := svc.CreateExplicit(ctx, req)
			assertCode(t, err, ps.CodeOperationConflict)
			assertUnchanged(t, store, before)
		})
	}
}

// ── 6. RunID attach-only fence ───────────────────────────────────────────────

func TestAttachRunID_SameConverges_DifferentConflicts(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	res, err := svc.CreateAutomatic(ctx, autoReq())
	if err != nil {
		t.Fatalf("CreateAutomatic: %v", err)
	}
	if res.Intent.RunID != "" {
		t.Fatalf("new intent carries RunID %q; this package must not mint RunIDs", res.Intent.RunID)
	}
	id := res.Intent.ID

	got, err := svc.AttachRunID(ctx, id, "run-A")
	if err != nil || got.RunID != "run-A" {
		t.Fatalf("attach run-A: got=%+v err=%v", got, err)
	}
	got, err = svc.AttachRunID(ctx, id, "run-A")
	if err != nil || got.RunID != "run-A" {
		t.Fatalf("re-attach run-A: got=%+v err=%v, want convergence", got, err)
	}

	_, err = svc.AttachRunID(ctx, id, "run-B")
	assertCode(t, err, CodeRunIDConflict)
	stored, err := svc.Get(ctx, id)
	if err != nil || stored.RunID != "run-A" {
		t.Fatalf("after conflicting attach: stored=%+v err=%v, want RunID run-A", stored, err)
	}

	_, err = svc.AttachRunID(ctx, "no-such-intent", "run-A")
	assertCode(t, err, ps.CodeNotFound)
}

// ── 7. failure between ledger/index and intent exposes neither ───────────────

func TestMemoryStore_FaultInjection_NoPartialState(t *testing.T) {
	injected := errors.New("injected crash")
	cases := []struct {
		name   string
		step   string
		create func(*Service) (CreateResult, error)
	}{
		{"explicit/after-ledger", stepLedgerStaged, func(s *Service) (CreateResult, error) {
			return s.CreateExplicit(context.Background(), explicitReq())
		}},
		{"explicit/after-intent", stepIntentStaged, func(s *Service) (CreateResult, error) {
			return s.CreateExplicit(context.Background(), explicitReq())
		}},
		{"automatic/after-index", stepIndexStaged, func(s *Service) (CreateResult, error) {
			return s.CreateAutomatic(context.Background(), autoReq())
		}},
		{"automatic/after-intent", stepIntentStaged, func(s *Service) (CreateResult, error) {
			return s.CreateAutomatic(context.Background(), autoReq())
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, store := newTestService(t)
			before := snapshot(store)

			hit := false
			store.fault = func(step string) error {
				if step == tc.step {
					hit = true
					return injected
				}
				return nil
			}
			if _, err := tc.create(svc); !errors.Is(err, injected) {
				t.Fatalf("create err = %v, want injected failure", err)
			}
			if !hit {
				t.Fatalf("fault step %q was never reached", tc.step)
			}
			assertUnchanged(t, store, before)

			store.fault = nil
			res, err := tc.create(svc)
			if err != nil || !res.Created {
				t.Fatalf("retry after fault: res=%+v err=%v, want a fresh create", res, err)
			}
		})
	}
}

// ── 8. empty coordinates / uncommitted revision fail closed ──────────────────

func TestEmptyCoordinates_FailClosedZeroMutation(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	before := snapshot(store)

	autoCases := []struct {
		name   string
		mutate func(*AutomaticRequest)
	}{
		{"policy id", func(r *AutomaticRequest) { r.AutoRunPolicyID = "" }},
		{"policy revision", func(r *AutomaticRequest) { r.AutoRunPolicyRevision = "" }},
		{"subject", func(r *AutomaticRequest) { r.InputBindingSubjectIdentity = "" }},
		{"pipeline id", func(r *AutomaticRequest) { r.PipelineRevision.PipelineID = "" }},
		{"pipeline revision", func(r *AutomaticRequest) { r.PipelineRevision.RevisionID = "" }},
		{"whole revision ref", func(r *AutomaticRequest) { r.PipelineRevision = PipelineRevisionRef{} }},
	}
	for _, tc := range autoCases {
		req := autoReq()
		tc.mutate(&req)
		_, err := svc.CreateAutomatic(ctx, req)
		if ps.CodeOf(err) != CodeMissingCoordinate {
			t.Errorf("automatic %s: err = %v, want %s", tc.name, err, CodeMissingCoordinate)
		}
	}

	explicitCases := []struct {
		name   string
		mutate func(*ExplicitRequest)
	}{
		{"operation id", func(r *ExplicitRequest) { r.OperationID = "" }},
		{"subject", func(r *ExplicitRequest) { r.InputBindingSubjectIdentity = "" }},
		{"pipeline id", func(r *ExplicitRequest) { r.PipelineRevision.PipelineID = "" }},
		{"pipeline revision", func(r *ExplicitRequest) { r.PipelineRevision.RevisionID = "" }},
	}
	for _, tc := range explicitCases {
		req := explicitReq()
		tc.mutate(&req)
		_, err := svc.CreateExplicit(ctx, req)
		if ps.CodeOf(err) != CodeMissingCoordinate {
			t.Errorf("explicit %s: err = %v, want %s", tc.name, err, CodeMissingCoordinate)
		}
	}

	if _, err := svc.AttachRunID(ctx, "", "run-A"); ps.CodeOf(err) != CodeMissingCoordinate {
		t.Errorf("attach with empty intent id: err = %v", err)
	}
	if _, err := svc.AttachRunID(ctx, "some-intent", ""); ps.CodeOf(err) != CodeMissingCoordinate {
		t.Errorf("attach with empty RunID: err = %v", err)
	}

	// A well-formed but uncommitted revision coordinate fails the exact read.
	uncommitted := autoReq()
	uncommitted.PipelineRevision = PipelineRevisionRef{PipelineID: "pipe-a", RevisionID: "rev-unknown"}
	_, err := svc.CreateAutomatic(ctx, uncommitted)
	assertCode(t, err, ps.CodeNotFound)
	uncommittedExplicit := explicitReq()
	uncommittedExplicit.PipelineRevision = PipelineRevisionRef{PipelineID: "pipe-other", RevisionID: "rev-1"}
	_, err = svc.CreateExplicit(ctx, uncommittedExplicit)
	assertCode(t, err, ps.CodeNotFound)

	assertUnchanged(t, store, before)
}

func TestRequireCommitted_RejectsMismatchedRead(t *testing.T) {
	// A reader that answers with a different revision than requested must not
	// be trusted as confirmation of the requested coordinate.
	store := NewMemoryStore()
	svc, err := NewService(store, mismatchedReader{})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	_, err = svc.CreateAutomatic(context.Background(), autoReq())
	assertCode(t, err, ps.CodeNotFound)
	assertUnchanged(t, store, storeState{intents: map[ID]Intent{}, auto: map[autoKey]ID{}, ops: map[string]ID{}})
}

type mismatchedReader struct{}

func (mismatchedReader) GetRevision(_ context.Context, pipelineID string, _ ps.PipelineRevisionID) (*ps.PipelineRevision, error) {
	return &ps.PipelineRevision{PipelineID: pipelineID, RevisionID: "some-other-revision"}, nil
}

// ── 9. no external service dependency in the I0 package ─────────────────────

func TestNoExternalServiceImports(t *testing.T) {
	allowed := map[string]bool{}
	for _, p := range []string{"context", "errors", "fmt", "sync", "github.com/google/uuid", "github.com/HeaInSeo/PipelineStore"} {
		allowed[p] = true
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	checked := 0
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		checked++
		for _, imp := range parsed.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: bad import %s", f, imp.Path.Value)
			}
			if !allowed[path] {
				t.Errorf("%s imports %q; PIPE-I0 must not depend on network, JUMI, PolicyScheduler, Tori, Kubernetes/Kueue, or Authorization packages", f, path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no non-test source files checked")
	}
}

// ── 10. caller mutation cannot reach stored frozen intent ────────────────────

func TestCallerMutation_DoesNotReachStoredIntent(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	req := autoReq()
	res, err := svc.CreateAutomatic(ctx, req)
	if err != nil {
		t.Fatalf("CreateAutomatic: %v", err)
	}
	frozen := res.Intent

	// Mutate the request and the returned value after the call.
	req.AutoRunPolicyRevision = "tampered"
	req.PipelineRevision = rev2
	res.Intent.AutoRunPolicyRevision = "tampered"
	res.Intent.PipelineRevision.RevisionID = "tampered"
	res.Intent.RunID = "tampered"

	stored, err := svc.Get(ctx, frozen.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored != frozen {
		t.Fatalf("stored intent changed by caller mutation:\nstored %+v\nfrozen %+v", stored, frozen)
	}
	if res.Intent == stored || req.PipelineRevision == stored.PipelineRevision {
		t.Fatal("caller-side mutation did not take effect; the test is not exercising aliasing")
	}

	// A value returned by a replay is likewise a copy.
	replay, err := svc.CreateAutomatic(ctx, autoReq())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	replay.Intent.InputBindingSubjectIdentity = "tampered"
	again, err := svc.Get(ctx, frozen.ID)
	if err != nil || again != frozen || replay.Intent == again {
		t.Fatalf("stored intent changed via replay result: stored=%+v err=%v", again, err)
	}
}

func TestNewService_RequiresDependencies(t *testing.T) {
	if _, err := NewService(nil, newCommitted()); err == nil {
		t.Error("NewService(nil store) must fail")
	}
	if _, err := NewService(NewMemoryStore(), nil); err == nil {
		t.Error("NewService(nil reader) must fail")
	}
}
