package intent

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
)

// ── fixtures ─────────────────────────────────────────────────────────────────

// pos is a fake publication-order provider position. Positions compare only
// within one domain. It is acceptance-test evidence only, never a production
// allow path.
type pos struct {
	domain string
	n      int
}

func (p pos) ComparePublication(other PublicationPosition) (int, bool) {
	q, ok := other.(pos)
	if !ok || q.domain != p.domain {
		return 0, false
	}
	switch {
	case p.n < q.n:
		return -1, true
	case p.n > q.n:
		return 1, true
	}
	return 0, true
}

func at(n int) PublicationPosition { return pos{domain: "tori", n: n} }

func newAt(n int) PublicationFacts {
	return PublicationFacts{Position: at(n), Class: OccurrenceNew}
}

func subjectReq(subject string) AutomaticRequest {
	r := autoReq()
	r.InputBindingSubjectIdentity = subject
	return r
}

func mustEnable(t *testing.T, svc *Service, frontier int) PolicyRecord {
	t.Helper()
	rec, err := svc.EnablePolicy(context.Background(), autoReq().AutoRunPolicyID, at(frontier))
	if err != nil {
		t.Fatalf("EnablePolicy(%d): %v", frontier, err)
	}
	return rec
}

func mustAdmit(t *testing.T, svc *Service, req AutomaticRequest, facts PublicationFacts, want AdmissionDecision) AdmissionResult {
	t.Helper()
	res, err := svc.AdmitAutomatic(context.Background(), req, facts)
	if err != nil {
		t.Fatalf("AdmitAutomatic: %v", err)
	}
	if res.Decision != want {
		t.Fatalf("decision = %s (reason %s), want %s", res.Decision, res.Reason, want)
	}
	return res
}

func assertNotAdmitted(t *testing.T, svc *Service, store *MemoryStore, req AutomaticRequest, facts PublicationFacts, want NotAdmittedReason) {
	t.Helper()
	before := snapshot(store)
	res := mustAdmit(t, svc, req, facts, NotAdmitted)
	if res.Reason != want {
		t.Fatalf("reason = %s, want %s", res.Reason, want)
	}
	if res.Result != (CreateResult{}) {
		t.Fatalf("not-admitted result carries an intent: %+v", res.Result)
	}
	assertUnchanged(t, store, before)
}

func assertEligible(t *testing.T, svc *Service, id ID, want bool) {
	t.Helper()
	got, in, err := svc.MaterializationEligible(context.Background(), id)
	if err != nil {
		t.Fatalf("MaterializationEligible: %v", err)
	}
	if got != want {
		t.Fatalf("eligible = %v, want %v (blockers %+v)", got, want, in.Blockers)
	}
}

// ── T1. publication at or before the first frontier is historical ────────────

func TestAdmission_T1_PreFrontierAtFirstActivate(t *testing.T) {
	svc, store := newTestService(t)
	rec := mustEnable(t, svc, 10)
	if rec.State != PolicyActive || rec.Epoch != 1 || rec.Seq != 1 {
		t.Fatalf("first activate = %+v, want ACTIVE epoch 1 seq 1", rec)
	}
	for _, n := range []int{1, 9, 10} {
		assertNotAdmitted(t, svc, store, subjectReq(fmt.Sprint("s", n)), newAt(n), ReasonPreFrontier)
	}
}

// ── T2. publications while disabled are not drained by re-enable ────────────

func TestAdmission_T2_DisabledBacklogNotReplayedOnReenable(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	policy := autoReq().AutoRunPolicyID
	mustEnable(t, svc, 10)
	if _, err := svc.DisablePolicy(ctx, policy); err != nil {
		t.Fatalf("DisablePolicy: %v", err)
	}
	// Published at 12 while disabled.
	assertNotAdmitted(t, svc, store, subjectReq("backlog"), newAt(12), ReasonPolicyNotActive)

	// A frontier earlier than, or not comparable with, the previous one is rejected.
	before := snapshot(store)
	_, err := svc.EnablePolicy(ctx, policy, at(9))
	assertCode(t, err, CodeFrontierInvalid)
	_, err = svc.EnablePolicy(ctx, policy, pos{domain: "other", n: 99})
	assertCode(t, err, CodeFrontierInvalid)
	assertUnchanged(t, store, before)

	rec := mustEnable(t, svc, 15)
	if rec.State != PolicyActive || rec.Epoch != 2 {
		t.Fatalf("re-enable = %+v, want ACTIVE epoch 2", rec)
	}
	assertNotAdmitted(t, svc, store, subjectReq("backlog"), newAt(12), ReasonPreFrontier)
	assertNotAdmitted(t, svc, store, subjectReq("boundary"), newAt(15), ReasonPreFrontier)
	res := mustAdmit(t, svc, subjectReq("fresh"), newAt(16), Admitted)
	if res.Result.Intent.AdmissionEpoch != 2 {
		t.Fatalf("AdmissionEpoch = %d, want 2", res.Result.Intent.AdmissionEpoch)
	}
}

// ── T3. new occurrence after the frontier → exactly one intent ──────────────

func TestAdmission_T3_NewAfterFrontier_ExactlyOneIntent(t *testing.T) {
	svc, store := newTestService(t)
	mustEnable(t, svc, 10)
	res := mustAdmit(t, svc, autoReq(), newAt(11), Admitted)
	in := res.Result.Intent
	if !res.Result.Created || in.ID == "" || in.AdmissionEpoch != 1 || in.Origin != OriginAutomatic {
		t.Fatalf("admitted = %+v", res)
	}
	if !in.Blockers.Clear() {
		t.Fatalf("fresh intent has blockers: %+v", in.Blockers)
	}
	if n := len(snapshot(store).intents); n != 1 {
		t.Fatalf("intents = %d, want 1", n)
	}
}

// ── T4. duplicate delivery / ACK loss → same intent ─────────────────────────

func TestAdmission_T4_DuplicateDelivery_SameIntent(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	mustEnable(t, svc, 10)

	const n = 32
	var wg sync.WaitGroup
	results := make([]AdmissionResult, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = svc.AdmitAutomatic(ctx, autoReq(), newAt(11))
		}()
	}
	wg.Wait()

	admitted := 0
	var id ID
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("admit %d: %v", i, errs[i])
		}
		switch results[i].Decision {
		case Admitted:
			admitted++
		case Replayed:
		default:
			t.Fatalf("admit %d decision = %s", i, results[i].Decision)
		}
		if id == "" {
			id = results[i].Result.Intent.ID
		} else if results[i].Result.Intent.ID != id {
			t.Fatalf("admit %d returned intent %q, want %q", i, results[i].Result.Intent.ID, id)
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted = %d, want 1", admitted)
	}

	// A redelivery after the policy is disabled still converges on the same
	// intent and writes nothing new.
	if _, err := svc.DisablePolicy(ctx, autoReq().AutoRunPolicyID); err != nil {
		t.Fatalf("DisablePolicy: %v", err)
	}
	before := snapshot(store)
	res := mustAdmit(t, svc, autoReq(), newAt(11), Replayed)
	if res.Result.Intent.ID != id {
		t.Fatalf("replay after disable returned %q, want %q", res.Result.Intent.ID, id)
	}
	assertUnchanged(t, store, before)
}

// ── T5. activation / publication boundary is decided by the log only ────────

func TestAdmission_T5_BoundaryAndUndeterminablePositions(t *testing.T) {
	svc, store := newTestService(t)
	mustEnable(t, svc, 10)
	assertNotAdmitted(t, svc, store, subjectReq("at-frontier"), newAt(10), ReasonPreFrontier)
	assertNotAdmitted(t, svc, store, subjectReq("other-domain"),
		PublicationFacts{Position: pos{domain: "other", n: 99}, Class: OccurrenceNew}, ReasonPositionNotComparable)
	assertNotAdmitted(t, svc, store, subjectReq("no-position"),
		PublicationFacts{Class: OccurrenceNew}, ReasonPositionUnknown)
}

func TestAdmission_T5_ConcurrentActivateAtBoundary_NeverAdmitted(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n+1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := svc.EnablePolicy(ctx, autoReq().AutoRunPolicyID, at(10)); err != nil {
			errs <- err
		}
	}()
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := svc.AdmitAutomatic(ctx, subjectReq(fmt.Sprint("s", i)), newAt(10))
			if err != nil {
				errs <- err
				return
			}
			if res.Decision != NotAdmitted {
				errs <- fmt.Errorf("subject %d at the frontier was %s", i, res.Decision)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := len(snapshot(store).intents); got != 0 {
		t.Fatalf("intents = %d, want 0", got)
	}
}

func TestAdmission_T5_ConcurrentDisable_NoUnheldAdmissionSlipsThrough(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	policy := autoReq().AutoRunPolicyID
	mustEnable(t, svc, 10)

	const n = 64
	var wg sync.WaitGroup
	errs := make(chan error, n+1)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.AdmitAutomatic(ctx, subjectReq(fmt.Sprint("s", i)), newAt(11+i)); err != nil {
				errs <- err
			}
		}()
		if i == n/2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := svc.DisablePolicy(ctx, policy); err != nil {
					errs <- err
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	// Every intent was admitted while ACTIVE at epoch 1 and is held by the
	// disable that followed it in the log.
	for id, in := range snapshot(store).intents {
		if in.AdmissionEpoch != 1 || in.Blockers.PolicyDisable != BlockerBlocked {
			t.Errorf("intent %q = %+v, want epoch 1 and held", id, in)
		}
	}
	assertNotAdmitted(t, svc, store, subjectReq("late"), newAt(1000), ReasonPolicyNotActive)
}

// ── T6. crash at the frontier write / restart → same epoch and frontier ─────

func TestAdmission_T6_CrashAtActivate_NoDoubleFrontier(t *testing.T) {
	injected := errors.New("injected crash")
	ctx := context.Background()
	policy := autoReq().AutoRunPolicyID
	svc, store := newTestService(t)
	before := snapshot(store)

	store.fault = func(step string) error {
		if step == stepPolicyStaged {
			return injected
		}
		return nil
	}
	if _, err := svc.EnablePolicy(ctx, policy, at(10)); !errors.Is(err, injected) {
		t.Fatalf("EnablePolicy err = %v, want injected", err)
	}
	assertUnchanged(t, store, before)
	store.fault = nil

	mustEnable(t, svc, 10)

	// Restart: a new Service over the same store retries the activation with
	// a later head. The committed epoch and frontier are kept.
	restarted, err := NewService(store, newCommitted(rev1, rev2))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	rec, err := restarted.EnablePolicy(ctx, policy, at(20))
	if err != nil {
		t.Fatalf("retry EnablePolicy: %v", err)
	}
	if rec.Epoch != 1 || !reflect.DeepEqual(rec.Frontier, at(10)) {
		t.Fatalf("after retry = %+v, want epoch 1 frontier 10", rec)
	}
	log, err := restarted.PolicyLog(ctx, policy)
	if err != nil {
		t.Fatalf("PolicyLog: %v", err)
	}
	if len(log) != 1 || log[0].Kind != TransitionActivate || log[0].Seq != 1 {
		t.Fatalf("log = %+v, want one ACTIVATE", log)
	}
	mustAdmit(t, restarted, autoReq(), newAt(15), Admitted)
}

func TestAdmission_FaultInjection_NoPartialState(t *testing.T) {
	injected := errors.New("injected crash")
	ctx := context.Background()
	policy := autoReq().AutoRunPolicyID
	cases := []struct {
		name string
		step string
		run  func(*Service) error
	}{
		{"disable/after-log", stepPolicyStaged, func(s *Service) error {
			_, err := s.DisablePolicy(ctx, policy)
			return err
		}},
		{"disable/after-hold", stepHoldStaged, func(s *Service) error {
			_, err := s.DisablePolicy(ctx, policy)
			return err
		}},
		{"retire/after-hold", stepHoldStaged, func(s *Service) error {
			_, err := s.RetirePolicy(ctx, policy)
			return err
		}},
		{"admit/after-index", stepIndexStaged, func(s *Service) error {
			_, err := s.AdmitAutomatic(ctx, subjectReq("new"), newAt(20))
			return err
		}},
		{"admit/after-intent", stepIntentStaged, func(s *Service) error {
			_, err := s.AdmitAutomatic(ctx, subjectReq("new"), newAt(20))
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, store := newTestService(t)
			mustEnable(t, svc, 10)
			mustAdmit(t, svc, autoReq(), newAt(11), Admitted)
			before := snapshot(store)

			hit := false
			store.fault = func(step string) error {
				if step == tc.step {
					hit = true
					return injected
				}
				return nil
			}
			if err := tc.run(svc); !errors.Is(err, injected) {
				t.Fatalf("err = %v, want injected failure", err)
			}
			if !hit {
				t.Fatalf("fault step %q was never reached", tc.step)
			}
			assertUnchanged(t, store, before)

			store.fault = nil
			if err := tc.run(svc); err != nil {
				t.Fatalf("retry after fault: %v", err)
			}
		})
	}
}

// ── T7. policy revision update never replays history ────────────────────────

func TestAdmission_T7_PolicyRevisionUpdate_NoHistoricalReplay(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	mustEnable(t, svc, 10)
	first := mustAdmit(t, svc, autoReq(), newAt(11), Admitted).Result.Intent

	updated := autoReq()
	updated.AutoRunPolicyRevision = "policy-rev-2"
	res := mustAdmit(t, svc, updated, newAt(11), Replayed)
	if res.Result.Intent != first {
		t.Fatalf("replay rewrote the frozen intent: %+v, want %+v", res.Result.Intent, first)
	}
	if res.Result.Divergence == nil || !res.Result.Divergence.PolicyRevisionChanged {
		t.Fatalf("divergence = %+v, want policy revision change", res.Result.Divergence)
	}

	historical := updated
	historical.InputBindingSubjectIdentity = "historical"
	assertNotAdmitted(t, svc, store, historical, newAt(5), ReasonPreFrontier)

	log, err := svc.PolicyLog(ctx, autoReq().AutoRunPolicyID)
	if err != nil || len(log) != 1 {
		t.Fatalf("log = %+v err=%v, want the single ACTIVATE", log, err)
	}
}

// ── T8 / T9. only a NEW occurrence is an automatic candidate ────────────────

func TestAdmission_T8_T9_OccurrenceClassFailsClosed(t *testing.T) {
	svc, store := newTestService(t)
	mustEnable(t, svc, 10)
	cases := []struct {
		class OccurrenceClass
		want  NotAdmittedReason
	}{
		{OccurrenceReexpression, ReasonOccurrenceNotNew},
		{OccurrenceUnknown, ReasonOccurrenceUnknown},
		{"", ReasonOccurrenceUnknown},
		{"new", ReasonOccurrenceUnknown},
	}
	for _, tc := range cases {
		assertNotAdmitted(t, svc, store, subjectReq("known"),
			PublicationFacts{Position: at(20), Class: tc.class}, tc.want)
	}
}

// ── T10. a retired policy ID is never reused ────────────────────────────────

func TestAdmission_T10_RetireThenReuse_Rejected(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	policy := autoReq().AutoRunPolicyID
	mustEnable(t, svc, 10)
	rec, err := svc.RetirePolicy(ctx, policy)
	if err != nil || rec.Policy.State != PolicyRetired {
		t.Fatalf("RetirePolicy = %+v err=%v", rec, err)
	}
	if again, err := svc.RetirePolicy(ctx, policy); err != nil || again.Policy != rec.Policy {
		t.Fatalf("second RetirePolicy = %+v err=%v, want unchanged", again, err)
	}

	before := snapshot(store)
	_, err = svc.EnablePolicy(ctx, policy, at(50))
	assertCode(t, err, CodePolicyRetired)
	_, err = svc.DisablePolicy(ctx, policy)
	assertCode(t, err, CodePolicyRetired)
	assertUnchanged(t, store, before)
	assertNotAdmitted(t, svc, store, autoReq(), newAt(60), ReasonPolicyNotActive)
}

// ── D3. disable: HOLD / reconcile / keep running; no implicit release ───────

func TestAdmission_D3_DisableBranches(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	policy := autoReq().AutoRunPolicyID
	mustEnable(t, svc, 10)
	unassigned := mustAdmit(t, svc, subjectReq("unassigned"), newAt(11), Admitted).Result.Intent.ID
	ambiguous := mustAdmit(t, svc, subjectReq("ambiguous"), newAt(12), Admitted).Result.Intent.ID
	accepted := mustAdmit(t, svc, subjectReq("accepted"), newAt(13), Admitted).Result.Intent.ID
	for id, run := range map[ID]RunID{ambiguous: "run-ambiguous", accepted: "run-accepted"} {
		if _, err := svc.AttachRunID(ctx, id, run); err != nil {
			t.Fatalf("AttachRunID: %v", err)
		}
	}

	res, err := svc.DisablePolicy(ctx, policy)
	if err != nil {
		t.Fatalf("DisablePolicy: %v", err)
	}
	if !reflect.DeepEqual(res.Held, []ID{unassigned}) {
		t.Fatalf("Held = %v, want [%s]", res.Held, unassigned)
	}
	wantAttached := []ID{ambiguous, accepted}
	if wantAttached[0] > wantAttached[1] {
		wantAttached[0], wantAttached[1] = wantAttached[1], wantAttached[0]
	}
	if !reflect.DeepEqual(res.RunIDAttached, wantAttached) {
		t.Fatalf("RunIDAttached = %v, want %v", res.RunIDAttached, wantAttached)
	}
	assertEligible(t, svc, unassigned, false)
	assertEligible(t, svc, ambiguous, true)
	assertEligible(t, svc, accepted, true)

	// Disabling again is idempotent and reports the same sets.
	if again, err := svc.DisablePolicy(ctx, policy); err != nil || !reflect.DeepEqual(again, res) {
		t.Fatalf("second DisablePolicy = %+v err=%v, want %+v", again, err, res)
	}

	// Same-RunID reconcile: unknown acceptance fails closed to HOLD; accepted
	// keeps running and is never cancelled.
	if _, err := svc.RecordDisabledAcceptance(ctx, ambiguous, AcceptanceUnknown); err != nil {
		t.Fatalf("RecordDisabledAcceptance(unknown): %v", err)
	}
	if in, err := svc.RecordDisabledAcceptance(ctx, accepted, AcceptanceAccepted); err != nil || !in.Blockers.Clear() {
		t.Fatalf("RecordDisabledAcceptance(accepted) = %+v err=%v", in, err)
	}
	assertEligible(t, svc, ambiguous, false)
	assertEligible(t, svc, accepted, true)
	_, err = svc.RecordDisabledAcceptance(ctx, unassigned, AcceptanceNotAccepted)
	assertCode(t, err, CodeInvalidTransition)

	// Re-enable releases nothing implicitly.
	mustEnable(t, svc, 20)
	assertEligible(t, svc, unassigned, false)
	assertEligible(t, svc, ambiguous, false)

	// Each owner releases only its own blocker.
	if _, err := svc.SetBlocker(ctx, unassigned, BlockerAuthorization, BlockerUnknown); err != nil {
		t.Fatalf("SetBlocker: %v", err)
	}
	if _, err := svc.ReleaseBlocker(ctx, unassigned, BlockerPolicyDisable); err != nil {
		t.Fatalf("ReleaseBlocker: %v", err)
	}
	assertEligible(t, svc, unassigned, false) // UNKNOWN authorization still blocks
	if _, err := svc.ReleaseBlocker(ctx, unassigned, BlockerAuthorization); err != nil {
		t.Fatalf("ReleaseBlocker: %v", err)
	}
	assertEligible(t, svc, unassigned, true)
}

func TestAdmission_D3_RetireHoldsLikeDisable(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	mustEnable(t, svc, 10)
	id := mustAdmit(t, svc, autoReq(), newAt(11), Admitted).Result.Intent.ID
	res, err := svc.RetirePolicy(ctx, autoReq().AutoRunPolicyID)
	if err != nil || !reflect.DeepEqual(res.Held, []ID{id}) {
		t.Fatalf("RetirePolicy = %+v err=%v, want %s held", res, err, id)
	}
	assertEligible(t, svc, id, false)
}

// ── validation / fail-closed inputs ─────────────────────────────────────────

func TestAdmission_InvalidInputs_FailClosedZeroMutation(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	policy := autoReq().AutoRunPolicyID

	assertNotAdmitted(t, svc, store, autoReq(), newAt(11), ReasonPolicyNotActive)

	before := snapshot(store)
	_, err := svc.EnablePolicy(ctx, policy, nil)
	assertCode(t, err, CodeMissingCoordinate)
	_, err = svc.EnablePolicy(ctx, "", at(1))
	assertCode(t, err, CodeMissingCoordinate)
	_, err = svc.DisablePolicy(ctx, policy)
	assertCode(t, err, ps.CodeNotFound)
	_, err = svc.RetirePolicy(ctx, policy)
	assertCode(t, err, ps.CodeNotFound)
	assertUnchanged(t, store, before)

	mustEnable(t, svc, 10)
	uncommitted := autoReq()
	uncommitted.PipelineRevision = PipelineRevisionRef{PipelineID: "pipe-a", RevisionID: "rev-missing"}
	before = snapshot(store)
	if _, err := svc.AdmitAutomatic(ctx, uncommitted, newAt(11)); err == nil {
		t.Fatal("admission against an uncommitted revision succeeded")
	}
	missingSubject := autoReq()
	missingSubject.InputBindingSubjectIdentity = ""
	_, err = svc.AdmitAutomatic(ctx, missingSubject, newAt(11))
	assertCode(t, err, CodeMissingCoordinate)
	assertUnchanged(t, store, before)

	id := mustAdmit(t, svc, autoReq(), newAt(11), Admitted).Result.Intent.ID
	before = snapshot(store)
	_, err = svc.SetBlocker(ctx, id, "SOMEONE_ELSE", BlockerBlocked)
	assertCode(t, err, CodeInvalidTransition)
	_, err = svc.SetBlocker(ctx, id, BlockerAuthorization, BlockerClear)
	assertCode(t, err, CodeInvalidTransition)
	_, err = svc.ReleaseBlocker(ctx, "no-such-intent", BlockerAuthorization)
	assertCode(t, err, ps.CodeNotFound)
	assertUnchanged(t, store, before)
}

func TestAdmission_BlockerFault_NoPartialState(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	mustEnable(t, svc, 10)
	id := mustAdmit(t, svc, autoReq(), newAt(11), Admitted).Result.Intent.ID
	before := snapshot(store)
	injected := errors.New("injected crash")
	store.fault = func(step string) error {
		if step == stepBlockerStaged {
			return injected
		}
		return nil
	}
	if _, err := svc.SetBlocker(ctx, id, BlockerCampaignPause, BlockerBlocked); !errors.Is(err, injected) {
		t.Fatalf("SetBlocker err = %v, want injected", err)
	}
	assertUnchanged(t, store, before)
}
