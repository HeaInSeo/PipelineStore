package intent

import (
	"context"
	"errors"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
)

// storeUnderTest is one Store backend plus its fault-injection hook.
type storeUnderTest struct {
	store    Store
	setFault func(func(step string) error)
}

// storeFactories lists every Store backend that must satisfy the shared
// intent/automatic-key/operation-ledger/RunID/blocker contract below.
func storeFactories() map[string]func(t *testing.T) storeUnderTest {
	return map[string]func(t *testing.T) storeUnderTest{
		"memory": func(*testing.T) storeUnderTest {
			m := NewMemoryStore()
			return storeUnderTest{store: m, setFault: func(f func(string) error) { m.fault = f }}
		},
		"sqlite": func(t *testing.T) storeUnderTest {
			s := newSQLiteStore(t, openSQLite(t, sqlitePath(t)))
			return storeUnderTest{store: s, setFault: func(f func(string) error) { s.fault = f }}
		},
	}
}

func runStoreContract(t *testing.T, name string, check func(t *testing.T, sut storeUnderTest)) {
	t.Helper()
	for backend, factory := range storeFactories() {
		t.Run(backend+"/"+name, func(t *testing.T) { check(t, factory(t)) })
	}
}

func automaticDraft(subject string) Intent {
	return Intent{
		Origin: OriginAutomatic, AutoRunPolicyID: "policy-1", AutoRunPolicyRevision: "policy-rev-1",
		InputBindingSubjectIdentity: subject, PipelineRevision: rev1,
	}
}

func explicitDraft(op string) Intent {
	return Intent{Origin: OriginExplicit, OperationID: op, InputBindingSubjectIdentity: "subject-1", PipelineRevision: rev1}
}

func mustCreateAutomatic(t *testing.T, s Store, subject string) Intent {
	t.Helper()
	in, _, err := s.createAutomatic(context.Background(), automaticDraft(subject))
	if err != nil {
		t.Fatalf("createAutomatic(%s): %v", subject, err)
	}
	return in
}

func mustGetIntent(t *testing.T, s Store, id ID) Intent {
	t.Helper()
	in, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return in
}

func TestStoreContract_AutomaticUniqueness(t *testing.T) {
	runStoreContract(t, "automatic", func(t *testing.T, sut storeUnderTest) {
		ctx := context.Background()
		first, created, err := sut.store.createAutomatic(ctx, automaticDraft("subject-1"))
		if err != nil || !created || first.ID == "" {
			t.Fatalf("first create: created=%v id=%q err=%v", created, first.ID, err)
		}
		changed := automaticDraft("subject-1")
		changed.AutoRunPolicyRevision, changed.PipelineRevision = "policy-rev-2", rev2
		again, created, err := sut.store.createAutomatic(ctx, changed)
		if err != nil || created || again != first {
			t.Fatalf("replay: created=%v err=%v got %+v want frozen %+v", created, err, again, first)
		}
		looked, found, err := sut.store.LookupAutomatic(ctx, "policy-1", "subject-1")
		if err != nil || !found || looked != first {
			t.Fatalf("LookupAutomatic: found=%v err=%v got %+v", found, err, looked)
		}
		if other := mustCreateAutomatic(t, sut.store, "subject-2"); other.ID == first.ID {
			t.Fatalf("distinct subjects share intent %q", first.ID)
		}
		if _, found, err := sut.store.LookupAutomatic(ctx, "policy-1", "subject-3"); err != nil || found {
			t.Fatalf("LookupAutomatic(miss): found=%v err=%v", found, err)
		}
	})
}

func TestStoreContract_ExplicitLedger(t *testing.T) {
	runStoreContract(t, "explicit", func(t *testing.T, sut storeUnderTest) {
		ctx := context.Background()
		bad := explicitDraft("op-bad")
		bad.RunID = "run-preset"
		if _, _, err := sut.store.createExplicit(ctx, bad); ps.CodeOf(err) != ps.CodeInvalidContract {
			t.Fatalf("preset RunID draft: err=%v, want %s", err, ps.CodeInvalidContract)
		}
		first, created, err := sut.store.createExplicit(ctx, explicitDraft("op-1"))
		if err != nil || !created {
			t.Fatalf("first create: created=%v err=%v", created, err)
		}
		again, created, err := sut.store.createExplicit(ctx, explicitDraft("op-1"))
		if err != nil || created || again != first {
			t.Fatalf("replay: created=%v err=%v got %+v want %+v", created, err, again, first)
		}
		looked, found, err := sut.store.LookupExplicit(ctx, "op-1")
		if err != nil || !found || looked != first {
			t.Fatalf("LookupExplicit: found=%v err=%v got %+v", found, err, looked)
		}
		if _, found, err := sut.store.LookupExplicit(ctx, "op-bad"); err != nil || found {
			t.Fatalf("rejected draft was recorded: found=%v err=%v", found, err)
		}
	})
}

func TestStoreContract_AttachRunID(t *testing.T) {
	runStoreContract(t, "attach", func(t *testing.T, sut storeUnderTest) {
		ctx := context.Background()
		if _, err := sut.store.AttachRunID(ctx, "missing", "run-1"); ps.CodeOf(err) != ps.CodeNotFound {
			t.Fatalf("attach to missing intent: err=%v, want %s", err, ps.CodeNotFound)
		}
		a := mustCreateAutomatic(t, sut.store, "subject-a")
		b := mustCreateAutomatic(t, sut.store, "subject-b")
		attached, err := sut.store.AttachRunID(ctx, a.ID, "run-1")
		if err != nil || attached.RunID != "run-1" {
			t.Fatalf("attach: %+v err=%v", attached, err)
		}
		if again, err := sut.store.AttachRunID(ctx, a.ID, "run-1"); err != nil || again != attached {
			t.Fatalf("same RunID re-attach: %+v err=%v", again, err)
		}
		if _, err := sut.store.AttachRunID(ctx, a.ID, "run-2"); ps.CodeOf(err) != CodeRunIDConflict {
			t.Fatalf("different RunID: err=%v, want %s", err, CodeRunIDConflict)
		}
		if _, err := sut.store.AttachRunID(ctx, b.ID, "run-1"); ps.CodeOf(err) != CodeRunIDConflict {
			t.Fatalf("RunID owned by another intent: err=%v, want %s", err, CodeRunIDConflict)
		}
		if got := mustGetIntent(t, sut.store, b.ID); got.RunID != "" {
			t.Fatalf("conflicting attach mutated intent b: %+v", got)
		}
		if got := mustGetIntent(t, sut.store, a.ID); got != attached {
			t.Fatalf("Get after attach: %+v want %+v", got, attached)
		}
	})
}

func TestStoreContract_Blockers(t *testing.T) {
	runStoreContract(t, "blockers", func(t *testing.T, sut storeUnderTest) {
		ctx := context.Background()
		if _, err := sut.store.UpdateBlocker(ctx, "missing", BlockerAuthorization, BlockerBlocked); ps.CodeOf(err) != ps.CodeNotFound {
			t.Fatalf("missing intent: err=%v, want %s", err, ps.CodeNotFound)
		}
		in := mustCreateAutomatic(t, sut.store, "subject-1")
		if _, err := sut.store.UpdateBlocker(ctx, in.ID, "NOT_AN_OWNER", BlockerBlocked); ps.CodeOf(err) != CodeInvalidTransition {
			t.Fatalf("unknown owner: err=%v, want %s", err, CodeInvalidTransition)
		}
		held, err := sut.store.UpdateBlocker(ctx, in.ID, BlockerAuthorization, BlockerBlocked)
		if err != nil || held.Blockers != (Blockers{Authorization: BlockerBlocked}) {
			t.Fatalf("block authorization: %+v err=%v", held.Blockers, err)
		}
		held, err = sut.store.UpdateBlocker(ctx, in.ID, BlockerCampaignPause, BlockerUnknown)
		if err != nil || held.Blockers != (Blockers{Authorization: BlockerBlocked, CampaignPause: BlockerUnknown}) {
			t.Fatalf("second owner: %+v err=%v", held.Blockers, err)
		}
		cleared, err := sut.store.UpdateBlocker(ctx, in.ID, BlockerAuthorization, BlockerClear)
		if err != nil || cleared.Blockers != (Blockers{CampaignPause: BlockerUnknown}) {
			t.Fatalf("release one owner: %+v err=%v", cleared.Blockers, err)
		}
		if got := mustGetIntent(t, sut.store, in.ID); got != cleared {
			t.Fatalf("Get after blockers: %+v want %+v", got, cleared)
		}
	})
}

// Every staged step of every write aborts the whole call: nothing becomes
// visible, and a retry without the fault applies exactly once.
func TestStoreContract_FaultInjectionIsAtomic(t *testing.T) {
	boom := errors.New("injected crash")
	failAt := func(sut storeUnderTest, step string) {
		sut.setFault(func(s string) error {
			if s == step {
				return boom
			}
			return nil
		})
	}
	for _, step := range []string{stepIndexStaged, stepIntentStaged} {
		runStoreContract(t, "createAutomatic@"+step, func(t *testing.T, sut storeUnderTest) {
			failAt(sut, step)
			if _, _, err := sut.store.createAutomatic(context.Background(), automaticDraft("subject-1")); !errors.Is(err, boom) {
				t.Fatalf("err=%v, want injected", err)
			}
			if _, found, _ := sut.store.LookupAutomatic(context.Background(), "policy-1", "subject-1"); found {
				t.Fatal("aborted automatic create is visible")
			}
			sut.setFault(nil)
			if _, created, err := sut.store.createAutomatic(context.Background(), automaticDraft("subject-1")); err != nil || !created {
				t.Fatalf("retry: created=%v err=%v", created, err)
			}
		})
	}
	for _, step := range []string{stepLedgerStaged, stepIntentStaged} {
		runStoreContract(t, "createExplicit@"+step, func(t *testing.T, sut storeUnderTest) {
			failAt(sut, step)
			if _, _, err := sut.store.createExplicit(context.Background(), explicitDraft("op-1")); !errors.Is(err, boom) {
				t.Fatalf("err=%v, want injected", err)
			}
			if _, found, _ := sut.store.LookupExplicit(context.Background(), "op-1"); found {
				t.Fatal("aborted explicit create is visible")
			}
			sut.setFault(nil)
			if _, created, err := sut.store.createExplicit(context.Background(), explicitDraft("op-1")); err != nil || !created {
				t.Fatalf("retry: created=%v err=%v", created, err)
			}
		})
	}
	for _, step := range []string{stepRunIndexStaged, stepIntentStaged} {
		runStoreContract(t, "attachRunID@"+step, func(t *testing.T, sut storeUnderTest) {
			a := mustCreateAutomatic(t, sut.store, "subject-a")
			b := mustCreateAutomatic(t, sut.store, "subject-b")
			failAt(sut, step)
			if _, err := sut.store.AttachRunID(context.Background(), a.ID, "run-1"); !errors.Is(err, boom) {
				t.Fatalf("err=%v, want injected", err)
			}
			sut.setFault(nil)
			if got := mustGetIntent(t, sut.store, a.ID); got.RunID != "" {
				t.Fatalf("aborted attach is visible: %+v", got)
			}
			// The RunID index entry must have been rolled back too.
			if got, err := sut.store.AttachRunID(context.Background(), b.ID, "run-1"); err != nil || got.RunID != "run-1" {
				t.Fatalf("RunID still reserved after aborted attach: %+v err=%v", got, err)
			}
		})
	}
	runStoreContract(t, "updateBlocker@"+stepBlockerStaged, func(t *testing.T, sut storeUnderTest) {
		in := mustCreateAutomatic(t, sut.store, "subject-1")
		failAt(sut, stepBlockerStaged)
		if _, err := sut.store.UpdateBlocker(context.Background(), in.ID, BlockerAuthorization, BlockerBlocked); !errors.Is(err, boom) {
			t.Fatalf("err=%v, want injected", err)
		}
		if got := mustGetIntent(t, sut.store, in.ID); got != in {
			t.Fatalf("aborted blocker update is visible: %+v", got)
		}
	})
}
