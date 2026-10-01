package sqlitestore

import (
	"context"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
	"github.com/HeaInSeo/PipelineStore/intent"
)

// The intent store shares the revision store's SQLite file: an explicit intent
// confirmed against a committed revision on the same file survives a reopen of
// that file together with the revision, and its replay converges.
func TestIntentStore_SharesFileWithRevisionsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	s, path := openTemp(t)
	rev := mustCommit(t, s, "op-rev", "pipe-a", validBody).Revision
	intents, err := s.IntentStore(ctx)
	if err != nil {
		t.Fatalf("IntentStore: %v", err)
	}
	svc, err := intent.NewService(intents, s)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	req := intent.ExplicitRequest{
		OperationID:                 "op-intent",
		InputBindingSubjectIdentity: "subject-1",
		PipelineRevision:            intent.PipelineRevisionRef{PipelineID: rev.PipelineID, RevisionID: rev.RevisionID},
	}
	first, err := svc.CreateExplicit(ctx, req)
	if err != nil || !first.Created {
		t.Fatalf("CreateExplicit: %+v err=%v", first, err)
	}
	missing := req
	missing.OperationID = "op-missing"
	missing.PipelineRevision.RevisionID = "not-committed"
	if _, err := svc.CreateExplicit(ctx, missing); ps.CodeOf(err) != ps.CodeNotFound {
		t.Fatalf("uncommitted revision: err=%v, want %s", err, ps.CodeNotFound)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := Open(path, testResolvers())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	reopened, err := r.IntentStore(ctx)
	if err != nil {
		t.Fatalf("IntentStore after reopen: %v", err)
	}
	svc, err = intent.NewService(reopened, r)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	again, err := svc.CreateExplicit(ctx, req)
	if err != nil || again.Created || again.Intent != first.Intent {
		t.Fatalf("replay after reopen: %+v err=%v, want %+v", again, err, first.Intent)
	}
	if _, err := r.GetRevision(ctx, rev.PipelineID, rev.RevisionID); err != nil {
		t.Fatalf("revision after reopen: %v", err)
	}
	if got := r.revisionCount(t); got != 1 {
		t.Fatalf("revisions = %d, want 1", got)
	}
}
