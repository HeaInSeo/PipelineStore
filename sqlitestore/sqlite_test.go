package sqlitestore

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	ps "github.com/HeaInSeo/PipelineStore"
	"github.com/HeaInSeo/PipelineStore/fake"
)

func testResolvers() ps.Resolvers {
	tf := fake.NewToolFunctionCatalog()
	tf.Add("cas-a", ps.ToolFunctionDecl{
		Outputs:    map[string]ps.OutputPortDecl{"out": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle}},
		Parameters: map[string]ps.ParameterDecl{"threads": {AllowedValues: []string{"1", "2", "4"}}},
	})
	tf.Add("cas-b", ps.ToolFunctionDecl{
		Inputs:  map[string]ps.InputPortDecl{"in": {DataFormat: "fastq", Cardinality: ps.CardinalitySingle, Required: true}},
		Outputs: map[string]ps.OutputPortDecl{"out": {DataFormat: "bam", Cardinality: ps.CardinalitySingle}},
	})
	return ps.Resolvers{ToolFunction: tf, Sori: fake.NewSoriCatalog()}
}

func bodyWithThreads(v string) []byte {
	return []byte(`{"semantic_derivation_version":"pipelinestore.pipeline-contract.v1","canonicalization_version":"pipelinestore.pipeline-contract.v1",
      "nodes":[{"node_id":"a","tool_function_cas_hash":"cas-a","fixed_parameters":[{"name":"threads","value":"` + v + `"}]},{"node_id":"b","tool_function_cas_hash":"cas-b","fixed_parameters":[]}],
      "direct_edges":[{"from_node_id":"a","from_output_port":"out","to_node_id":"b","to_input_port":"in"}],
      "reusable_asset_bindings":[],"external_input_slots":[]}`)
}

var validBody = bodyWithThreads("4")

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	s, err := Open(path, testResolvers())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

// revisionCount is an in-package helper for asserting no partial mutation.
func (s *Store) revisionCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM revisions`).Scan(&n); err != nil {
		t.Fatalf("count revisions: %v", err)
	}
	return n
}

func mustCommit(t *testing.T, s *Store, op, pipeline string, body []byte) ps.CommitResult {
	t.Helper()
	res, err := s.Commit(context.Background(), ps.CommitRequest{OperationID: op, PipelineID: pipeline, Contract: body})
	if err != nil {
		t.Fatalf("commit(%s,%s): %v", op, pipeline, err)
	}
	return res
}

// T04 (identity half): same body under different PipelineID -> same content
// digest but distinct revision identity.
func TestT04_DistinctIdentitySameDigest(t *testing.T) {
	s, _ := openTemp(t)
	r1 := mustCommit(t, s, "op1", "pipeA", validBody)
	r2 := mustCommit(t, s, "op2", "pipeB", validBody)
	if r1.Revision.ContractDigest != r2.Revision.ContractDigest {
		t.Fatal("same body under different pipelines should share a content digest")
	}
	if r1.Revision.RevisionID == r2.Revision.RevisionID {
		t.Fatal("different pipelines must yield distinct revision identities")
	}
	if !r1.Created || !r2.Created {
		t.Fatal("both commits should have created new revisions")
	}
}

// T05: same PipelineID + canon version + digest, different operation -> return
// the existing revision.
func TestT05_ContentConvergence(t *testing.T) {
	s, _ := openTemp(t)
	r1 := mustCommit(t, s, "op1", "pipe", validBody)
	r2 := mustCommit(t, s, "op2", "pipe", validBody)
	if r1.Revision.RevisionID != r2.Revision.RevisionID {
		t.Fatal("convergent commit should return the existing revision id")
	}
	if r2.Created {
		t.Fatal("convergent commit must not mint a new revision")
	}
	if got := s.revisionCount(t); got != 1 {
		t.Fatalf("expected exactly 1 revision, got %d", got)
	}
}

// T06: same operation + different body -> conflict, zero mutation.
func TestT06_OperationConflict(t *testing.T) {
	s, _ := openTemp(t)
	r1 := mustCommit(t, s, "op1", "pipe", validBody)

	_, err := s.Commit(context.Background(), ps.CommitRequest{OperationID: "op1", PipelineID: "pipe", Contract: bodyWithThreads("2")})
	if ps.CodeOf(err) != ps.CodeOperationConflict {
		t.Fatalf("expected OPERATION_CONFLICT, got %v", err)
	}
	if got := s.revisionCount(t); got != 1 {
		t.Fatalf("conflict must not mutate: expected 1 revision, got %d", got)
	}
	// Original revision remains intact and readable.
	if _, err := s.GetRevision(context.Background(), "pipe", r1.Revision.RevisionID); err != nil {
		t.Fatalf("original revision should still be readable: %v", err)
	}
}

// T14 & T27: a fault around the durable commit boundary leaves no half
// revision/operation/index, and no RevisionID becomes externally addressable.
func TestT14_T27_FaultAtCommitBoundary(t *testing.T) {
	s, path := openTemp(t)
	s.faultBeforeCommit = func() error { return errors.New("simulated crash") }

	res, err := s.Commit(context.Background(), ps.CommitRequest{OperationID: "op1", PipelineID: "pipe", Contract: validBody})
	if err == nil {
		t.Fatal("expected commit to fail under injected fault")
	}
	if res.Revision != nil {
		t.Fatal("no revision may be returned when the durable commit did not succeed")
	}
	if got := s.revisionCount(t); got != 0 {
		t.Fatalf("fault must leave zero revisions, got %d", got)
	}
	// Nothing durable was written: reopen and confirm the ledger/index are empty.
	_ = s.Close()
	s2, err := Open(path, testResolvers())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	if got := s2.revisionCount(t); got != 0 {
		t.Fatalf("after restart, fault must leave zero revisions, got %d", got)
	}
	var ops int
	if err := s2.db.QueryRow(`SELECT COUNT(*) FROM operations`).Scan(&ops); err != nil {
		t.Fatalf("count operations: %v", err)
	}
	if ops != 0 {
		t.Fatalf("fault must leave zero operation-ledger rows, got %d", ops)
	}
}

// T15: crash/retry never mints duplicate revisions; a retry after a fault mints
// exactly one, and after restart an idempotent retry returns the same revision.
func TestT15_CrashRetryNoDuplicate(t *testing.T) {
	s, path := openTemp(t)

	// First attempt faults before the durable commit.
	s.faultBeforeCommit = func() error { return errors.New("crash") }
	if _, err := s.Commit(context.Background(), ps.CommitRequest{OperationID: "op1", PipelineID: "pipe", Contract: validBody}); err == nil {
		t.Fatal("expected fault")
	}

	// Retry with the same operation id now succeeds and mints exactly one revision.
	s.faultBeforeCommit = nil
	r := mustCommit(t, s, "op1", "pipe", validBody)
	if !r.Created {
		t.Fatal("retry after fault should create the revision")
	}
	if got := s.revisionCount(t); got != 1 {
		t.Fatalf("crash+retry must not duplicate: expected 1 revision, got %d", got)
	}

	// Restart and retry the same operation: same committed revision, no duplicate.
	_ = s.Close()
	s2, err := Open(path, testResolvers())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	r2 := mustCommit(t, s2, "op1", "pipe", validBody)
	if r2.Created {
		t.Fatal("post-restart retry must not create a new revision")
	}
	if r2.Revision.RevisionID != r.Revision.RevisionID {
		t.Fatal("post-restart retry must return the same committed revision")
	}
	if got := s2.revisionCount(t); got != 1 {
		t.Fatalf("expected 1 revision after restart+retry, got %d", got)
	}
}

// T19: stored body corruption / stored digest mismatch on exact read -> fail closed.
func TestT19_IntegrityFailClosed(t *testing.T) {
	s, _ := openTemp(t)
	r := mustCommit(t, s, "op1", "pipe", validBody)

	// Corrupt the stored canonical body.
	if _, err := s.db.Exec(`UPDATE revisions SET canonical_body = ? WHERE pipeline_id = ? AND revision_id = ?`,
		[]byte(`{"tampered":true}`), "pipe", string(r.Revision.RevisionID)); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	_, err := s.GetRevision(context.Background(), "pipe", r.Revision.RevisionID)
	if ps.CodeOf(err) != ps.CodeIntegrity {
		t.Fatalf("expected INTEGRITY_ERROR on corrupted body, got %v", err)
	}

	// Fresh store: tamper the stored digest instead.
	s2, _ := openTemp(t)
	r2 := mustCommit(t, s2, "op1", "pipe", validBody)
	if _, err := s2.db.Exec(`UPDATE revisions SET contract_digest = ? WHERE pipeline_id = ? AND revision_id = ?`,
		strings.Repeat("0", 64), "pipe", string(r2.Revision.RevisionID)); err != nil {
		t.Fatalf("tamper digest: %v", err)
	}
	_, err = s2.GetRevision(context.Background(), "pipe", r2.Revision.RevisionID)
	if ps.CodeOf(err) != ps.CodeIntegrity {
		t.Fatalf("expected INTEGRITY_ERROR on digest mismatch, got %v", err)
	}
}

// Read-path fails closed when a redundant version column is tampered while the
// canonical body + digest stay mutually consistent (the body is authoritative
// and digest-protected; a drifted column must not be returned). Guards the §8
// "fail closed on any mismatch" contract symmetrically across both version columns.
func TestReadCrossChecksVersionColumns(t *testing.T) {
	s, _ := openTemp(t)
	r := mustCommit(t, s, "op1", "pipe", validBody)
	// Tamper ONLY the semantic_derivation_version column; canonical_body and
	// contract_digest remain mutually consistent, so the digest check passes.
	if _, err := s.db.Exec(`UPDATE revisions SET semantic_derivation_version = ? WHERE pipeline_id = ? AND revision_id = ?`,
		"evil", "pipe", string(r.Revision.RevisionID)); err != nil {
		t.Fatalf("tamper semver column: %v", err)
	}
	_, err := s.GetRevision(context.Background(), "pipe", r.Revision.RevisionID)
	if ps.CodeOf(err) != ps.CodeIntegrity {
		t.Fatalf("expected INTEGRITY_ERROR on semantic_derivation_version column drift, got %v", err)
	}
}

// T28: an acknowledged commit survives restart and an exact read passes digest
// integrity.
func TestT28_AckSurvivesRestart(t *testing.T) {
	s, path := openTemp(t)
	r := mustCommit(t, s, "op1", "pipe", validBody)
	_ = s.Close()

	s2, err := Open(path, testResolvers())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	got, err := s2.GetRevision(context.Background(), "pipe", r.Revision.RevisionID)
	if err != nil {
		t.Fatalf("exact read after restart failed: %v", err)
	}
	if got.ContractDigest != r.Revision.ContractDigest {
		t.Fatal("digest changed across restart")
	}
	if string(got.CanonicalBody) != string(r.Revision.CanonicalBody) {
		t.Fatal("canonical body changed across restart")
	}
}

// Exact read miss returns NOT_FOUND.
func TestGetRevision_NotFound(t *testing.T) {
	s, _ := openTemp(t)
	_, err := s.GetRevision(context.Background(), "pipe", ps.PipelineRevisionID("nope"))
	if ps.CodeOf(err) != ps.CodeNotFound {
		t.Fatalf("expected NOT_FOUND, got %v", err)
	}
}

// Concurrent identical commits (same operation id) converge to one revision with
// no duplication and no data race.
func TestConcurrentIdempotentCommit(t *testing.T) {
	s, _ := openTemp(t)
	const n = 8
	var wg sync.WaitGroup
	ids := make([]ps.PipelineRevisionID, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := s.Commit(context.Background(), ps.CommitRequest{OperationID: "op1", PipelineID: "pipe", Contract: validBody})
			errs[i] = err
			if err == nil {
				ids[i] = res.Revision.RevisionID
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("commit %d failed: %v", i, err)
		}
	}
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("idempotent concurrent commits diverged: %s vs %s", ids[i], ids[0])
		}
	}
	if got := s.revisionCount(t); got != 1 {
		t.Fatalf("expected exactly 1 revision, got %d", got)
	}
}
