package intent

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"

	ps "github.com/HeaInSeo/PipelineStore"
)

// Store is the storage boundary for RunGenerationIntent state. A production
// implementation must make each method's writes crash-safe and atomic: a
// failure at any point leaves either all of a call's writes or none of them.
// Stored intents are immutable except for the one-time RunID attach.
type Store interface {
	// LookupAutomatic returns the intent recorded for the automatic uniqueness
	// domain (policyID, subject), with found=false when there is none.
	LookupAutomatic(ctx context.Context, policyID, subject string) (stored Intent, found bool, err error)
	// LookupExplicit returns the intent recorded for operationID, with
	// found=false when there is none.
	LookupExplicit(ctx context.Context, operationID string) (stored Intent, found bool, err error)
	// CreateAutomatic returns the intent already recorded for the draft's
	// automatic uniqueness domain (AutoRunPolicyID, InputBindingSubjectIdentity)
	// with created=false, or records the draft under a newly allocated ID
	// together with its uniqueness index entry and returns created=true. An
	// existing intent is never modified.
	CreateAutomatic(ctx context.Context, draft Intent) (stored Intent, created bool, err error)
	// CreateExplicit returns the intent already recorded for draft.OperationID
	// with created=false, or records the operation-ledger entry and the new
	// intent together and returns created=true. An existing intent is never
	// modified; comparing its semantics with the draft is the caller's job.
	CreateExplicit(ctx context.Context, draft Intent) (stored Intent, created bool, err error)
	// AttachRunID attaches run to the intent. Attaching the RunID that is
	// already attached is a no-op. Attaching a different RunID, or a RunID
	// already attached to another intent in this store, fails with
	// CodeRunIDConflict and changes nothing; the RunID-to-intent index and the
	// intent are written together. An unknown id fails with ps.CodeNotFound.
	AttachRunID(ctx context.Context, id ID, run RunID) (Intent, error)
	// Get returns the intent by exact id, or ps.CodeNotFound.
	Get(ctx context.Context, id ID) (Intent, error)

	// GetPolicy returns the record folded from policyID's lifecycle log, with
	// found=false when the log is empty.
	GetPolicy(ctx context.Context, policyID string) (rec PolicyRecord, found bool, err error)
	// PolicyLog returns a copy of policyID's lifecycle log in order.
	PolicyLog(ctx context.Context, policyID string) ([]PolicyTransition, error)
	// GetPolicyMembership returns GetPolicy's result together with the
	// policy's lifecycle membership, both read in one serialized read.
	GetPolicyMembership(ctx context.Context, policyID string) (rec PolicyRecord, members PolicyMembership, found bool, err error)
	// AppendPolicyTransition appends t to policyID's log only if the log's
	// current length is expectedSeq, else fails with CodePolicyStale and
	// changes nothing. t.Seq is assigned by the store. When holdUnassigned is
	// set, every automatic intent of the policy without a RunID gets a
	// POLICY_DISABLE blocker in the same atomic write, and the returned
	// membership is captured in that write, after the holds; otherwise it is
	// empty.
	AppendPolicyTransition(ctx context.Context, policyID string, expectedSeq uint64, t PolicyTransition, holdUnassigned bool) (PolicyRecord, PolicyMembership, error)
	// CreateAutomaticAdmitted behaves as CreateAutomatic when an intent already
	// exists for the draft's automatic uniqueness domain. Otherwise it records
	// the draft only if the draft's policy is ACTIVE at epoch, and fails with
	// CodePolicyStale, changing nothing, if it is not. The policy check and the
	// create are one serialized write.
	CreateAutomaticAdmitted(ctx context.Context, draft Intent, epoch uint64) (stored Intent, created bool, err error)
	// UpdateBlocker sets owner's blocker on the intent to state (BlockerClear
	// releases it) and leaves every other owner's blocker unchanged. An unknown
	// id fails with ps.CodeNotFound.
	UpdateBlocker(ctx context.Context, id ID, owner BlockerOwner, state BlockerState) (Intent, error)
}

// Fault-injection points inside a MemoryStore write, after a write is staged and
// before the call's writes become visible. A durable Store's crash window sits
// at the same points.
const (
	stepIndexStaged    = "auto-index-staged"
	stepLedgerStaged   = "operation-ledger-staged"
	stepRunIndexStaged = "run-index-staged"
	stepIntentStaged   = "intent-staged"
	stepPolicyStaged   = "policy-log-staged"
	stepHoldStaged     = "hold-staged"
	stepBlockerStaged  = "blocker-staged"
)

type autoKey struct {
	policyID string
	subject  string
}

// MemoryStore is the in-memory reference implementation of Store. Each call
// runs under one lock and makes its writes visible together, so it models the
// atomic contract; it is NOT durable and is not evidence of production
// durability.
type MemoryStore struct {
	mu      sync.Mutex
	intents map[ID]Intent
	auto    map[autoKey]ID
	ops     map[string]ID
	runs    map[RunID]ID

	policyLogs map[string][]PolicyTransition
	policies   map[string]PolicyRecord

	// fault, when set by tests, is called at each staged step of a write and
	// aborts the write when it returns an error.
	fault func(step string) error
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		intents: map[ID]Intent{},
		auto:    map[autoKey]ID{},
		ops:     map[string]ID{},
		runs:    map[RunID]ID{},

		policyLogs: map[string][]PolicyTransition{},
		policies:   map[string]PolicyRecord{},
	}
}

// LookupAutomatic implements Store.
func (m *MemoryStore) LookupAutomatic(ctx context.Context, policyID, subject string) (Intent, bool, error) {
	if err := ctx.Err(); err != nil {
		return Intent{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	id, ok := m.auto[autoKey{policyID: policyID, subject: subject}]
	if !ok {
		return Intent{}, false, nil
	}
	return m.intents[id], true, nil
}

// LookupExplicit implements Store.
func (m *MemoryStore) LookupExplicit(ctx context.Context, operationID string) (Intent, bool, error) {
	if err := ctx.Err(); err != nil {
		return Intent{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	id, ok := m.ops[operationID]
	if !ok {
		return Intent{}, false, nil
	}
	return m.intents[id], true, nil
}

func (m *MemoryStore) inject(step string) error {
	if m.fault == nil {
		return nil
	}
	return m.fault(step)
}

func (m *MemoryStore) allocateID() (ID, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	id := ID(u.String())
	if _, taken := m.intents[id]; taken {
		return "", newError(ps.CodeDuplicate, "allocated intent id %q already exists", id)
	}
	return id, nil
}

// CreateAutomatic implements Store.
func (m *MemoryStore) CreateAutomatic(ctx context.Context, draft Intent) (Intent, bool, error) {
	if err := ctx.Err(); err != nil {
		return Intent{}, false, err
	}
	key := autoKey{policyID: draft.AutoRunPolicyID, subject: draft.InputBindingSubjectIdentity}

	m.mu.Lock()
	defer m.mu.Unlock()

	if id, ok := m.auto[key]; ok {
		return m.intents[id], false, nil
	}
	id, err := m.allocateID()
	if err != nil {
		return Intent{}, false, err
	}
	draft.ID = id
	if err := m.inject(stepIndexStaged); err != nil {
		return Intent{}, false, err
	}
	if err := m.inject(stepIntentStaged); err != nil {
		return Intent{}, false, err
	}
	m.auto[key] = id
	m.intents[id] = draft
	return draft, true, nil
}

// CreateExplicit implements Store.
func (m *MemoryStore) CreateExplicit(ctx context.Context, draft Intent) (Intent, bool, error) {
	if err := ctx.Err(); err != nil {
		return Intent{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if id, ok := m.ops[draft.OperationID]; ok {
		return m.intents[id], false, nil
	}
	id, err := m.allocateID()
	if err != nil {
		return Intent{}, false, err
	}
	draft.ID = id
	if err := m.inject(stepLedgerStaged); err != nil {
		return Intent{}, false, err
	}
	if err := m.inject(stepIntentStaged); err != nil {
		return Intent{}, false, err
	}
	m.ops[draft.OperationID] = id
	m.intents[id] = draft
	return draft, true, nil
}

// AttachRunID implements Store.
func (m *MemoryStore) AttachRunID(ctx context.Context, id ID, run RunID) (Intent, error) {
	if err := ctx.Err(); err != nil {
		return Intent{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	in, ok := m.intents[id]
	if !ok {
		return Intent{}, newError(ps.CodeNotFound, "intent %q not found", id)
	}
	if in.RunID == run {
		return in, nil
	}
	if in.RunID != "" {
		return Intent{}, newError(CodeRunIDConflict, "intent %q already has RunID %q; refusing %q", id, in.RunID, run)
	}
	if owner, taken := m.runs[run]; taken {
		return Intent{}, newError(CodeRunIDConflict, "RunID %q is already attached to intent %q; refusing intent %q", run, owner, id)
	}
	if err := m.inject(stepRunIndexStaged); err != nil {
		return Intent{}, err
	}
	if err := m.inject(stepIntentStaged); err != nil {
		return Intent{}, err
	}
	in.RunID = run
	m.runs[run] = id
	m.intents[id] = in
	return in, nil
}

// Get implements Store.
func (m *MemoryStore) Get(ctx context.Context, id ID) (Intent, error) {
	if err := ctx.Err(); err != nil {
		return Intent{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	in, ok := m.intents[id]
	if !ok {
		return Intent{}, newError(ps.CodeNotFound, "intent %q not found", id)
	}
	return in, nil
}

// GetPolicy implements Store.
func (m *MemoryStore) GetPolicy(ctx context.Context, policyID string) (PolicyRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return PolicyRecord{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.policies[policyID]
	return rec, ok, nil
}

// PolicyLog implements Store.
func (m *MemoryStore) PolicyLog(ctx context.Context, policyID string) ([]PolicyTransition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]PolicyTransition(nil), m.policyLogs[policyID]...), nil
}

// GetPolicyMembership implements Store.
func (m *MemoryStore) GetPolicyMembership(ctx context.Context, policyID string) (PolicyRecord, PolicyMembership, bool, error) {
	if err := ctx.Err(); err != nil {
		return PolicyRecord{}, PolicyMembership{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.policies[policyID]
	if !ok {
		return PolicyRecord{}, PolicyMembership{}, false, nil
	}
	return rec, m.membershipLocked(policyID), true, nil
}

// membershipLocked classifies policyID's automatic intents. m.mu must be held.
func (m *MemoryStore) membershipLocked(policyID string) PolicyMembership {
	var out PolicyMembership
	for key, id := range m.auto {
		if key.policyID != policyID {
			continue
		}
		in := m.intents[id]
		switch {
		case in.Blockers.PolicyDisable != BlockerClear:
			out.Held = append(out.Held, id)
		case in.RunID != "":
			out.RunIDAttached = append(out.RunIDAttached, id)
		}
	}
	sort.Slice(out.Held, func(i, j int) bool { return out.Held[i] < out.Held[j] })
	sort.Slice(out.RunIDAttached, func(i, j int) bool { return out.RunIDAttached[i] < out.RunIDAttached[j] })
	return out
}

// AppendPolicyTransition implements Store.
func (m *MemoryStore) AppendPolicyTransition(ctx context.Context, policyID string, expectedSeq uint64, t PolicyTransition, holdUnassigned bool) (PolicyRecord, PolicyMembership, error) {
	if err := ctx.Err(); err != nil {
		return PolicyRecord{}, PolicyMembership{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	log := m.policyLogs[policyID]
	if uint64(len(log)) != expectedSeq {
		return PolicyRecord{}, PolicyMembership{}, newError(CodePolicyStale,
			"policy %q log length is %d, expected %d", policyID, len(log), expectedSeq)
	}
	t.Seq = expectedSeq + 1
	next := m.policies[policyID].apply(policyID, t)

	var held []ID
	if holdUnassigned {
		for key, id := range m.auto {
			in := m.intents[id]
			if key.policyID == policyID && in.RunID == "" && in.Blockers.PolicyDisable != BlockerBlocked {
				held = append(held, id)
			}
		}
	}
	if err := m.inject(stepPolicyStaged); err != nil {
		return PolicyRecord{}, PolicyMembership{}, err
	}
	if holdUnassigned {
		if err := m.inject(stepHoldStaged); err != nil {
			return PolicyRecord{}, PolicyMembership{}, err
		}
	}
	m.policyLogs[policyID] = append(log[:len(log):len(log)], t)
	m.policies[policyID] = next
	for _, id := range held {
		in := m.intents[id]
		in.Blockers.PolicyDisable = BlockerBlocked
		m.intents[id] = in
	}
	var members PolicyMembership
	if holdUnassigned {
		members = m.membershipLocked(policyID)
	}
	return next, members, nil
}

// CreateAutomaticAdmitted implements Store.
func (m *MemoryStore) CreateAutomaticAdmitted(ctx context.Context, draft Intent, epoch uint64) (Intent, bool, error) {
	if err := ctx.Err(); err != nil {
		return Intent{}, false, err
	}
	key := autoKey{policyID: draft.AutoRunPolicyID, subject: draft.InputBindingSubjectIdentity}

	m.mu.Lock()
	defer m.mu.Unlock()

	if id, ok := m.auto[key]; ok {
		return m.intents[id], false, nil
	}
	rec, ok := m.policies[draft.AutoRunPolicyID]
	if !ok || rec.State != PolicyActive || rec.Epoch != epoch {
		return Intent{}, false, newError(CodePolicyStale,
			"policy %q is no longer ACTIVE at epoch %d", draft.AutoRunPolicyID, epoch)
	}
	id, err := m.allocateID()
	if err != nil {
		return Intent{}, false, err
	}
	draft.ID = id
	if err := m.inject(stepIndexStaged); err != nil {
		return Intent{}, false, err
	}
	if err := m.inject(stepIntentStaged); err != nil {
		return Intent{}, false, err
	}
	m.auto[key] = id
	m.intents[id] = draft
	return draft, true, nil
}

// UpdateBlocker implements Store.
func (m *MemoryStore) UpdateBlocker(ctx context.Context, id ID, owner BlockerOwner, state BlockerState) (Intent, error) {
	if err := ctx.Err(); err != nil {
		return Intent{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	in, ok := m.intents[id]
	if !ok {
		return Intent{}, newError(ps.CodeNotFound, "intent %q not found", id)
	}
	slot := in.Blockers.slot(owner)
	if slot == nil {
		return Intent{}, newError(CodeInvalidTransition, "unknown blocker owner %q", owner)
	}
	if *slot == state {
		return in, nil
	}
	if err := m.inject(stepBlockerStaged); err != nil {
		return Intent{}, err
	}
	*slot = state
	m.intents[id] = in
	return in, nil
}
