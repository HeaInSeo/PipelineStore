package intent

import (
	"context"

	ps "github.com/HeaInSeo/PipelineStore"
)

// PublicationPosition is an opaque position in one admission domain's
// authoritative publication order. It is supplied by the caller (the source
// publication authority) and is never derived by this package from time,
// event, delivery, listing, or row order. Implementations must be immutable.
type PublicationPosition interface {
	// ComparePublication returns -1, 0 or +1 as the receiver is before, at,
	// or after other. ok is false when the two positions are not comparable:
	// other belongs to a different admission domain, or the order between
	// them cannot be determined. Callers treat ok=false as fail-closed.
	ComparePublication(other PublicationPosition) (cmp int, ok bool)
}

// OccurrenceClass is the source-published provenance class of a subject's
// publication. Only OccurrenceNew is an automatic admission candidate.
type OccurrenceClass string

const (
	// OccurrenceNew is a new underlying occurrence.
	OccurrenceNew OccurrenceClass = "NEW"
	// OccurrenceReexpression is a re-expression of a known occurrence
	// (revision change, scope widening, reclassification, supersession).
	OccurrenceReexpression OccurrenceClass = "REEXPRESSION"
	// OccurrenceUnknown is an unknown provenance class. It is never
	// auto-admitted; it is backfill-class.
	OccurrenceUnknown OccurrenceClass = "UNKNOWN"
)

// PublicationFacts are the caller-provided publication facts for one subject.
// A nil Position is an unknown position.
type PublicationFacts struct {
	Position PublicationPosition
	Class    OccurrenceClass
}

// PolicyState is the lifecycle state of an AutoRunPolicyID.
type PolicyState string

const (
	PolicyActive   PolicyState = "ACTIVE"
	PolicyDisabled PolicyState = "DISABLED"
	// PolicyRetired is terminal; the ID is never reused.
	PolicyRetired PolicyState = "RETIRED"
)

// TransitionKind is the kind of one policy lifecycle log entry.
type TransitionKind string

const (
	TransitionActivate TransitionKind = "ACTIVATE"
	TransitionDisable  TransitionKind = "DISABLE"
	TransitionReenable TransitionKind = "REENABLE"
	TransitionRetire   TransitionKind = "RETIRE"
)

// PolicyTransition is one entry of a policy's serialized lifecycle log. The
// frontier of an ACTIVATE or REENABLE is recorded in the same entry as the
// state change, so the two can never be observed apart.
type PolicyTransition struct {
	// Seq is the 1-based position of the entry in the policy's log.
	Seq      uint64
	Kind     TransitionKind
	Epoch    uint64
	Frontier PublicationPosition
}

// PolicyRecord is the current state of a policy, folded from its log.
type PolicyRecord struct {
	PolicyID string
	State    PolicyState
	// Epoch starts at 1 on ACTIVATE and increases by one on each REENABLE.
	Epoch uint64
	// Frontier is the publication position captured by the transition that
	// opened the current epoch. Only positions strictly after it are admitted.
	Frontier PublicationPosition
	// Seq is the length of the policy's log; it is the compare-and-append
	// token for the next transition.
	Seq uint64
}

// apply returns the record after appending t.
func (r PolicyRecord) apply(policyID string, t PolicyTransition) PolicyRecord {
	r.PolicyID = policyID
	r.Seq = t.Seq
	r.Epoch = t.Epoch
	switch t.Kind {
	case TransitionActivate, TransitionReenable:
		r.State = PolicyActive
		r.Frontier = t.Frontier
	case TransitionDisable:
		r.State = PolicyDisabled
	case TransitionRetire:
		r.State = PolicyRetired
	}
	return r
}

// BlockerOwner names the one authority allowed to set and release a blocker.
type BlockerOwner string

const (
	BlockerPolicyDisable         BlockerOwner = "POLICY_DISABLE"
	BlockerCampaignPause         BlockerOwner = "CAMPAIGN_PAUSE"
	BlockerAuthorization         BlockerOwner = "AUTHORIZATION"
	BlockerMaterializationPrereq BlockerOwner = "MATERIALIZATION_PREREQ"
)

// BlockerState is one owner's hold. The zero value is clear.
type BlockerState string

const (
	BlockerClear   BlockerState = ""
	BlockerBlocked BlockerState = "BLOCKED"
	// BlockerUnknown counts as blocked.
	BlockerUnknown BlockerState = "UNKNOWN"
)

// Blockers is the owner-scoped hold set of an intent. It is a value type so
// intents never alias stored state.
type Blockers struct {
	PolicyDisable         BlockerState
	CampaignPause         BlockerState
	Authorization         BlockerState
	MaterializationPrereq BlockerState
}

// Clear reports whether every blocker is clear.
func (b Blockers) Clear() bool {
	return b == Blockers{}
}

// slot returns the owner's blocker field, or nil for an unknown owner.
func (b *Blockers) slot(owner BlockerOwner) *BlockerState {
	switch owner {
	case BlockerPolicyDisable:
		return &b.PolicyDisable
	case BlockerCampaignPause:
		return &b.CampaignPause
	case BlockerAuthorization:
		return &b.Authorization
	case BlockerMaterializationPrereq:
		return &b.MaterializationPrereq
	}
	return nil
}

// AdmissionDecision is the outcome of the automatic admission gate.
type AdmissionDecision string

const (
	// Admitted: this call recorded a new automatic intent.
	Admitted AdmissionDecision = "ADMITTED"
	// Replayed: an intent already exists for (policy, subject) and is
	// returned unchanged, with any Divergence.
	Replayed AdmissionDecision = "REPLAYED"
	// NotAdmitted: nothing was written; see NotAdmittedReason.
	NotAdmitted AdmissionDecision = "NOT_ADMITTED"
)

// NotAdmittedReason explains a NotAdmitted decision. Every reason is
// fail-closed: the subject is left to an explicit Backfill/Campaign.
type NotAdmittedReason string

const (
	ReasonPolicyNotActive       NotAdmittedReason = "POLICY_NOT_ACTIVE"
	ReasonOccurrenceNotNew      NotAdmittedReason = "OCCURRENCE_NOT_NEW"
	ReasonOccurrenceUnknown     NotAdmittedReason = "OCCURRENCE_UNKNOWN"
	ReasonPositionUnknown       NotAdmittedReason = "POSITION_UNKNOWN"
	ReasonPositionNotComparable NotAdmittedReason = "POSITION_NOT_COMPARABLE"
	ReasonPreFrontier           NotAdmittedReason = "PRE_FRONTIER"
)

// AdmissionResult is the outcome of Service.AdmitAutomatic.
type AdmissionResult struct {
	Decision AdmissionDecision
	// Reason is set only for NotAdmitted.
	Reason NotAdmittedReason
	// Policy is the policy record the gate evaluated. It is zero for Replayed
	// and when the policy does not exist.
	Policy PolicyRecord
	// Result is set for Admitted and Replayed.
	Result CreateResult
}

// PolicyMembership is the lifecycle membership of a policy's automatic
// intents. A Store captures it in the same serialized write or read as the
// policy record it accompanies, so it never includes an intent admitted under
// a later epoch than that record. Both lists are sorted.
type PolicyMembership struct {
	// Held lists the policy's intents that hold a POLICY_DISABLE blocker.
	Held []ID
	// RunIDAttached lists the policy's intents with a RunID and no
	// POLICY_DISABLE blocker. Each must be reconciled by the same RunID and
	// reported through RecordDisabledAcceptance; disable never cancels them.
	RunIDAttached []ID
}

// LifecycleResult is the outcome of a disable or retire. Its membership is a
// snapshot taken atomically with Policy.
type LifecycleResult struct {
	Policy PolicyRecord
	PolicyMembership
}

// AcceptanceObservation is the caller's same-RunID reconcile result for an
// intent whose policy was disabled after the RunID was attached.
type AcceptanceObservation string

const (
	AcceptanceAccepted    AcceptanceObservation = "ACCEPTED"
	AcceptanceNotAccepted AcceptanceObservation = "NOT_ACCEPTED"
	AcceptanceUnknown     AcceptanceObservation = "UNKNOWN"
)

// maxPolicyRetries bounds compare-and-append retries against concurrent
// policy transitions.
const maxPolicyRetries = 8

func (s *Service) requirePolicyID(policyID string) error {
	if policyID == "" {
		return missing("AutoRunPolicyID")
	}
	return nil
}

// Policy returns the current record of policyID, with found=false if the
// policy was never activated.
func (s *Service) Policy(ctx context.Context, policyID string) (PolicyRecord, bool, error) {
	if err := s.requirePolicyID(policyID); err != nil {
		return PolicyRecord{}, false, err
	}
	return s.store.GetPolicy(ctx, policyID)
}

// PolicyLog returns a copy of policyID's lifecycle log in order.
func (s *Service) PolicyLog(ctx context.Context, policyID string) ([]PolicyTransition, error) {
	if err := s.requirePolicyID(policyID); err != nil {
		return nil, err
	}
	return s.store.PolicyLog(ctx, policyID)
}

// EnablePolicy activates policyID with the given frontier, or re-enables a
// disabled policy under a new epoch with a new frontier. frontier must be the
// source's publication head as of this transition; only publications strictly
// after it are admitted in the opened epoch, so a backlog published before
// activation or while disabled is never implicitly replayed.
//
// Enabling an ACTIVE policy returns the current record unchanged: a retry
// after a crash never opens a second epoch or moves the frontier. A re-enable
// frontier must be comparable with and not earlier than the previous one.
// A retired policy fails with CodePolicyRetired.
func (s *Service) EnablePolicy(ctx context.Context, policyID string, frontier PublicationPosition) (PolicyRecord, error) {
	if err := s.requirePolicyID(policyID); err != nil {
		return PolicyRecord{}, err
	}
	if frontier == nil {
		return PolicyRecord{}, missing("frontier")
	}
	for range maxPolicyRetries {
		rec, found, err := s.store.GetPolicy(ctx, policyID)
		if err != nil {
			return PolicyRecord{}, err
		}
		t := PolicyTransition{Kind: TransitionActivate, Epoch: 1, Frontier: frontier}
		if found {
			switch rec.State {
			case PolicyActive:
				return rec, nil
			case PolicyRetired:
				return PolicyRecord{}, newError(CodePolicyRetired, "policy %q is retired and cannot be reused", policyID)
			case PolicyDisabled:
				cmp, ok := frontier.ComparePublication(rec.Frontier)
				if !ok || cmp < 0 {
					return PolicyRecord{}, newError(CodeFrontierInvalid,
						"re-enable frontier of policy %q is not comparable with or is earlier than the epoch %d frontier",
						policyID, rec.Epoch)
				}
				t = PolicyTransition{Kind: TransitionReenable, Epoch: rec.Epoch + 1, Frontier: frontier}
			}
		}
		next, _, err := s.store.AppendPolicyTransition(ctx, policyID, rec.Seq, t, false)
		if ps.CodeOf(err) == CodePolicyStale {
			continue
		}
		return next, err
	}
	return PolicyRecord{}, newError(CodePolicyStale, "policy %q changed concurrently on every attempt", policyID)
}

// DisablePolicy disables an ACTIVE policy. In the same atomic write every
// automatic intent of the policy without a RunID gets a POLICY_DISABLE
// blocker (HOLD). Intents with a RunID are listed for same-RunID reconcile and
// are never cancelled. Disable is not a drain: a later re-enable does not
// release any blocker. Disabling a DISABLED policy returns its current state.
func (s *Service) DisablePolicy(ctx context.Context, policyID string) (LifecycleResult, error) {
	return s.stopPolicy(ctx, policyID, TransitionDisable)
}

// RetirePolicy retires a policy permanently with the same holds as
// DisablePolicy. The ID can never be enabled again. Retiring a RETIRED policy
// returns its current state.
func (s *Service) RetirePolicy(ctx context.Context, policyID string) (LifecycleResult, error) {
	return s.stopPolicy(ctx, policyID, TransitionRetire)
}

func (s *Service) stopPolicy(ctx context.Context, policyID string, kind TransitionKind) (LifecycleResult, error) {
	if err := s.requirePolicyID(policyID); err != nil {
		return LifecycleResult{}, err
	}
	target := PolicyDisabled
	if kind == TransitionRetire {
		target = PolicyRetired
	}
	for range maxPolicyRetries {
		// The record and its membership are read together, so an idempotent
		// repeat never reports intents admitted after a concurrent re-enable.
		rec, members, found, err := s.store.GetPolicyMembership(ctx, policyID)
		if err != nil {
			return LifecycleResult{}, err
		}
		if !found {
			return LifecycleResult{}, newError(ps.CodeNotFound, "policy %q not found", policyID)
		}
		if rec.State == target {
			return LifecycleResult{Policy: rec, PolicyMembership: members}, nil
		}
		if rec.State == PolicyRetired {
			return LifecycleResult{}, newError(CodePolicyRetired, "policy %q is retired", policyID)
		}
		t := PolicyTransition{Kind: kind, Epoch: rec.Epoch}
		next, members, err := s.store.AppendPolicyTransition(ctx, policyID, rec.Seq, t, true)
		if ps.CodeOf(err) == CodePolicyStale {
			continue
		}
		if err != nil {
			return LifecycleResult{}, err
		}
		return LifecycleResult{Policy: next, PolicyMembership: members}, nil
	}
	return LifecycleResult{}, newError(CodePolicyStale, "policy %q changed concurrently on every attempt", policyID)
}

// AdmitAutomatic is the automatic admission gate. An existing intent for
// (AutoRunPolicyID, InputBindingSubjectIdentity) is replayed as in
// CreateAutomatic. Otherwise a new intent is recorded only if the policy is
// ACTIVE, the occurrence class is exactly NEW, and the publication position is
// known, comparable with, and strictly after the current epoch's frontier.
// Every other case is NotAdmitted and writes nothing.
//
// The create is conditional on the policy still being ACTIVE at the evaluated
// epoch, so an admission can never be recorded after a disable, retire, or
// re-enable it did not observe.
func (s *Service) AdmitAutomatic(ctx context.Context, req AutomaticRequest, facts PublicationFacts) (AdmissionResult, error) {
	if err := req.validate(); err != nil {
		return AdmissionResult{}, err
	}
	existing, found, err := s.store.LookupAutomatic(ctx, req.AutoRunPolicyID, req.InputBindingSubjectIdentity)
	if err != nil {
		return AdmissionResult{}, err
	}
	if found {
		return AdmissionResult{Decision: Replayed, Result: automaticReplay(existing, req)}, nil
	}
	revisionChecked := false
	for range maxPolicyRetries {
		rec, found, err := s.store.GetPolicy(ctx, req.AutoRunPolicyID)
		if err != nil {
			return AdmissionResult{}, err
		}
		if !found {
			return AdmissionResult{Decision: NotAdmitted, Reason: ReasonPolicyNotActive}, nil
		}
		if reason, ok := admissible(rec, facts); !ok {
			return AdmissionResult{Decision: NotAdmitted, Reason: reason, Policy: rec}, nil
		}
		if !revisionChecked {
			if err := s.requireCommitted(ctx, req.PipelineRevision); err != nil {
				return AdmissionResult{}, err
			}
			revisionChecked = true
		}
		stored, created, err := s.store.CreateAutomaticAdmitted(ctx, Intent{
			Origin:                      OriginAutomatic,
			AutoRunPolicyID:             req.AutoRunPolicyID,
			AutoRunPolicyRevision:       req.AutoRunPolicyRevision,
			InputBindingSubjectIdentity: req.InputBindingSubjectIdentity,
			PipelineRevision:            req.PipelineRevision,
			AdmissionEpoch:              rec.Epoch,
		}, rec.Epoch)
		if ps.CodeOf(err) == CodePolicyStale {
			continue
		}
		if err != nil {
			return AdmissionResult{}, err
		}
		if !created {
			// Lost a race to a concurrent creator after the lookup.
			return AdmissionResult{Decision: Replayed, Result: automaticReplay(stored, req)}, nil
		}
		return AdmissionResult{Decision: Admitted, Policy: rec, Result: CreateResult{Intent: stored, Created: true}}, nil
	}
	return AdmissionResult{}, newError(CodePolicyStale, "policy %q changed concurrently on every attempt", req.AutoRunPolicyID)
}

// admissible applies the fail-closed admission rules to one policy record.
func admissible(rec PolicyRecord, facts PublicationFacts) (NotAdmittedReason, bool) {
	if rec.State != PolicyActive {
		return ReasonPolicyNotActive, false
	}
	switch facts.Class {
	case OccurrenceNew:
	case OccurrenceReexpression:
		return ReasonOccurrenceNotNew, false
	default:
		return ReasonOccurrenceUnknown, false
	}
	if facts.Position == nil {
		return ReasonPositionUnknown, false
	}
	cmp, ok := facts.Position.ComparePublication(rec.Frontier)
	if !ok {
		return ReasonPositionNotComparable, false
	}
	if cmp <= 0 {
		return ReasonPreFrontier, false
	}
	return "", true
}

// RecordDisabledAcceptance applies the caller's same-RunID reconcile result to
// an automatic intent with an attached RunID. ACCEPTED leaves the intent
// running; NOT_ACCEPTED and anything else (including UNKNOWN) set the
// POLICY_DISABLE blocker, fail-closed.
func (s *Service) RecordDisabledAcceptance(ctx context.Context, id ID, obs AcceptanceObservation) (Intent, error) {
	in, err := s.Get(ctx, id)
	if err != nil {
		return Intent{}, err
	}
	if in.Origin != OriginAutomatic || in.RunID == "" {
		return Intent{}, newError(CodeInvalidTransition,
			"intent %q is not an automatic intent with an attached RunID", id)
	}
	if obs == AcceptanceAccepted {
		return in, nil
	}
	return s.store.UpdateBlocker(ctx, id, BlockerPolicyDisable, BlockerBlocked)
}

// SetBlocker sets owner's blocker on the intent to BLOCKED or UNKNOWN.
func (s *Service) SetBlocker(ctx context.Context, id ID, owner BlockerOwner, state BlockerState) (Intent, error) {
	if state != BlockerBlocked && state != BlockerUnknown {
		return Intent{}, newError(CodeInvalidTransition, "blocker state %q is not BLOCKED or UNKNOWN", state)
	}
	return s.updateBlocker(ctx, id, owner, state)
}

// ReleaseBlocker clears owner's blocker on the intent and no other owner's.
func (s *Service) ReleaseBlocker(ctx context.Context, id ID, owner BlockerOwner) (Intent, error) {
	return s.updateBlocker(ctx, id, owner, BlockerClear)
}

func (s *Service) updateBlocker(ctx context.Context, id ID, owner BlockerOwner, state BlockerState) (Intent, error) {
	if id == "" {
		return Intent{}, missing("intent ID")
	}
	if (&Blockers{}).slot(owner) == nil {
		return Intent{}, newError(CodeInvalidTransition, "unknown blocker owner %q", owner)
	}
	return s.store.UpdateBlocker(ctx, id, owner, state)
}

// MaterializationEligible reports whether the intent's blockers are all
// clear. It is a necessary, not a sufficient, condition: the materialization
// rechecks (authorization, lifecycle, bindings, source integrity) remain the
// caller's and are fail-closed.
func (s *Service) MaterializationEligible(ctx context.Context, id ID) (bool, Intent, error) {
	in, err := s.Get(ctx, id)
	if err != nil {
		return false, Intent{}, err
	}
	return in.Blockers.Clear(), in, nil
}
