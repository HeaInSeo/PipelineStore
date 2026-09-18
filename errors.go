package pipelinestore

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable error class for PipelineStore rejections.
type Code string

const (
	// CodeInvalidContract is a generic structural/decoding failure.
	CodeInvalidContract Code = "INVALID_CONTRACT"
	// CodeUnknownKey is an unknown semantic-body key (rejected, never ignored).
	CodeUnknownKey Code = "UNKNOWN_KEY"
	// CodeSemanticNull is an explicit JSON null in the semantic body (absence != null).
	CodeSemanticNull Code = "SEMANTIC_NULL"
	// CodeDuplicate is a rejected duplicate (node id, edge, parameter, binding, object key).
	CodeDuplicate Code = "DUPLICATE"
	// CodeUnsupportedVersion is an unsupported schema/canonicalization/semantic-derivation version.
	CodeUnsupportedVersion Code = "UNSUPPORTED_VERSION"
	// CodeUnsupportedCapability is a known-but-inactive reserved capability (e.g. tool_profile_digest).
	CodeUnsupportedCapability Code = "UNSUPPORTED_CAPABILITY"
	// CodeToolFunctionUnresolved is a missing or unresolvable exact ToolFunction pin.
	CodeToolFunctionUnresolved Code = "TOOL_FUNCTION_UNRESOLVED"
	// CodeParameterInvalid is a fixed-parameter name not declared or value invalid under the pin.
	CodeParameterInvalid Code = "PARAMETER_INVALID"
	// CodeEdgeInvalid is a dangling/unknown edge, binding, or slot target node/port.
	CodeEdgeInvalid Code = "EDGE_INVALID"
	// CodeGraphCycle is a dependency cycle in direct_edges.
	CodeGraphCycle Code = "GRAPH_CYCLE"
	// CodeQ16Mismatch is a DataFormat/Cardinality (Q16) L0 mismatch, required UNKNOWN/UNSPECIFIED,
	// or an implicit adapter/transform requirement.
	CodeQ16Mismatch Code = "Q16_MISMATCH"
	// CodeCardinalityUnsupported is a MULTIPLE/composite/scatter/fan-out cardinality (out of v1 profile).
	CodeCardinalityUnsupported Code = "CARDINALITY_UNSUPPORTED"
	// CodeProviderMissing is a required input with zero providers.
	CodeProviderMissing Code = "PROVIDER_MISSING"
	// CodeProviderConflict is a SINGLE target input with more than one provider.
	CodeProviderConflict Code = "PROVIDER_CONFLICT"
	// CodeAssetUnresolved is an unverifiable reusable asset exact revision/member.
	CodeAssetUnresolved Code = "ASSET_UNRESOLVED"
	// CodeSlotInvalid is a duplicate/non-unique slot_id or a slot reused for multiple targets.
	CodeSlotInvalid Code = "SLOT_INVALID"
	// CodeOperationConflict is a same operation_id replay with a different immutable body.
	CodeOperationConflict Code = "OPERATION_CONFLICT"
	// CodeIntegrity is a stored-body/stored-digest integrity failure on exact read (fail closed).
	CodeIntegrity Code = "INTEGRITY_ERROR"
	// CodeNotFound is an exact read miss for (PipelineID, PipelineRevisionID).
	CodeNotFound Code = "NOT_FOUND"
)

// Error is a PipelineStore domain error carrying a stable Code and optional Detail.
type Error struct {
	Code   Code
	Msg    string
	Detail string
}

func (e *Error) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Msg, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}

func newErr(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func newErrDetail(code Code, detail, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...), Detail: detail}
}

// CodeOf returns the Code of err if it is (or wraps) a *Error, else "".
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
