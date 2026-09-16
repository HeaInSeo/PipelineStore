package pipelinestore

import "context"

// Cardinality is a port/asset cardinality. In PIPE-I1 only SINGLE is an active
// capability; anything else is out of profile and rejected.
type Cardinality string

const (
	// CardinalityUnspecified is the zero value; treated as UNKNOWN/UNSPECIFIED.
	CardinalityUnspecified Cardinality = ""
	CardinalitySingle      Cardinality = "SINGLE"
	CardinalityMultiple    Cardinality = "MULTIPLE"
	CardinalityComposite   Cardinality = "COMPOSITE"
	CardinalityScatter     Cardinality = "SCATTER"
)

// InputPortDecl is a declared input port of a runnable ToolFunction.
type InputPortDecl struct {
	// DataFormat is the Q16 data-format identifier. Empty, "UNKNOWN", or
	// "UNSPECIFIED" are sentinels meaning the format is not known.
	DataFormat  string
	Cardinality Cardinality
	Required    bool
}

// OutputPortDecl is a declared output port of a runnable ToolFunction.
type OutputPortDecl struct {
	DataFormat  string
	Cardinality Cardinality
}

// ParameterDecl declares a fixed parameter accepted by a runnable ToolFunction.
type ParameterDecl struct {
	// AllowedValues, when non-empty, restricts the value to an exact member of the
	// set (no coercion). When empty, any string value is accepted verbatim.
	AllowedValues []string
}

// ToolFunctionDecl is the read-only declaration of an exact runnable
// ToolFunction, as returned by NodeVault for a given cas_hash.
type ToolFunctionDecl struct {
	Inputs     map[string]InputPortDecl
	Outputs    map[string]OutputPortDecl
	Parameters map[string]ParameterDecl
}

// AssetMemberDecl is the read-only declaration of an exact Sori asset member.
type AssetMemberDecl struct {
	DataFormat  string
	Cardinality Cardinality
}

// ToolFunctionResolver is a NARROW, read-only lookup of an exact runnable
// ToolFunction by its content-addressed hash. It performs no build, no mutation,
// and no alias/latest resolution.
type ToolFunctionResolver interface {
	ResolveToolFunction(ctx context.Context, casHash string) (ToolFunctionDecl, error)
}

// SoriResolver is a NARROW, read-only verification of an exact Sori asset
// revision/member. It performs no mutation.
type SoriResolver interface {
	VerifyAssetMember(ctx context.Context, assetID, assetRevisionID, memberKey string) (AssetMemberDecl, error)
}

// Resolvers bundles the read-only resolvers required for validation. There is no
// active ToolProfile pin resolver in v1: a present tool_profile_digest is
// rejected before persistence.
type Resolvers struct {
	ToolFunction ToolFunctionResolver
	Sori         SoriResolver
}

// isUnknownFormat reports whether a Q16 data format is an unknown/unspecified
// sentinel.
func isUnknownFormat(f string) bool {
	return f == "" || f == "UNKNOWN" || f == "UNSPECIFIED"
}
