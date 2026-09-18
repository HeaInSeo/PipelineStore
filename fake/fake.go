// Package fake provides deterministic, in-memory implementations of the
// PipelineStore read-only resolver interfaces for tests. It performs NO network
// I/O and no production transport of any kind.
package fake

import (
	"context"
	"fmt"

	ps "github.com/HeaInSeo/PipelineStore"
)

// ToolFunctionCatalog is a deterministic ToolFunctionResolver backed by a map
// keyed on exact cas_hash.
type ToolFunctionCatalog struct {
	Funcs map[string]ps.ToolFunctionDecl
}

// NewToolFunctionCatalog returns an empty catalog.
func NewToolFunctionCatalog() *ToolFunctionCatalog {
	return &ToolFunctionCatalog{Funcs: map[string]ps.ToolFunctionDecl{}}
}

// Add registers a declaration under an exact cas_hash and returns the catalog.
func (c *ToolFunctionCatalog) Add(casHash string, decl ps.ToolFunctionDecl) *ToolFunctionCatalog {
	c.Funcs[casHash] = decl
	return c
}

// ResolveToolFunction implements ps.ToolFunctionResolver with exact-match lookup.
func (c *ToolFunctionCatalog) ResolveToolFunction(_ context.Context, casHash string) (ps.ToolFunctionDecl, error) {
	decl, ok := c.Funcs[casHash]
	if !ok {
		return ps.ToolFunctionDecl{}, fmt.Errorf("no runnable ToolFunction for cas_hash %q", casHash)
	}
	return decl, nil
}

// SoriCatalog is a deterministic SoriResolver backed by a map keyed on the exact
// (asset_id, asset_revision_id, member_key) triple.
type SoriCatalog struct {
	Members map[string]ps.AssetMemberDecl
}

// NewSoriCatalog returns an empty catalog.
func NewSoriCatalog() *SoriCatalog {
	return &SoriCatalog{Members: map[string]ps.AssetMemberDecl{}}
}

func soriKey(assetID, revID, memberKey string) string {
	return assetID + "\x00" + revID + "\x00" + memberKey
}

// Add registers a member declaration under the exact triple and returns the catalog.
func (c *SoriCatalog) Add(assetID, revID, memberKey string, decl ps.AssetMemberDecl) *SoriCatalog {
	c.Members[soriKey(assetID, revID, memberKey)] = decl
	return c
}

// VerifyAssetMember implements ps.SoriResolver with exact-match verification.
func (c *SoriCatalog) VerifyAssetMember(_ context.Context, assetID, revID, memberKey string) (ps.AssetMemberDecl, error) {
	decl, ok := c.Members[soriKey(assetID, revID, memberKey)]
	if !ok {
		return ps.AssetMemberDecl{}, fmt.Errorf("no Sori asset member %q/%q/%q", assetID, revID, memberKey)
	}
	return decl, nil
}
