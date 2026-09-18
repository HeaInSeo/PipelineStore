package pipelinestore

import (
	"crypto/sha256"
	"encoding/hex"
)

// digestDomainTag is the domain-separation prefix for the pipeline-contract
// digest: the v1 contract identifier followed by a NUL byte.
const digestDomainTag = ContractVersionV1 + "\x00"

// DigestCanonical computes the PipelineContractDigest over an already-canonical
// body:
//
//	SHA256("pipelinestore.pipeline-contract.v1\0" || canonical_json_v1(contract))
//
// It returns the lowercase hex-encoded SHA-256. Only the v1 canonicalization
// version is supported; callers must ensure canon is canonical_json_v1 output.
func DigestCanonical(canonical []byte) string {
	h := sha256.New()
	h.Write([]byte(digestDomainTag))
	h.Write(canonical)
	return hex.EncodeToString(h.Sum(nil))
}

// Digest canonicalizes c and returns its PipelineContractDigest.
func Digest(c *PipelineContract) string {
	return DigestCanonical(Canonicalize(c))
}
