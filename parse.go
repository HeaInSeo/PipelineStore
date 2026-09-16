package pipelinestore

import (
	"bytes"
	"encoding/json"
	"strings"
)

// Parse strictly decodes a v1 pipeline-contract JSON body.
//
// Strictness rules enforced here (before any validation or persistence):
//   - explicit JSON null anywhere in the body is REJECTED (absence != null);
//   - duplicate object keys are REJECTED (no silent last-wins);
//   - unknown semantic-body keys are REJECTED (this is how physical path/mount/
//     PVC/locator fields are kept out of the v1 schema);
//   - trailing data after the JSON value is REJECTED.
//
// No Unicode normalization, case-folding, or trimming is performed: byte-distinct
// strings (e.g. NFC vs NFD) remain distinct.
func Parse(data []byte) (*PipelineContract, error) {
	if err := checkStrictJSON(data); err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c PipelineContract
	if err := dec.Decode(&c); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return nil, newErr(CodeUnknownKey, "%v", err)
		}
		return nil, newErr(CodeInvalidContract, "decode failed: %v", err)
	}
	if dec.More() {
		return nil, newErr(CodeInvalidContract, "trailing data after JSON contract")
	}
	return &c, nil
}

// checkStrictJSON walks the raw token stream to reject explicit nulls and
// duplicate object keys at any depth. DisallowUnknownFields (in Parse) does not
// catch either of these.
func checkStrictJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := walkStrict(dec); err != nil {
		return err
	}
	// Reject trailing tokens after the top-level value.
	if _, err := dec.Token(); err == nil {
		return newErr(CodeInvalidContract, "trailing data after JSON contract")
	}
	return nil
}

func walkStrict(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return newErr(CodeInvalidContract, "malformed JSON: %v", err)
	}
	switch t := tok.(type) {
	case nil:
		// JSON null.
		return newErr(CodeSemanticNull, "explicit JSON null is not permitted in the semantic body")
	case json.Delim:
		switch t {
		case '{':
			seen := map[string]struct{}{}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return newErr(CodeInvalidContract, "malformed JSON object key: %v", err)
				}
				key, ok := keyTok.(string)
				if !ok {
					return newErr(CodeInvalidContract, "non-string JSON object key")
				}
				if _, dup := seen[key]; dup {
					return newErrDetail(CodeDuplicate, "object key "+key, "duplicate object key")
				}
				seen[key] = struct{}{}
				if err := walkStrict(dec); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // consume '}'
				return newErr(CodeInvalidContract, "malformed JSON: %v", err)
			}
		case '[':
			for dec.More() {
				if err := walkStrict(dec); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // consume ']'
				return newErr(CodeInvalidContract, "malformed JSON: %v", err)
			}
		}
	}
	return nil
}
