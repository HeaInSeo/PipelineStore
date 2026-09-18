package pipelinestore

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"
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
	// Reject invalid UTF-8 BEFORE any decoding/tokenization: distinct malformed
	// byte sequences must not collapse into the Unicode replacement character
	// during decoding (fail closed).
	if !utf8.Valid(data) {
		return nil, newErr(CodeInvalidContract, "contract bytes are not valid UTF-8")
	}
	// Reject JSON *escaped* lone surrogates (e.g. \ud800 / \udc00). utf8.Valid
	// above only inspects the raw bytes and cannot see a surrogate expressed as an
	// ASCII \u escape; the strict JSON decoder would silently coerce such a lone
	// surrogate to U+FFFD, collapsing distinct malformed values. Fail closed here
	// before any decode. Valid high+low surrogate pairs are accepted.
	if err := rejectUnpairedSurrogateEscapes(data); err != nil {
		return nil, err
	}
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

// rejectUnpairedSurrogateEscapes scans the raw body for JSON \uXXXX escapes and
// rejects any lone surrogate code unit: a high surrogate (\uD800–\uDBFF) not
// immediately followed by a low-surrogate escape (\uDC00–\uDFFF), or a low
// surrogate not immediately preceded by a high. A valid high+low pair (e.g.
// "𐀀" == U+10000) is accepted. It performs no normalization or
// coercion; a malformed \u escape is left for the strict JSON decoder to report.
func rejectUnpairedSurrogateEscapes(data []byte) error {
	i := 0
	for i < len(data) {
		if data[i] != '\\' {
			i++
			continue
		}
		// A run of consecutive backslashes: an even-length run is fully escaped
		// backslashes (no escape introducer follows); an odd-length run introduces
		// an escape with the character immediately after the run.
		j := i
		for j < len(data) && data[j] == '\\' {
			j++
		}
		if (j-i)%2 == 0 {
			i = j
			continue
		}
		// data[j] is the escape-type character following the escaping backslash.
		if j >= len(data) || data[j] != 'u' {
			i = j
			continue
		}
		cu, ok := parseHex4(data, j+1)
		if !ok {
			// Malformed \u escape; defer to the strict JSON decoder for the error.
			i = j + 1
			continue
		}
		switch {
		case cu >= 0xD800 && cu <= 0xDBFF:
			// High surrogate: the very next token must be a low-surrogate escape.
			at := j + 5
			if at+1 < len(data) && data[at] == '\\' && data[at+1] == 'u' {
				if lo, ok := parseHex4(data, at+2); ok && lo >= 0xDC00 && lo <= 0xDFFF {
					i = at + 6 // consume the whole \uXXXX\uYYYY pair
					continue
				}
			}
			return newErr(CodeInvalidContract, "JSON contains an unpaired high surrogate escape \\u%04x", cu)
		case cu >= 0xDC00 && cu <= 0xDFFF:
			// Low surrogate not consumed by a preceding high surrogate above.
			return newErr(CodeInvalidContract, "JSON contains an unpaired low surrogate escape \\u%04x", cu)
		default:
			i = j + 5
		}
	}
	return nil
}

// parseHex4 decodes exactly four hex digits starting at data[at] into a code
// unit. It reports ok=false on truncation or a non-hex digit.
func parseHex4(data []byte, at int) (int, bool) {
	if at+4 > len(data) {
		return 0, false
	}
	v := 0
	for k := 0; k < 4; k++ {
		c := data[at+k]
		var d int
		switch {
		case c >= '0' && c <= '9':
			d = int(c - '0')
		case c >= 'a' && c <= 'f':
			d = int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			d = int(c-'A') + 10
		default:
			return 0, false
		}
		v = v*16 + d
	}
	return v, true
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
