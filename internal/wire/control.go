package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const PreNegotiationControlBytes = 16 << 10

var (
	ErrInvalidControl = errors.New("sgsp wire: invalid control JSON")
	ErrDuplicateKey   = errors.New("sgsp wire: duplicate JSON object key")
	ErrNesting        = errors.New("sgsp wire: JSON nesting exceeds limit")
)

// Control keeps an operation and its validated JSON fields without losing a
// future-version extension. Schema-specific validation belongs to the caller.
type Control struct {
	Op     string
	Fields map[string]json.RawMessage
}

func EncodeControl(raw []byte, maxBytes int) ([]byte, error) {
	if maxBytes <= 0 || maxBytes > PreNegotiationControlBytes {
		maxBytes = PreNegotiationControlBytes
	}
	if len(raw) > maxBytes {
		return nil, ErrTooLarge
	}
	if _, err := DecodeControlBody(raw, maxBytes); err != nil {
		return nil, err
	}
	b, err := AppendVarint(nil, uint64(len(raw)))
	if err != nil {
		return nil, err
	}
	return append(b, raw...), nil
}
func DecodeControl(src []byte, maxBytes int) (Control, int, error) {
	length, n, err := DecodeVarint(src)
	if err != nil {
		return Control{}, 0, err
	}
	if maxBytes <= 0 || maxBytes > PreNegotiationControlBytes {
		maxBytes = PreNegotiationControlBytes
	}
	if length > uint64(maxBytes) {
		return Control{}, 0, ErrTooLarge
	}
	if length > uint64(len(src)-n) {
		return Control{}, 0, ErrTruncated
	}
	control, err := DecodeControlBody(src[n:n+int(length)], maxBytes)
	return control, n + int(length), err
}
func DecodeControlBody(raw []byte, maxBytes int) (Control, error) {
	if len(raw) == 0 || len(raw) > maxBytes || !utf8.Valid(raw) {
		return Control{}, ErrInvalidControl
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := validateValue(decoder, 0, true); err != nil {
		return Control{}, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return Control{}, ErrInvalidControl
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Control{}, ErrInvalidControl
	}
	op, ok := fields["op"]
	if !ok || !jsonString(op, 128, false) {
		return Control{}, ErrInvalidControl
	}
	var operation string
	_ = json.Unmarshal(op, &operation)
	control := Control{Op: operation, Fields: fields}
	if err := ValidateControl(control); err != nil {
		return Control{}, err
	}
	return control, nil
}
func validateValue(decoder *json.Decoder, depth int, root bool) error {
	if depth > 8 {
		return ErrNesting
	}
	token, err := decoder.Token()
	if err != nil {
		return ErrInvalidControl
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			if root == false && depth == 8 {
				return ErrNesting
			}
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return ErrInvalidControl
				}
				key, ok := keyToken.(string)
				if !ok {
					return ErrInvalidControl
				}
				if _, duplicate := seen[key]; duplicate {
					return ErrDuplicateKey
				}
				seen[key] = struct{}{}
				if err := validateValue(decoder, depth+1, false); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return ErrInvalidControl
			}
			return nil
		case '[':
			if root {
				return ErrInvalidControl
			}
			for decoder.More() {
				if err := validateValue(decoder, depth+1, false); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return ErrInvalidControl
			}
			return nil
		}
	}
	if root {
		return ErrInvalidControl
	}
	return nil
}
func jsonString(raw json.RawMessage, max int, empty bool) bool {
	var value string
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &value) != nil || (!empty && value == "") || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	return true
}
func RequireCapabilities(control Control, supported map[string]bool) error {
	raw, ok := control.Fields["required"]
	if !ok {
		return ErrInvalidControl
	}
	var capabilities []string
	if err := json.Unmarshal(raw, &capabilities); err != nil {
		return ErrInvalidControl
	}
	seen := map[string]struct{}{}
	for _, capability := range capabilities {
		if capability == "" {
			return ErrInvalidControl
		}
		if _, duplicate := seen[capability]; duplicate {
			return fmt.Errorf("%w: duplicate capability", ErrInvalidControl)
		}
		seen[capability] = struct{}{}
		if !supported[capability] {
			return fmt.Errorf("%w: %s", ErrInvalidKind, capability)
		}
	}
	return nil
}

// ValidateControl performs the syntax-only portion of section 4.4. It does
// not validate negotiated values or authentication; those require endpoint
// state and are deliberately owned by later milestones.
func ValidateControl(control Control) error {
	switch control.Op {
	case "hello":
		return validateHello(control.Fields)
	case "welcome":
		return validateWelcome(control.Fields)
	case "reject":
		return validateCodeMessage(control.Fields, false)
	case "refresh":
		return validateCredential(control.Fields)
	case "refresh_result":
		if err := validateCodeMessage(control.Fields, true); err != nil {
			return err
		}
		if raw, ok := control.Fields["expires_unix_ms"]; ok && !positiveInteger(raw) {
			return ErrInvalidControl
		}
		return nil
	case "close":
		return validateCodeMessage(control.Fields, false)
	case "close_ack":
		return nil
	default:
		return ErrInvalidControl
	}
}

func validateHello(fields map[string]json.RawMessage) error {
	if !oneOfString(fields["role"], "game", "bootstrap") || !validText(fields["app"], 128, false) || !validText(fields["app_version"], 128, false) {
		return ErrInvalidControl
	}
	if err := validateStringList(fields["required"], 128); err != nil {
		return err
	}
	if err := validateLimits(fields["limits"]); err != nil {
		return err
	}
	if err := validateCredential(fields); err != nil {
		return err
	}
	if raw, ok := fields["group"]; ok && !validText(raw, 256, false) {
		return ErrInvalidControl
	}
	if raw, ok := fields["admission"]; ok && !validText(raw, 4096, false) {
		return ErrInvalidControl
	}
	if raw, ok := fields["resume"]; ok {
		if err := validateResume(raw); err != nil {
			return err
		}
	}
	return nil
}
func validateWelcome(fields map[string]json.RawMessage) error {
	if !validID(fields["session_id"]) || !positiveDecimalString(fields["epoch"]) || !validOwner(fields["owner"]) {
		return ErrInvalidControl
	}
	principal, ok := object(fields["principal"])
	if !ok || !validText(principal["issuer"], 128, false) || !validText(principal["subject"], 128, false) || !positiveInteger(principal["expires_unix_ms"]) {
		return ErrInvalidControl
	}
	if err := validateLimits(fields["limits"]); err != nil {
		return err
	}
	if err := validateStringList(fields["capabilities"], 128); err != nil || !boolean(fields["resumed"]) || !boolean(fields["resumable"]) || !nonnegativeInteger(fields["resume_grace_ms"]) {
		return ErrInvalidControl
	}
	if raw, ok := fields["resume_secret"]; ok && !validBase64Exact(raw, 32) {
		return ErrInvalidControl
	}
	return nil
}
func validateCodeMessage(fields map[string]json.RawMessage, allowExpiry bool) error {
	if !nonnegativeInteger(fields["code"]) || !validText(fields["message"], maxDiagnosticBytes, true) {
		return ErrInvalidControl
	}
	return nil
}
func validateCredential(fields map[string]json.RawMessage) error {
	credential, ok := object(fields["credential"])
	if !ok || !validText(credential["scheme"], 128, false) || !validBase64AtMost(credential["data"], 4096) {
		return ErrInvalidControl
	}
	return nil
}
func validateLimits(raw json.RawMessage) error {
	limits, ok := object(raw)
	if !ok {
		return ErrInvalidControl
	}
	for _, key := range []string{"control_bytes", "message_bytes", "datagram_bytes", "reliable_channels", "datagram_channels", "requests", "streams"} {
		if !positiveInteger(limits[key]) {
			return ErrInvalidControl
		}
	}
	return nil
}
func validateResume(raw json.RawMessage) error {
	resume, ok := object(raw)
	if !ok || !validID(resume["session_id"]) || !validText(resume["owner_id"], 128, false) || !validID(resume["incarnation"]) || !validBase64Exact(resume["secret"], 32) {
		return ErrInvalidControl
	}
	return nil
}
func validOwner(raw json.RawMessage) bool {
	owner, ok := object(raw)
	return ok && validText(owner["id"], 128, false) && validID(owner["incarnation"]) && validText(owner["address"], 128, false) && validText(owner["server_name"], 128, false)
}
func object(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	var out map[string]json.RawMessage
	return out, json.Unmarshal(raw, &out) == nil && out != nil
}
func validText(raw json.RawMessage, max int, empty bool) bool { return jsonString(raw, max, empty) }
func boolean(raw json.RawMessage) bool {
	return bytes.Equal(raw, []byte("true")) || bytes.Equal(raw, []byte("false"))
}
func positiveInteger(raw json.RawMessage) bool {
	var value uint64
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null")) && json.Unmarshal(raw, &value) == nil && value > 0
}
func nonnegativeInteger(raw json.RawMessage) bool {
	var value uint64
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null")) && json.Unmarshal(raw, &value) == nil
}
func oneOfString(raw json.RawMessage, values ...string) bool {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
func positiveDecimalString(raw json.RawMessage) bool {
	var value string
	return json.Unmarshal(raw, &value) == nil && value != "" && value[0] != '0' && strings.Trim(value, "0123456789") == ""
}
func validID(raw json.RawMessage) bool {
	var value string
	if json.Unmarshal(raw, &value) != nil || len(value) != 32 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func validBase64Exact(raw json.RawMessage, bytes int) bool {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == bytes
}
func validBase64AtMost(raw json.RawMessage, max int) bool {
	var value string
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &value) != nil {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) <= max
}
func validateStringList(raw json.RawMessage, max int) error {
	var values []string
	if len(raw) == 0 || raw[0] != '[' || json.Unmarshal(raw, &values) != nil {
		return ErrInvalidControl
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		if value == "" || len(value) > max || !utf8.ValidString(value) {
			return ErrInvalidControl
		}
		if _, duplicate := seen[value]; duplicate {
			return ErrInvalidControl
		}
		seen[value] = struct{}{}
	}
	return nil
}
