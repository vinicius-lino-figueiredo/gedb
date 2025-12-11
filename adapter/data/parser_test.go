package data

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func stripWhitespace(s string) string {
	return strings.Map(func(r rune) rune {
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' {
			return -1
		}
		return r
	}, s)
}

var allValueIndent = `{
	"Bool": true,
	"Int": 2,
	"Int8": 3,
	"Int16": 4,
	"Int32": 5,
	"Int64": 6,
	"Uint": 7,
	"Uint8": 8,
	"Uint16": 9,
	"Uint32": 10,
	"Uint64": 11,
	"Uintptr": 12,
	"Float32": 14.1,
	"Float64": 15.1,
	"bar": "foo",
	"bar2": "foo2",
	"IntStr": "42",
	"UintptrStr": "44",
	"PBool": null,
	"PInt": null,
	"PInt8": null,
	"PInt16": null,
	"PInt32": null,
	"PInt64": null,
	"PUint": null,
	"PUint8": null,
	"PUint16": null,
	"PUint32": null,
	"PUint64": null,
	"PUintptr": null,
	"PFloat32": null,
	"PFloat64": null,
	"String": "16",
	"PString": null,
	"Map": {
		"17": {
			"Tag": "tag17"
		},
		"18": {
			"Tag": "tag18"
		}
	},
	"MapP": {
		"19": {
			"Tag": "tag19"
		},
		"20": null
	},
	"PMap": null,
	"PMapP": null,
	"EmptyMap": {},
	"NilMap": null,
	"Slice": [
		{
			"Tag": "tag20"
		},
		{
			"Tag": "tag21"
		}
	],
	"SliceP": [
		{
			"Tag": "tag22"
		},
		null,
		{
			"Tag": "tag23"
		}
	],
	"PSlice": null,
	"PSliceP": null,
	"EmptySlice": [],
	"NilSlice": null,
	"StringSlice": [
		"str24",
		"str25",
		"str26"
	],
	"ByteSlice": "Gxwd",
	"Small": {
		"Tag": "tag30"
	},
	"PSmall": {
		"Tag": "tag31"
	},
	"PPSmall": null,
	"Interface": 5.2,
	"PInterface": null
}`

var allValueIndentMap = M{
	"Bool":       true,
	"Int":        2.0,
	"Int8":       3.0,
	"Int16":      4.0,
	"Int32":      5.0,
	"Int64":      6.0,
	"Uint":       7.0,
	"Uint8":      8.0,
	"Uint16":     9.0,
	"Uint32":     10.0,
	"Uint64":     11.0,
	"Uintptr":    12.0,
	"Float32":    14.1,
	"Float64":    15.1,
	"bar":        "foo",
	"bar2":       "foo2",
	"IntStr":     "42",
	"UintptrStr": "44",
	"PBool":      nil,
	"PInt":       nil,
	"PInt8":      nil,
	"PInt16":     nil,
	"PInt32":     nil,
	"PInt64":     nil,
	"PUint":      nil,
	"PUint8":     nil,
	"PUint16":    nil,
	"PUint32":    nil,
	"PUint64":    nil,
	"PUintptr":   nil,
	"PFloat32":   nil,
	"PFloat64":   nil,
	"String":     "16",
	"PString":    nil,
	"Map": M{
		"17": M{
			"Tag": "tag17",
		},
		"18": M{
			"Tag": "tag18",
		},
	},
	"MapP": M{
		"19": M{
			"Tag": "tag19",
		},
		"20": nil,
	},
	"PMap":     nil,
	"PMapP":    nil,
	"EmptyMap": M{},
	"NilMap":   nil,
	"Slice": []any{
		M{
			"Tag": "tag20",
		},
		M{
			"Tag": "tag21",
		},
	},
	"SliceP": []any{
		M{
			"Tag": "tag22",
		},
		nil,
		M{
			"Tag": "tag23",
		},
	},
	"PSlice":     nil,
	"PSliceP":    nil,
	"EmptySlice": []any{},
	"NilSlice":   nil,
	"StringSlice": []any{
		"str24",
		"str25",
		"str26",
	},
	"ByteSlice": "Gxwd",
	"Small": M{
		"Tag": "tag30",
	},
	"PSmall": M{
		"Tag": "tag31",
	},
	"PPSmall":    nil,
	"Interface":  5.2,
	"PInterface": nil,
}

var pallValueIndent = `{
	"Bool": false,
	"Int": 0,
	"Int8": 0,
	"Int16": 0,
	"Int32": 0,
	"Int64": 0,
	"Uint": 0,
	"Uint8": 0,
	"Uint16": 0,
	"Uint32": 0,
	"Uint64": 0,
	"Uintptr": 0,
	"Float32": 0,
	"Float64": 0,
	"bar": "",
	"bar2": "",
        "IntStr": "0",
	"UintptrStr": "0",
	"PBool": true,
	"PInt": 2,
	"PInt8": 3,
	"PInt16": 4,
	"PInt32": 5,
	"PInt64": 6,
	"PUint": 7,
	"PUint8": 8,
	"PUint16": 9,
	"PUint32": 10,
	"PUint64": 11,
	"PUintptr": 12,
	"PFloat32": 14.1,
	"PFloat64": 15.1,
	"String": "",
	"PString": "16",
	"Map": null,
	"MapP": null,
	"PMap": {
		"17": {
			"Tag": "tag17"
		},
		"18": {
			"Tag": "tag18"
		}
	},
	"PMapP": {
		"19": {
			"Tag": "tag19"
		},
		"20": null
	},
	"EmptyMap": null,
	"NilMap": null,
	"Slice": null,
	"SliceP": null,
	"PSlice": [
		{
			"Tag": "tag20"
		},
		{
			"Tag": "tag21"
		}
	],
	"PSliceP": [
		{
			"Tag": "tag22"
		},
		null,
		{
			"Tag": "tag23"
		}
	],
	"EmptySlice": null,
	"NilSlice": null,
	"StringSlice": null,
	"ByteSlice": null,
	"Small": {
		"Tag": ""
	},
	"PSmall": null,
	"PPSmall": {
		"Tag": "tag31"
	},
	"Interface": null,
	"PInterface": 5.2
}`

var pallValueIndentMap = M{
	"Bool":       false,
	"Int":        0.0,
	"Int8":       0.0,
	"Int16":      0.0,
	"Int32":      0.0,
	"Int64":      0.0,
	"Uint":       0.0,
	"Uint8":      0.0,
	"Uint16":     0.0,
	"Uint32":     0.0,
	"Uint64":     0.0,
	"Uintptr":    0.0,
	"Float32":    0.0,
	"Float64":    0.0,
	"bar":        "",
	"bar2":       "",
	"IntStr":     "0",
	"UintptrStr": "0",
	"PBool":      true,
	"PInt":       2.0,
	"PInt8":      3.0,
	"PInt16":     4.0,
	"PInt32":     5.0,
	"PInt64":     6.0,
	"PUint":      7.0,
	"PUint8":     8.0,
	"PUint16":    9.0,
	"PUint32":    10.0,
	"PUint64":    11.0,
	"PUintptr":   12.0,
	"PFloat32":   14.1,
	"PFloat64":   15.1,
	"String":     "",
	"PString":    "16",
	"Map":        nil,
	"MapP":       nil,
	"PMap": M{
		"17": M{
			"Tag": "tag17",
		},
		"18": M{
			"Tag": "tag18",
		},
	},
	"PMapP": M{
		"19": M{
			"Tag": "tag19",
		},
		"20": nil,
	},
	"EmptyMap": nil,
	"NilMap":   nil,
	"Slice":    nil,
	"SliceP":   nil,
	"PSlice": []any{
		M{
			"Tag": "tag20",
		},
		M{
			"Tag": "tag21",
		},
	},
	"PSliceP": []any{
		M{
			"Tag": "tag22",
		},
		nil,
		M{
			"Tag": "tag23",
		},
	},
	"EmptySlice":  nil,
	"NilSlice":    nil,
	"StringSlice": nil,
	"ByteSlice":   nil,
	"Small": M{
		"Tag": "",
	},
	"PSmall": nil,
	"PPSmall": M{
		"Tag": "tag31",
	},
	"Interface":  nil,
	"PInterface": 5.2,
}

var pallValueCompact = stripWhitespace(pallValueIndent)

var allValueCompact = stripWhitespace(allValueIndent)

type testCase struct {
	CaseName string
	in       string
	out      any
	errIs    error
	errAs    error
}

var validCases = []testCase{
	// Basic primitive values
	{CaseName: "bool_true", in: `true`, out: true},
	{CaseName: "int_1", in: `1`, out: 1.0},
	{CaseName: "float_1_2", in: `1.2`, out: 1.2},
	{CaseName: "negative_int", in: `-5`, out: -5.0},
	{CaseName: "null_value", in: "null", out: nil},

	// String values (including Unicode)
	{CaseName: "unicode_string", in: `"a\u1234"`, out: "a\u1234"},
	{CaseName: "escaped_url", in: `"http:\/\/"`, out: "http://"},
	{CaseName: "unicode_clef", in: `"g-clef: \uD834\uDD1E"`, out: "g-clef: \U0001D11E"},
	{CaseName: "invalid_unicode", in: `"invalid: \uD834x\uDD1E"`, out: "invalid: �x�"},
	{CaseName: "colon_string", in: `"x:y"`, out: "x:y"},
	{CaseName: "base64_string", in: `"AQID"`, out: "AQID"},
	{CaseName: "numeric_string", in: `"5"`, out: "5"},
	{CaseName: "invalid_string", in: `"invalid"`, out: "invalid"},
	{CaseName: "escape_chars", in: `"\f\t\r\n\b\/\\"`, out: "\f\t\r\n\b/\\"},

	// String values with whitespace
	{CaseName: "bool_with_whitespace", in: "\n true ", out: true},

	// Unicode edge cases
	{CaseName: "unicode_xff", in: "\"hello\xffworld\"", out: "hello�world"},
	{CaseName: "unicode_c2c2", in: "\"hello\xc2\xc2world\"", out: "hello��world"},
	{CaseName: "unicode_c2ff", in: "\"hello\xc2\xffworld\"", out: "hello��world"},
	{CaseName: "unicode_ud800", in: "\"hello\\ud800world\"", out: "hello�world"},
	{CaseName: "unicode_double_ud800", in: "\"hello\\ud800\\ud800world\"", out: "hello��world"},
	{CaseName: "unicode_utf8_surrogate", in: "\"hello\xed\xa0\x80\xed\xb0\x80world\"", out: "hello������world"},

	// Simple objects
	{CaseName: "object_single_field", in: `{"X": 23}`, out: M{"X": 23.0}},
	{CaseName: "object_lowercase_key", in: `{"x": 1}`, out: M{"x": 1.0}},
	{CaseName: "object_s_field", in: `{"S": 23}`, out: M{"S": 23.0}},
	{CaseName: "object_two_fields", in: `{"Y": 1, "Z": 2}`, out: M{"Y": 1.0, "Z": 2.0}},
	{CaseName: "object_hello_field", in: `{"hello": 1}`, out: M{"hello": 1.0}},
	{CaseName: "object_compact_format", in: `{"X": 1,"Y":2}`, out: M{"X": 1.0, "Y": 2.0}},
	{CaseName: "object_string_value", in: `{"foo": "bar"}`, out: M{"foo": "bar"}},
	{CaseName: "object_invalid_string", in: `{"A":"invalid"}`, out: M{"A": "invalid"}},

	// Nested objects
	{CaseName: "nested_object_simple", in: `{"T": {"X": 23}}`, out: M{"T": M{"X": 23.0}}},
	{CaseName: "nested_object_string", in: `{"M":{"T":"x:y"}}`, out: M{"M": M{"T": "x:y"}}},
	{CaseName: "object_colon_value", in: `{"M":"x:y"}`, out: M{"M": "x:y"}},
	{CaseName: "nested_object_hello", in: `{"V": {"F2": "hello"}}`, out: M{"V": M{"F2": "hello"}}},
	{CaseName: "nested_with_empty_object", in: `{"V": {"F4": {}, "F2": "hello"}}`, out: M{"V": M{"F4": M{}, "F2": "hello"}}},
	{CaseName: "object_level1a", in: `{"Level1a": "hello"}`, out: M{"Level1a": "hello"}},

	// Complex objects
	{CaseName: "object_three_fields", in: `{"F1":1,"F2":2,"F3":3}`, out: M{"F1": 1.0, "F2": 2.0, "F3": 3.0}},
	{CaseName: "complex_mixed_types", in: `{"k1":1,"k2":"s","k3":[1,2.0,3e-3],"k4":{"kk1":"s","kk2":2}}`, out: M{"k1": 1.0, "k2": "s", "k3": []any{1.0, 2.0, 3e-3}, "k4": M{"kk1": "s", "kk2": 2.0}}},
	{CaseName: "similar_keys_alpha", in: `{"alpha": "abc", "alphabet": "xyz"}`, out: M{"alpha": "abc", "alphabet": "xyz"}},
	{CaseName: "object_alpha_only", in: `{"alpha": "abc"}`, out: M{"alpha": "abc"}},

	// Objects with special keys
	{CaseName: "colon_key_true", in: `{"x:y":true}`, out: M{"x:y": true}},
	{CaseName: "duplicate_colon_key", in: `{"x:y":false,"x:y":true}`, out: M{"x:y": true}},
	{CaseName: "timestamp_key", in: `{"2009-11-10T23:00:00Z": "hello world"}`, out: M{"2009-11-10T23:00:00Z": "hello world"}},
	{CaseName: "asdf_key", in: `{"asdf": "hello world"}`, out: M{"asdf": "hello world"}},

	// Objects with numeric keys
	{CaseName: "numeric_keys_sequence", in: `{"-1":"a","0":"b","1":"c"}`, out: M{"-1": "a", "0": "b", "1": "c"}},
	{CaseName: "numeric_keys_unordered", in: `{"0":"a","10":"c","9":"b"}`, out: M{"0": "a", "10": "c", "9": "b"}},
	{CaseName: "int64_limit_keys", in: `{"-9223372036854775808":"min","9223372036854775807":"max"}`, out: M{"-9223372036854775808": "min", "9223372036854775807": "max"}},
	{CaseName: "uint64_max_key", in: `{"18446744073709551615":"max"}`, out: M{"18446744073709551615": "max"}},
	{CaseName: "numeric_keys_bool", in: `{"0":false,"10":true}`, out: M{"0": false, "10": true}},
	{CaseName: "alphanumeric_key_u2", in: `{"u2":4}`, out: M{"u2": 4.0}},
	{CaseName: "numeric_string_key_2", in: `{"2":4}`, out: M{"2": 4.0}},
	{CaseName: "same_key_value_abc", in: `{"abc":"abc"}`, out: M{"abc": "abc"}},
	{CaseName: "large_numeric_key_256", in: `{"256":"abc"}`, out: M{"256": "abc"}},
	{CaseName: "large_numeric_key_128", in: `{"128":"abc"}`, out: M{"128": "abc"}},
	{CaseName: "negative_numeric_key", in: `{"-1":"abc"}`, out: M{"-1": "abc"}},
	{CaseName: "n_key_numeric", in: `{"N":5}`, out: M{"N": 5.0}},
	{CaseName: "n_key_string", in: `{"N":"5"}`, out: M{"N": "5"}},

	// Objects with duplicate keys
	{CaseName: "duplicate_keys_last_wins", in: `{"I": 0, "I": null, "J": null}`, out: M{"I": nil, "J": nil}},

	// Objects with nested structures
	{CaseName: "nested_mixed_keys", in: `{"F":{"a":2,"3":4}}`, out: M{"F": M{"a": 2.0, "3": 4.0}}},
	{CaseName: "data_string_number", in: `{"data":{"test1": "bob", "test2": 123}}`, out: M{"data": M{"test1": "bob", "test2": 123.0}}},
	{CaseName: "data_number_string", in: `{"data":{"test1": 123, "test2": "bob"}}`, out: M{"data": M{"test1": 123.0, "test2": "bob"}}},
	{CaseName: "triple_nested_object", in: `{"PP": {"T": {"Y": "bad-type"}}}`, out: M{"PP": M{"T": M{"Y": "bad-type"}}}},

	// Arrays
	{CaseName: "array_numbers", in: `[1, 2, 3]`, out: []any{1.0, 2.0, 3.0}},
	{CaseName: "empty_array", in: `[]`, out: []any{}},
	{CaseName: "array_single_string", in: `["x:y"]`, out: []any{"x:y"}},
	{CaseName: "array_strings", in: `["Z01","Z02","Z03"]`, out: []any{"Z01", "Z02", "Z03"}},
	{CaseName: "array_mixed_types", in: `[1,2,true,4,5]`, out: []any{1.0, 2.0, true, 4.0, 5.0}},
	{CaseName: "array_of_objects", in: `{"Ts": [{"Y": 1}, {"Y": 2}, {"Y": "bad-type"}]}`, out: M{"Ts": []any{M{"Y": 1.0}, M{"Y": 2.0}, M{"Y": "bad-type"}}}},

	// Objects with array/null values
	{CaseName: "object_empty_array", in: `{"T":[]}`, out: M{"T": []any{}}},
	{CaseName: "object_null_value", in: `{"T":null}`, out: M{"T": nil}},

	// String to bool parsing tests
	{CaseName: "string_true", in: `{"B":"true"}`, out: M{"B": "true"}},
	{CaseName: "string_false", in: `{"B":"false"}`, out: M{"B": "false"}},
	{CaseName: "string_maybe", in: `{"B": "maybe"}`, out: M{"B": "maybe"}},
	{CaseName: "string_tru_partial", in: `{"B": "tru"}`, out: M{"B": "tru"}},
	{CaseName: "string_false_caps", in: `{"B": "False"}`, out: M{"B": "False"}},
	{CaseName: "string_null", in: `{"B": "null"}`, out: M{"B": "null"}},
	{CaseName: "string_nul_partial", in: `{"B": "nul"}`, out: M{"B": "nul"}},
	{CaseName: "b_array_numbers", in: `{"B": [2, 3]}`, out: M{"B": []any{2.0, 3.0}}},

	// Large numbers and scientific notation
	{CaseName: "number_5", in: `5`, out: 5.0},
	{CaseName: "small_decimal", in: `0.000001`, out: 0.000001},
	{CaseName: "scientific_1e_minus_7", in: `1e-7`, out: 1e-7},
	{CaseName: "very_large_integer", in: `100000000000000000000`, out: 100000000000000000000.0},
	{CaseName: "scientific_1e_plus_21", in: `1e+21`, out: 1e+21},
	{CaseName: "negative_small_decimal", in: `-0.000001`, out: -0.000001},
	{CaseName: "negative_scientific_1e_minus_7", in: `-1e-7`, out: -1e-7},
	{CaseName: "negative_very_large_integer", in: `-100000000000000000000`, out: -100000000000000000000.0},
	{CaseName: "negative_scientific_1e_plus_21", in: `-1e+21`, out: -1e+21},
	{CaseName: "large_precision_number", in: `999999999999999900000`, out: 999999999999999900000.0},
	{CaseName: "js_max_safe_integer", in: `9007199254740992`, out: 9007199254740992.0},
	{CaseName: "js_max_safe_integer_plus_1", in: `9007199254740993`, out: 9007199254740993.0},

	// Complex test data structures
	{CaseName: "all_values_indented", in: allValueIndent, out: allValueIndentMap},
	{CaseName: "all_pointer_values_indented", in: pallValueIndent, out: pallValueIndentMap},
	{CaseName: "all_values_compact", in: allValueCompact, out: allValueIndentMap},
	{CaseName: "all_pointer_values_compact", in: pallValueCompact, out: pallValueIndentMap},
}

var invalidCases = []testCase{
	// String value (invalid unicode)
	{CaseName: "invalid_unicode_hex", in: `"invalid \uzzzzzz"`, errIs: ErrInvalidUTF8Char},

	// Syntax error cases
	{CaseName: "incomplete_object_missing_value", in: `{"X": "foo", "Y"}`, errIs: ErrNoColon},
	{CaseName: "array_invalid_operator", in: `[1, 2, 3+]`, errIs: ErrInvalidNumber},
	{CaseName: "object_invalid_number", in: `{"X":12x}`, errIs: ErrNoComma},
	{CaseName: "array_missing_closing_bracket", in: `[2, 3`, errIs: io.ErrUnexpectedEOF},
	{CaseName: "object_incomplete_negative", in: `{"F3": -}`, errIs: ErrInvalidNumber},
	{CaseName: "array_wrong_closing_brace", in: `[1,2,true,4,5}`, errIs: ErrNoComma},
	{CaseName: "invalid_json_text", in: `invalid`, errIs: ErrInvalidNumber},
	{CaseName: "number_as_key", in: `{1:2}`, errIs: ErrExpectedString},
	{CaseName: "unclosed_object", in: `{"key":"value"`, errIs: io.ErrUnexpectedEOF},
	{CaseName: "unterminated_bool", in: `{"key":fals`, errIs: ErrInvalidLiteral{Value: "fals"}},
	{CaseName: "control_char_in_string", in: "\"control\x00\"", errIs: ErrInvalidControlChar{Char: '\x00'}},

	// Raw value errors (control characters)
	{CaseName: "control_char_before_number", in: "\x01 42", errIs: ErrInvalidNumber},
	{CaseName: "control_char_after_number", in: " 42 \x01", errIs: ErrTrailingData},
	{CaseName: "control_char_before_bool", in: "\x01 true", errIs: ErrInvalidNumber},
	{CaseName: "control_char_after_bool", in: " false \x01", errIs: ErrTrailingData},
	{CaseName: "control_char_before_float", in: "\x01 1.2", errIs: ErrInvalidNumber},
	{CaseName: "control_char_after_float", in: " 3.4 \x01", errIs: ErrTrailingData},
	{CaseName: "control_char_before_string", in: "\x01 \"string\"", errIs: ErrInvalidNumber},
	{CaseName: "control_char_after_string", in: " \"string\" \x01", errIs: ErrTrailingData},
	{CaseName: "unknown_escape", in: `"\j"`, errIs: ErrUnknwownEscapeChar{Char: 'j'}},
	{CaseName: "incomplete_escape", in: `"a\"`, errIs: ErrUnterminatedString},
	{CaseName: "empty_content", in: ``, errIs: io.ErrUnexpectedEOF},
}

func TestParser(t *testing.T) {
	for _, tc := range validCases {
		t.Run(tc.CaseName, func(t *testing.T) {
			p := parser{data: []byte(tc.in), n: len(tc.in)}

			res, err := p.parse()
			assert.NoError(t, err, tc.in)
			assert.Equal(t, tc.out, res)

			jsonerr := json.Unmarshal([]byte(tc.in), new(any))
			assert.NoError(t, jsonerr, tc.in)

		})
	}
	for _, tc := range invalidCases {
		t.Run(tc.CaseName, func(t *testing.T) {
			p := parser{data: []byte(tc.in), n: len(tc.in)}

			res, err := p.parse()
			assert.ErrorIs(t, err, tc.errIs)
			assert.Nil(t, res)

			jsonerr := json.Unmarshal([]byte(tc.in), new(any))
			assert.Error(t, jsonerr, tc.in)
		})
	}
}

func TestErrorMessages(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{ErrInvalidLiteral{Value: "xyz"}, `invalid literal "xyz"`},
		{ErrUnknwownEscapeChar{Char: 'x'}, `unknown escape char, 'x'`},
		{ErrInvalidControlChar{Char: '\x01'}, `invalid control char, '\x01'`},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.err.Error())
	}
}
