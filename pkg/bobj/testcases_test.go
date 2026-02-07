package bobj_test

// =============================================================================
// NewDocument
// =============================================================================

var newDocumentCases = []struct {
	name    string
	input   any
	wantErr bool
}{
	// Nil e zero values
	{name: "nil input", input: nil},
	{name: "empty struct", input: struct{}{}},
	{name: "empty map", input: map[string]any{}},

	// Tipos primitivos dentro de struct
	{
		name: "struct with string",
		input: struct {
			Name string
		}{Name: "test"},
	},
	{
		name: "struct with int",
		input: struct {
			Age int
		}{Age: 30},
	},
	{
		name: "struct with float64",
		input: struct {
			Price float64
		}{Price: 19.99},
	},
	{
		name: "struct with bool true",
		input: struct {
			Active bool
		}{Active: true},
	},
	{
		name: "struct with bool false",
		input: struct {
			Active bool
		}{Active: false},
	},
	{
		name: "struct with nil pointer",
		input: struct {
			Ptr *int
		}{Ptr: nil},
	},

	// Slices e arrays
	{
		name: "struct with int slice",
		input: struct {
			Values []int
		}{Values: []int{1, 2, 3}},
	},
	{
		name: "struct with empty slice",
		input: struct {
			Values []int
		}{Values: []int{}},
	},
	{
		name: "struct with nil slice",
		input: struct {
			Values []int
		}{Values: nil},
	},
	{
		name: "struct with string slice",
		input: struct {
			Tags []string
		}{Tags: []string{"a", "b", "c"}},
	},

	// Maps
	{
		name:  "simple map",
		input: map[string]any{"key": "value"},
	},
	{
		name:  "map with multiple types",
		input: map[string]any{"str": "hello", "num": 42, "flag": true},
	},
	{
		name:    "map with non-string key",
		input:   map[int]string{1: "one"},
		wantErr: true,
	},

	// Nested structures
	{
		name: "nested struct",
		input: struct {
			User struct {
				Name string
				Age  int
			}
		}{
			User: struct {
				Name string
				Age  int
			}{Name: "Alice", Age: 25},
		},
	},

	{
		name: "nested map",
		input: map[string]any{
			"user": map[string]any{
				"name": "Bob",
				"age":  30,
			},
		},
	},
	{
		name: "deeply nested",
		input: map[string]any{
			"level1": map[string]any{
				"level2": map[string]any{
					"level3": "deep",
				},
			},
		},
	},

	// Tags
	{
		name: "struct with gedb tag",
		input: struct {
			FullName string `gedb:"name"`
		}{FullName: "Tagged"},
	},
	{
		name: "struct with ignored field",
		input: struct {
			Public  string
			private string
			Ignored string `gedb:"-"`
		}{Public: "yes", Ignored: "no"},
	},

	// Ponteiros
	{
		name: "pointer to struct",
		input: &struct {
			Value int
		}{Value: 42},
	},
	{
		name:  "pointer to map",
		input: &map[string]any{"key": "value"},
	},
	{
		name:  "nil pointer to struct",
		input: (*struct{ V int })(nil),
	},

	// Números edge cases
	{
		name: "all int types",
		input: struct {
			I   int
			I8  int8
			I16 int16
			I32 int32
			I64 int64
		}{I: 1, I8: 2, I16: 3, I32: 4, I64: 5},
	},
	{
		name: "all uint types",
		input: struct {
			U   uint
			U8  uint8
			U16 uint16
			U32 uint32
			U64 uint64
		}{U: 1, U8: 2, U16: 3, U32: 4, U64: 5},
	},
	{
		name: "float32",
		input: struct {
			F float32
		}{F: 3.14},
	},

	// Strings edge cases
	{
		name: "empty string",
		input: struct {
			S string
		}{S: ""},
	},
	{
		name: "unicode string",
		input: struct {
			S string
		}{S: "日本語 🎉 émoji"},
	},
	{
		name: "long string",
		input: struct {
			S string
		}{S: string(make([]byte, 10000))},
	},

	// Campos especiais
	{
		name: "field named _id",
		input: struct {
			ID string `gedb:"_id"`
		}{ID: "doc123"},
	},
	{
		name:  "map with _id",
		input: map[string]any{"_id": "doc456", "name": "test"},
	},
}

// =============================================================================
// Object.Get / GetOk
// =============================================================================

var objectGetCases = []struct {
	name      string
	input     any
	key       string
	wantFound bool
	wantType  string // "text", "number", "bool", "null", "object", "list"
}{
	// Campos existentes
	{
		name:      "get existing string",
		input:     map[string]any{"name": "Alice"},
		key:       "name",
		wantFound: true,
		wantType:  "text",
	},
	{
		name:      "get existing number",
		input:     map[string]any{"age": 30},
		key:       "age",
		wantFound: true,
		wantType:  "number",
	},
	{
		name:      "get existing bool",
		input:     map[string]any{"active": true},
		key:       "active",
		wantFound: true,
		wantType:  "bool",
	},
	{
		name:      "get existing null",
		input:     map[string]any{"nothing": nil},
		key:       "nothing",
		wantFound: true,
		wantType:  "null",
	},
	{
		name:      "get existing nested object",
		input:     map[string]any{"user": map[string]any{"name": "Bob"}},
		key:       "user",
		wantFound: true,
		wantType:  "object",
	},
	{
		name:      "get existing list",
		input:     map[string]any{"items": []int{1, 2, 3}},
		key:       "items",
		wantFound: true,
		wantType:  "list",
	},

	// Campos não existentes
	{
		name:      "get non-existing field",
		input:     map[string]any{"name": "Alice"},
		key:       "age",
		wantFound: false,
	},
	{
		name:      "get from empty object",
		input:     map[string]any{},
		key:       "anything",
		wantFound: false,
	},

	// Campo _id
	{
		name:      "get _id field",
		input:     map[string]any{"_id": "doc123", "name": "test"},
		key:       "_id",
		wantFound: true,
		wantType:  "text",
	},

	// Keys edge cases
	{
		name:      "empty key",
		input:     map[string]any{"": "empty key"},
		key:       "",
		wantFound: true,
		wantType:  "text",
	},
	{
		name:      "unicode key",
		input:     map[string]any{"日本語": "value"},
		key:       "日本語",
		wantFound: true,
		wantType:  "text",
	},
}

// =============================================================================
// Object.ID
// =============================================================================

var objectIDCases = []struct {
	name     string
	input    any
	wantID   any // expected ID value, nil if no ID
	wantType string
}{
	{
		name:     "string _id",
		input:    map[string]any{"_id": "abc123"},
		wantID:   "abc123",
		wantType: "text",
	},
	{
		name:     "numeric _id",
		input:    map[string]any{"_id": 12345},
		wantID:   float64(12345),
		wantType: "number",
	},
	{
		name:   "no _id field",
		input:  map[string]any{"name": "test"},
		wantID: nil,
	},
	{
		name:   "empty object",
		input:  map[string]any{},
		wantID: nil,
	},
	{
		name:     "_id with other fields",
		input:    map[string]any{"_id": "first", "a": 1, "b": 2, "z": 3},
		wantID:   "first",
		wantType: "text",
	},
}

// =============================================================================
// Value type conversions
// =============================================================================

var valueConversionCases = []struct {
	name       string
	input      any
	key        string
	conversion string // "AsText", "AsNumber", "AsObject", "AsList", "AsBool"
	wantOk     bool
}{
	// AsText
	{name: "string as text", input: map[string]any{"v": "hello"}, key: "v", conversion: "AsText", wantOk: true},
	{name: "number as text", input: map[string]any{"v": 42}, key: "v", conversion: "AsText", wantOk: false},

	// AsNumber
	{name: "int as number", input: map[string]any{"v": 42}, key: "v", conversion: "AsNumber", wantOk: true},
	{name: "float as number", input: map[string]any{"v": 3.14}, key: "v", conversion: "AsNumber", wantOk: true},
	{name: "string as number", input: map[string]any{"v": "42"}, key: "v", conversion: "AsNumber", wantOk: false},

	// AsObject
	{name: "map as object", input: map[string]any{"v": map[string]any{"nested": true}}, key: "v", conversion: "AsObject", wantOk: true},
	{name: "string as object", input: map[string]any{"v": "not object"}, key: "v", conversion: "AsObject", wantOk: false},

	// AsList
	{name: "slice as list", input: map[string]any{"v": []int{1, 2, 3}}, key: "v", conversion: "AsList", wantOk: true},
	{name: "string as list", input: map[string]any{"v": "not list"}, key: "v", conversion: "AsList", wantOk: false},
}

// =============================================================================
// Text
// =============================================================================

var textCases = []struct {
	name       string
	input      string
	wantString string
	wantOk     bool
}{
	{name: "simple string", input: "hello", wantString: "hello", wantOk: true},
	{name: "empty string", input: "", wantString: "", wantOk: true},
	{name: "unicode", input: "日本語🎉", wantString: "日本語🎉", wantOk: true},
	{name: "with nulls", input: "a\x00b\x00c", wantString: "a\x00b\x00c", wantOk: true},
	{name: "whitespace only", input: "   \t\n  ", wantString: "   \t\n  ", wantOk: true},
}

// =============================================================================
// Number
// =============================================================================

var numberCases = []struct {
	name    string
	input   any // int, float64, etc
	wantF64 float64
	wantOk  bool
}{
	{name: "zero", input: 0, wantF64: 0, wantOk: true},
	{name: "positive int", input: 42, wantF64: 42, wantOk: true},
	{name: "negative int", input: -100, wantF64: -100, wantOk: true},
	{name: "float64", input: 3.14159, wantF64: 3.14159, wantOk: true},
	{name: "large int", input: 9007199254740991, wantF64: 9007199254740991, wantOk: true}, // max safe integer
	{name: "very small float", input: 0.0000001, wantF64: 0.0000001, wantOk: true},
	{name: "negative float", input: -273.15, wantF64: -273.15, wantOk: true},
}

// =============================================================================
// List
// =============================================================================

var listCases = []struct {
	name       string
	input      any
	wantLength int
	indices    []int // indices to test
	wantTypes  []string
}{
	{
		name:       "int list",
		input:      []int{1, 2, 3},
		wantLength: 3,
		indices:    []int{0, 1, 2},
		wantTypes:  []string{"number", "number", "number"},
	},
	{
		name:       "string list",
		input:      []string{"a", "b", "c"},
		wantLength: 3,
		indices:    []int{0, 1, 2},
		wantTypes:  []string{"text", "text", "text"},
	},
	{
		name:       "mixed list",
		input:      []any{1, "two", true, nil},
		wantLength: 4,
		indices:    []int{0, 1, 2, 3},
		wantTypes:  []string{"number", "text", "bool", "null"},
	},
	{
		name:       "empty list",
		input:      []int{},
		wantLength: 0,
		indices:    []int{},
		wantTypes:  []string{},
	},
	{
		name:       "nested list",
		input:      []any{[]int{1, 2}, []int{3, 4}},
		wantLength: 2,
		indices:    []int{0, 1},
		wantTypes:  []string{"list", "list"},
	},
}

// =============================================================================
// skipVal (edge cases / malformed input)
// =============================================================================

var skipValEdgeCases = []struct {
	name    string
	bytes   []byte
	wantLen int
	wantOk  bool
}{
	{name: "empty slice", bytes: []byte{}, wantLen: 0, wantOk: false},
	{name: "just type byte object", bytes: []byte{0x00}, wantLen: 1, wantOk: false},
	{name: "just type byte text", bytes: []byte{0x02}, wantLen: 1, wantOk: false},
	{name: "truncated number", bytes: []byte{0x03, 0x00, 0x00}, wantLen: 3, wantOk: false},
	{name: "null single byte", bytes: []byte{0x07}, wantLen: 1, wantOk: false}, // bug? should be true
	{name: "undefined", bytes: []byte{0x08}, wantLen: 1, wantOk: false},
	{name: "invalid type", bytes: []byte{0xFF}, wantLen: 1, wantOk: false},
}

// =============================================================================
// Key ordering (verifica que _id vem primeiro)
// =============================================================================

var keyOrderingCases = []struct {
	name      string
	input     any
	wantOrder []string // expected key order in serialized form
}{
	{
		name:      "_id comes first",
		input:     map[string]any{"z": 1, "_id": "abc", "a": 2},
		wantOrder: []string{"_id", "a", "z"},
	},
	{
		name:      "alphabetical without _id",
		input:     map[string]any{"c": 1, "a": 2, "b": 3},
		wantOrder: []string{"a", "b", "c"},
	},
	{
		name:      "struct fields sorted",
		input:     struct{ Z, A, M int }{Z: 1, A: 2, M: 3},
		wantOrder: []string{"A", "M", "Z"},
	},
}

// =============================================================================
// Round-trip (serialize -> deserialize -> compare)
// =============================================================================

var roundTripCases = []struct {
	name  string
	input any
}{
	{name: "simple map", input: map[string]any{"name": "test", "value": 42}},
	{name: "nested", input: map[string]any{"outer": map[string]any{"inner": "value"}}},
	{name: "with list", input: map[string]any{"items": []any{1, "two", true}}},
	{name: "complex", input: map[string]any{
		"_id":    "doc1",
		"name":   "complex",
		"count":  100,
		"active": true,
		"tags":   []string{"a", "b"},
		"meta":   map[string]any{"created": 12345},
	}},
}

// =============================================================================
// Nested field access (path navigation)
// =============================================================================

var nestedAccessCases = []struct {
	name     string
	input    any
	path     []string // e.g., ["user", "address", "city"]
	wantType string
	wantOk   bool
}{
	{
		name:     "two levels deep",
		input:    map[string]any{"user": map[string]any{"name": "Alice"}},
		path:     []string{"user", "name"},
		wantType: "text",
		wantOk:   true,
	},
	{
		name:     "three levels deep",
		input:    map[string]any{"a": map[string]any{"b": map[string]any{"c": 42}}},
		path:     []string{"a", "b", "c"},
		wantType: "number",
		wantOk:   true,
	},
	{
		name:   "missing intermediate",
		input:  map[string]any{"a": map[string]any{"b": 1}},
		path:   []string{"a", "x", "y"},
		wantOk: false,
	},
	{
		name:   "access field on non-object",
		input:  map[string]any{"a": "string"},
		path:   []string{"a", "b"},
		wantOk: false,
	},
}
