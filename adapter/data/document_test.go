package data

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

type MTestSuite struct {
	suite.Suite
}

func (s *MTestSuite) TestSimpleMap() {
	obj := map[string]any{
		"yeah": "sure",
		"of":   "course",
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"yeah": "sure", "of": "course"}, doc)
}

func (s *MTestSuite) TestSimpleStruct() {
	obj := struct{ No, Yes string }{
		No:  "way",
		Yes: "indeed",
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"No": "way", "Yes": "indeed"}, doc)
}

func (s *MTestSuite) TestUnexportedField() {
	obj := struct{ No, yes string }{
		No:  "way",
		yes: "indeed",
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"No": "way"}, doc)
}

func (s *MTestSuite) TestIgnoreField() {
	obj := struct {
		No  string
		Yes string `gedb:"-"`
	}{
		No:  "way",
		Yes: "indeed",
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"No": "way"}, doc)
}

func (s *MTestSuite) TestPointerValue() {
	obj := &struct{ No, Yes string }{
		No:  "way",
		Yes: "indeed",
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"No": "way", "Yes": "indeed"}, doc)
}

func (s *MTestSuite) TestPointerToNilPointer() {
	obj := (*struct{})(nil)

	doc, err := NewDocument(&obj)
	s.NoError(err)
	s.Equal(M{}, doc)
}

func (s *MTestSuite) TestNamedStruct() {
	obj := struct {
		Compliment1 bool `gedb:"Hello"`
		Compliment2 bool `gedb:"Hi"`
	}{
		Compliment1: true,
		Compliment2: false,
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"Hello": true, "Hi": false}, doc)
}

func (s *MTestSuite) TestEmtpyTag() {
	obj := struct {
		Compliment1 bool `gedb:"Hello"`
		Compliment2 bool `gedb:""`
	}{
		Compliment1: true,
		Compliment2: false,
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"Hello": true, "Compliment2": false}, doc)
}

func (s *MTestSuite) TestOmitEmpty() {
	obj := struct {
		Compliment1 bool  `gedb:"Hello,omitempty"`
		Compliment2 any   `gedb:"Hi,omitempty"`
		Compliment3 []int `gedb:"Sup,omitempty"`
	}{
		Compliment1: true,
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"Hello": true}, doc)
}

func (s *MTestSuite) TestOmitZero() {
	obj := struct {
		Compliment1 bool `gedb:"Hello,omitzero"`
		Compliment2 bool `gedb:"Hi,omitzero"`
	}{
		Compliment1: true,
		Compliment2: false,
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"Hello": true}, doc)
}

func (s *MTestSuite) TestNestedMap() {
	obj := map[string]any{
		"nested": map[string]any{
			"a": "b",
		},
		"x": "y",
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"nested": M{"a": "b"}, "x": "y"}, doc)
}

func (s *MTestSuite) TestNestedStruct() {
	obj := struct {
		Nested struct {
			A int `gedb:"a"`
		}
		X float64 `gedb:"x"`
	}{}
	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"Nested": M{"a": 0}, "x": 0.0}, doc)
}

func (s *MTestSuite) TestNullable() {
	obj := struct {
		Map       map[string]any
		Function  func()
		Channel   chan struct{}
		Slice     []any
		Interface interface{ do() }
	}{
		Map:       nil,
		Function:  nil,
		Channel:   nil,
		Slice:     nil,
		Interface: nil,
	}

	expected := M{
		"Map":       nil,
		"Function":  nil,
		"Channel":   nil,
		"Slice":     nil,
		"Interface": nil,
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(expected, doc, "should use any nil value as any(nil)")

}

func (s *MTestSuite) TestAnyValueMap() {
	now := time.Now()
	rgx := regexp.MustCompile(`^123$`)
	obj := map[string]any{
		"planets": map[string]int{
			"count": 8,
		},
		"values": map[string]float64{
			"pi": 3.14,
		},
		"furniture": map[string]string{
			"couch": "livingroom",
		},
		"nested": map[string]map[string]any{
			"subObj": {
				"value": "a",
				"time":  now,
				"regex": rgx,
			},
			"null": nil,
		},
	}

	expected := M{
		"planets":   M{"count": 8},
		"values":    M{"pi": 3.14},
		"furniture": M{"couch": "livingroom"},
		"nested": M{
			"subObj": M{"value": "a", "time": now, "regex": rgx},
			"null":   nil,
		},
	}

	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(expected, doc)
}

func (s *MTestSuite) TestNonStringKeyMap() {
	obj := map[string]any{
		"value": map[int]any{
			1: 123,
		},
	}
	_, err := NewDocument(obj)
	s.ErrorIs(err, ErrMapKeyType)
}

func (s *MTestSuite) TestNilArg() {
	doc, err := NewDocument(nil)
	s.NoError(err)
	s.Equal(M{}, doc)
}

func (s *MTestSuite) TestNonStructArg() {
	_, err := NewDocument(1)
	s.ErrorIs(err, domain.ErrDocumentType{
		Reason: "expected map or struct, got int",
	})
}

func (s *MTestSuite) TestFastPath() {
	now := time.Now()
	testCases := []struct {
		in  any
		out M
	}{
		{in: map[string]string{"abc": "123"}, out: M{"abc": "123"}},
		{in: map[string]bool{"abc": true}, out: M{"abc": true}},
		{in: map[string]int{"abc": 123}, out: M{"abc": int(123)}},
		{in: map[string]int8{"abc": 123}, out: M{"abc": int8(123)}},
		{in: map[string]int16{"abc": 123}, out: M{"abc": int16(123)}},
		{in: map[string]int32{"abc": 123}, out: M{"abc": int32(123)}},
		{in: map[string]int64{"abc": 123}, out: M{"abc": int64(123)}},
		{in: map[string]uint{"abc": 123}, out: M{"abc": uint(123)}},
		{in: map[string]uint8{"abc": 123}, out: M{"abc": uint8(123)}},
		{in: map[string]uint16{"abc": 123}, out: M{"abc": uint16(123)}},
		{in: map[string]uint32{"abc": 123}, out: M{"abc": uint32(123)}},
		{in: map[string]uint64{"abc": 123}, out: M{"abc": uint64(123)}},
		{in: map[string]float32{"abc": 123}, out: M{"abc": float32(123)}},
		{in: map[string]float64{"abc": 123}, out: M{"abc": float64(123)}},
		{in: map[string]time.Time{"abc": now}, out: M{"abc": now}},
		{in: map[string]time.Duration{"abc": time.Hour}, out: M{"abc": time.Hour}},
	}

	for _, tc := range testCases {
		doc, err := NewDocument(tc.in)
		s.NoError(err)
		s.Equal(tc.out, doc)
	}

}

func (s *MTestSuite) TestArray() {
	obj := map[string][]string{"names": {"Huguinho", "Zezinho", "Luisinho"}}
	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(M{"names": []any{"Huguinho", "Zezinho", "Luisinho"}}, doc)
}

func (s *MTestSuite) TestKeepFunction() {
	ptr := &struct{}{}
	fn := func() any { return ptr }
	obj := map[string]any{"function": fn}
	doc, err := NewDocument(obj)
	s.NoError(err)
	s.Equal(ptr, doc.Get("function").(func() any)())
}

func (s *MTestSuite) TestKeepChannelRef() {
	ch := make(chan int, 1)
	obj := map[string]any{"channel": ch}
	doc, err := NewDocument(obj)
	s.NoError(err)
	doc.Get("channel").(chan int) <- 1
	s.Equal(1, <-ch)
}

func (s *MTestSuite) TestInvalidStructField() {
	obj := struct{ A map[int]int }{A: map[int]int{}}
	_, err := NewDocument(obj)
	s.ErrorIs(err, ErrMapKeyType)
}

func (s *MTestSuite) TestID() {
	id := uuid.NewString()
	obj := map[string]string{"_id": id}
	doc, err := NewDocument(obj)
	s.NoError(err)
	s.True(doc.Has("_id"))
	s.Equal(id, doc.ID())
	s.Equal(id, doc.Get("_id"))
}

func (s *MTestSuite) TestIterationFunctions() {

	doc := M{
		"name": "option",
		"age":  99,
		"key":  "value",
		"pi":   3.14,
	}

	hashMap := maps.Collect(doc.Iter())
	keys := slices.Collect(doc.Keys())
	values := slices.Collect(doc.Values())

	s.Len(hashMap, len(doc))
	s.Len(keys, len(doc))
	s.Len(values, len(doc))
	for key, value := range doc {
		s.Contains(hashMap, key)
		s.Equal(value, hashMap[key])
		s.Contains(keys, key)
		s.Contains(values, value)
	}

}

func (s *MTestSuite) TestSet() {
	doc := M{"a": nil}
	doc.Set("a", "b")
	doc.Set("c", "d")
	s.Equal(M{"a": "b", "c": "d"}, doc)
}

func (s *MTestSuite) TestUnset() {
	doc := M{"a": nil}
	doc.Unset("a")
	doc.Unset("b")
	s.Equal(M{}, doc)
}

func (s *MTestSuite) TestD() {
	doc := M{"a": M{"h": "i"}, "b": 1}
	s.Equal(M{"h": "i"}, doc.D("a"))
	s.Nil(doc.D("b"))
	s.Nil(doc.D("c"))
}

func (s *MTestSuite) TestLen() {

	loop := 1000
	m := make(M)

	s.Equal(0, m.Len())

	for i := range loop {
		m[strconv.Itoa(i)] = i
		s.Equal(i+1, m.Len())
	}
}

func (s *MTestSuite) TestUnmarshalValidJSON() {
	j := `{
		"1": 2,
		"value": [1, 2.5, null, "a", "\n", ["b"], {}],
		"key": {"hey": "ya"}
	}`

	expected := M{
		"1":     2.0,
		"value": []any{1.0, 2.5, nil, "a", "\n", []any{"b"}, M{}},
		"key":   M{"hey": "ya"},
	}

	m := make(M)
	err := json.Unmarshal([]byte(j), &m)
	s.NoError(err)
	s.Equal(expected, m)
}

func (s *MTestSuite) TestUnmarshalInvalidJSON() {
	j := `{"a":FALSE}`

	m := make(M)

	// parse will not return error unless the json passed is invalid, which
	// will not even reach parse() if using json.Unmarshal. Calling it
	// directly allows me to pass an invalid json.
	err := m.UnmarshalJSON([]byte(j))
	s.ErrorIs(err, ErrInvalidNumber)
}

func (s *MTestSuite) TestUnmarshalNonObjectJSON() {
	j := `"a"`

	m := make(M)
	err := json.Unmarshal([]byte(j), &m)
	s.ErrorIs(err, ErrNonObject)
}

func TestMTestSuite(t *testing.T) {
	suite.Run(t, new(MTestSuite))
}

func BenchmarkNewDocument_SmallMap(b *testing.B) {
	obj := map[string]any{
		"name":  "test",
		"value": 42,
		"bool":  true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, err := NewDocument(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewDocument_LargeMap(b *testing.B) {
	obj := make(map[string]any, 1000)
	for i := range 1000 {
		obj[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, err := NewDocument(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewDocument_DeepNested(b *testing.B) {
	obj := map[string]any{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": map[string]any{
					"level4": map[string]any{
						"level5": map[string]any{
							"data": "deeply nested value",
							"num":  123,
						},
					},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, err := NewDocument(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewDocument_Struct(b *testing.B) {
	type TestStruct struct {
		Name    string `gedb:"name"`
		Value   int    `gedb:"value"`
		Enabled bool   `gedb:"enabled"`
		Data    map[string]any
	}

	obj := TestStruct{
		Name:    "benchmark",
		Value:   100,
		Enabled: true,
		Data: map[string]any{
			"extra": "info",
			"count": 456,
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, err := NewDocument(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewDocument_ManySmallDocuments(b *testing.B) {
	objects := make([]map[string]any, 1000)
	for i := range 1000 {
		objects[i] = map[string]any{
			"id":   i,
			"name": fmt.Sprintf("item_%d", i),
			"type": "test",
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		for _, obj := range objects {
			_, err := NewDocument(obj)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkNewDocument_ArraysAndSlices(b *testing.B) {
	obj := map[string]any{
		"numbers": []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"strings": []string{"a", "b", "c", "d", "e"},
		"mixed": []any{
			"string",
			123,
			true,
			map[string]any{"nested": "value"},
		},
		"nested_arrays": [][]int{
			{1, 2, 3},
			{4, 5, 6},
			{7, 8, 9},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, err := NewDocument(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewDocument_MemoryIntensive(b *testing.B) {
	largeData := make([]string, 10000)
	for i := range largeData {
		largeData[i] = fmt.Sprintf("large_string_data_%d_with_extra_content_to_increase_memory_usage", i)
	}

	obj := map[string]any{
		"large_array": largeData,
		"metadata": map[string]any{
			"size": len(largeData),
			"type": "memory_test",
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, err := NewDocument(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}
