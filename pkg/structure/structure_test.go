package structure

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/goccy/go-reflect"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
)

var simpleMapsTestCases = []any{
	map[string]string{"A": "0"}, map[string]bool{"B": true},
	map[string]int{"C": -2}, map[string]int8{"D": -3},
	map[string]int16{"E": -4}, map[string]int32{"F": -5},
	map[string]int64{"G": -6}, map[string]uint{"H": 7},
	map[string]uint8{"I": 8}, map[string]uint16{"J": 9},
	map[string]uint32{"K": 10}, map[string]uint64{"L": 11},
	map[string]float32{"M": 12.5}, map[string]float64{"N": 13.5},
	data.M{"O": []any{14}}, map[string]any{"P": []int{15}},
	map[string]time.Time{"Q": time.UnixMilli(16)},
	map[string]*regexp.Regexp{"R": regexp.MustCompile(`17`)},
	map[string][]byte{"S": []byte("18")},
}

var simpleStructTestCases = []any{
	struct{ A string }{A: "0"},
	struct{ B bool }{B: true},
	struct{ C int }{C: -2},
	struct{ D int8 }{D: -3},
	struct{ E int16 }{E: -4},
	struct{ F int32 }{F: -5},
	struct{ G int64 }{G: -6},
	struct{ H uint }{H: 7},
	struct{ I uint8 }{I: 8},
	struct{ J uint16 }{J: 9},
	struct{ K uint32 }{K: 10},
	struct{ L uint64 }{L: 11},
	struct{ M float32 }{M: 12.5},
	struct{ N float64 }{N: 13.5},
	struct{ O []any }{O: []any{14}},
	struct{ P []int }{P: []int{15}},
	struct{ Q time.Time }{Q: time.UnixMilli(16)},
	struct{ R *regexp.Regexp }{R: regexp.MustCompile(`17`)},
	struct{ S []byte }{S: []byte("18")},
}

var namedStructTestCases = []any{
	struct {
		B string `gedb:"A"`
	}{B: "0"},
	struct {
		C bool `gedb:"B"`
	}{C: true},
	struct {
		D int `gedb:"C"`
	}{D: -2},
	struct {
		E int8 `gedb:"D"`
	}{E: -3},
	struct {
		F int16 `gedb:"E"`
	}{F: -4},
	struct {
		G int32 `gedb:"F"`
	}{G: -5},
	struct {
		H int64 `gedb:"G"`
	}{H: -6},
	struct {
		I uint `gedb:"H"`
	}{I: 7},
	struct {
		J uint8 `gedb:"I"`
	}{J: 8},
	struct {
		K uint16 `gedb:"J"`
	}{K: 9},
	struct {
		L uint32 `gedb:"K"`
	}{L: 10},
	struct {
		M uint64 `gedb:"L"`
	}{M: 11},
	struct {
		N float32 `gedb:"M"`
	}{N: 12.5},
	struct {
		O float64 `gedb:"N"`
	}{O: 13.5},
	struct {
		P []any `gedb:"O"`
	}{P: []any{14}},
	struct {
		Q []int `gedb:"P"`
	}{Q: []int{15}},
	struct {
		R time.Time `gedb:"Q"`
	}{R: time.UnixMilli(16)},
	struct {
		S *regexp.Regexp `gedb:"R"`
	}{S: regexp.MustCompile(`17`)},
	struct {
		T []byte `gedb:"S"`
	}{T: []byte("18")},
}

var mapPointerTestCases = [...]any{
	&map[string]string{"A": "0"}, &map[string]bool{"B": true},
	&map[string]int{"C": -2}, &map[string]int8{"D": -3},
	&map[string]int16{"E": -4}, &map[string]int32{"F": -5},
	&map[string]int64{"G": -6}, &map[string]uint{"H": 7},
	&map[string]uint8{"I": 8}, &map[string]uint16{"J": 9},
	&map[string]uint32{"K": 10}, &map[string]uint64{"L": 11},
	&map[string]float32{"M": 12.5}, &map[string]float64{"N": 13.5},
	&data.M{"O": []any{14}}, &map[string]any{"P": []int{15}},
	&map[string]time.Time{"Q": time.UnixMilli(16)},
	&map[string]*regexp.Regexp{"R": regexp.MustCompile(`17`)},
	&map[string][]byte{"S": []byte("18")},
}

var expectedSimple = [...]struct {
	key   string
	value any
}{
	{key: "A", value: "0"}, {key: "B", value: true},
	{key: "C", value: -2}, {key: "D", value: int8(-3)},
	{key: "E", value: int16(-4)}, {key: "F", value: int32(-5)},
	{key: "G", value: int64(-6)}, {key: "H", value: uint(7)},
	{key: "I", value: uint8(8)}, {key: "J", value: uint16(9)},
	{key: "K", value: uint32(10)}, {key: "L", value: uint64(11)},
	{key: "M", value: float32(12.5)}, {key: "N", value: 13.5},
	{key: "O", value: []any{14}}, {key: "P", value: []int{15}},
	{key: "Q", value: time.UnixMilli(16)},
	{key: "R", value: regexp.MustCompile(`17`)},
	{key: "S", value: []byte("18")},
}

var simpleSlicesTestCases = [...]any{
	[]string{"1", "2", "3"}, []bool{false, true, true}, []int{-1, 2, 3},
	[]int8{3, -2, 1}, []int16{2, 3, -1}, []int32{2, -1, 3},
	[]int64{-12, 67, 69}, []uint{0, 0, 0}, []uint8{1, 1, 1},
	[]uint16{2, 2, 2}, []uint32{3, 3, 3}, []uint64{4, 4, 4},
	[]float32{1.5, 1.2, 4.3}, []float64{6.7, 6.9, 12.3},
	[]any{"string", 1, false},
	[]time.Time{time.UnixMilli(1), time.UnixMilli(2), time.UnixMilli(3)},
	[]*regexp.Regexp{regexp.MustCompile(`1`), regexp.MustCompile(`2`), regexp.MustCompile(`3`)},
	[][]byte{[]byte("a"), []byte("b"), []byte("c")},
}

var simpleArraysTestCases = [...]any{
	[...]string{"1", "2", "3"}, [...]bool{false, true, true}, [...]int{-1, 2, 3},
	[...]int8{3, -2, 1}, [...]int16{2, 3, -1}, [...]int32{2, -1, 3},
	[...]int64{-12, 67, 69}, [...]uint{0, 0, 0}, [...]uint8{1, 1, 1},
	[...]uint16{2, 2, 2}, [...]uint32{3, 3, 3}, [...]uint64{4, 4, 4},
	[...]float32{1.5, 1.2, 4.3}, [...]float64{6.7, 6.9, 12.3},
	[...]any{"string", 1, false},
	[...]time.Time{time.UnixMilli(1), time.UnixMilli(2), time.UnixMilli(3)},
	[...]*regexp.Regexp{regexp.MustCompile(`1`), regexp.MustCompile(`2`), regexp.MustCompile(`3`)},
	[...][]byte{[]byte("a"), []byte("b"), []byte("c")},
}

var slicePointersTestCases = [...]any{
	&[]string{"1", "2", "3"}, &[]bool{false, true, true}, &[]int{-1, 2, 3},
	&[]int8{3, -2, 1}, &[]int16{2, 3, -1}, &[]int32{2, -1, 3},
	&[]int64{-12, 67, 69}, &[]uint{0, 0, 0}, &[]uint8{1, 1, 1},
	&[]uint16{2, 2, 2}, &[]uint32{3, 3, 3}, &[]uint64{4, 4, 4},
	&[]float32{1.5, 1.2, 4.3}, &[]float64{6.7, 6.9, 12.3},
	&[]any{"string", 1, false},
	&[]time.Time{time.UnixMilli(1), time.UnixMilli(2), time.UnixMilli(3)},
	&[]*regexp.Regexp{regexp.MustCompile(`1`), regexp.MustCompile(`2`), regexp.MustCompile(`3`)},
	&[][]byte{[]byte("a"), []byte("b"), []byte("c")},
}

var expectedSimpleSlices = [...][]any{
	{"1", "2", "3"}, {false, true, true}, {-1, 2, 3},
	{int8(3), int8(-2), int8(1)}, {int16(2), int16(3), int16(-1)},
	{int32(2), int32(-1), int32(3)}, {int64(-12), int64(67), int64(69)},
	{uint(0), uint(0), uint(0)}, {uint8(1), uint8(1), uint8(1)},
	{uint16(2), uint16(2), uint16(2)}, {uint32(3), uint32(3), uint32(3)},
	{uint64(4), uint64(4), uint64(4)},
	{float32(1.5), float32(1.2), float32(4.3)}, {6.7, 6.9, 12.3},
	{"string", 1, false},
	{time.UnixMilli(1), time.UnixMilli(2), time.UnixMilli(3)},
	{regexp.MustCompile(`1`), regexp.MustCompile(`2`), regexp.MustCompile(`3`)},
	{[]byte("a"), []byte("b"), []byte("c")},
}

type StructureTestSuite struct {
	suite.Suite
}

func (s *StructureTestSuite) TestSeq2SimpleMap() {
	for n, tc := range simpleMapsTestCases {
		s.Run(fmt.Sprintf("%T", expectedSimple[n].value), func() {
			seq, l, err := Seq2(tc)
			if !s.NoError(err) {
				return
			}
			s.Equal(1, l)
			res := maps.Collect(seq)
			s.Len(res, 1)
			if !s.Contains(res, expectedSimple[n].key) {
				return
			}
			s.Equal(expectedSimple[n].value, res[expectedSimple[n].key])
		})
	}
}

func (s *StructureTestSuite) TestSeq2SimpleStruct() {
	for n, tc := range simpleStructTestCases {
		s.Run(fmt.Sprintf("%T", expectedSimple[n].value), func() {
			seq, l, err := Seq2(tc)
			if !s.NoError(err) {
				return
			}
			s.Equal(1, l)
			res := maps.Collect(seq)
			s.Len(res, 1)
			if !s.Contains(res, expectedSimple[n].key) {
				return
			}
			s.Equal(expectedSimple[n].value, res[expectedSimple[n].key])
		})
	}
}

func (s *StructureTestSuite) TestSeq2NamedStruct() {
	for n, tc := range namedStructTestCases {
		s.Run(fmt.Sprintf("%T", expectedSimple[n].value), func() {
			seq, l, err := Seq2(tc)
			if !s.NoError(err) {
				return
			}
			s.Equal(1, l)
			res := maps.Collect(seq)
			s.Len(res, 1)
			if !s.Contains(res, expectedSimple[n].key) {
				return
			}
			s.Equal(expectedSimple[n].value, res[expectedSimple[n].key])
		})
	}
}

func (s *StructureTestSuite) TestSeq2OmitFlags() {
	testCase := struct {
		unexportedDoesntAppear  int
		NameIsTheSame           string
		NameChanges             bool    `gedb:"becomesTheTag"`
		TagRespectsCase         float64 `gedb:"tagRespectsCase"`
		ThisShowsUpUnchanged    any     `gedb:",omitEmpty"`
		ThisDoesNot             any     `gedb:",omitEmpty"`
		ThisCannotBeOmited      int     `gedb:",omitEmpty"`
		ThisShowsUpUnchangedToo any     `gedb:",omitZero"`
		ThisDoesNotShowUp       any     `gedb:",omitZero"`
		ThisDoesNotShowUpEither int     `gedb:",omitZero"`
		ThisAlsoShowsUp         int     `gedb:",omitZero"`
	}{
		unexportedDoesntAppear:  0,
		NameIsTheSame:           "yes",
		NameChanges:             true,
		TagRespectsCase:         6.7,
		ThisShowsUpUnchanged:    "when it is not empty",
		ThisDoesNot:             nil,
		ThisCannotBeOmited:      0,
		ThisShowsUpUnchangedToo: "because it is not nil",
		ThisDoesNotShowUp:       nil,
		ThisDoesNotShowUpEither: 0,
		ThisAlsoShowsUp:         12,
	}
	expected := map[string]any{
		"NameIsTheSame":           "yes",
		"becomesTheTag":           true,
		"tagRespectsCase":         6.7,
		"ThisShowsUpUnchanged":    "when it is not empty",
		"ThisCannotBeOmited":      0,
		"ThisShowsUpUnchangedToo": "because it is not nil",
		"ThisAlsoShowsUp":         12,
	}
	seq2, l, err := Seq2(testCase)
	if !s.NoError(err) {
		return
	}
	s.Equal(7, l)
	res := maps.Collect(seq2)
	s.Equal(expected, res)
}

func (s *StructureTestSuite) TestSeq2Primitive() {
	primitives := []any{
		1, int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1),
		uint16(1), uint32(1), uint64(1), float32(1), 1,
		time.Now(), regexp.MustCompile(`^abc`), []byte("gief"),
	}

	for _, primitive := range primitives {
		s.Run(fmt.Sprintf("%T", primitive), func() {
			seq2, length, err := Seq2(primitive)
			s.Nil(seq2)
			s.Zero(length)
			var e ErrNonObject
			if !s.ErrorAs(err, &e) {
				return
			}
			s.Equal(reflect.TypeOf(primitive), e.Type)
		})
	}

}

func (s *StructureTestSuite) TestSeq2NilArgument() {
	seq2, length, err := Seq2(nil)
	s.ErrorIs(err, ErrNilObj)
	s.Zero(length)
	s.Nil(seq2)
}

func (s *StructureTestSuite) TestPointerSeq2() {
	s.Run("Nil", func() {
		seq2, length, err := Seq2((*map[string]string)(nil))
		s.ErrorIs(err, ErrNilObj)
		s.Zero(length)
		s.Nil(seq2)
	})

	s.Run("NonNil", func() {
		for p, tc := range mapPointerTestCases {
			s.Run(fmt.Sprintf("%T", expectedSimple[p].value), func() {
				seq, l, err := Seq2(tc)
				if !s.NoError(err) {
					return
				}
				s.Equal(1, l)
				res := maps.Collect(seq)
				s.Len(res, 1)
				if !s.Contains(res, expectedSimple[p].key) {
					return
				}
				s.Equal(expectedSimple[p].value, res[expectedSimple[p].key])
			})
		}
	})

	// [*data.M] implements [domain.Document] without dereferencing, so we
	// need a pointer to a pointer.
	s.Run("DocumentPointer", func() {
		m := &data.M{"a": "b", "c": 4}
		seq2, length, err := Seq2(&m)
		if !s.NoError(err) {
			return
		}
		s.Equal(2, length)
		s.Equal(map[string]any{"a": "b", "c": 4}, maps.Collect(seq2))
	})

	s.Run("PointerToNonObj", func() {
		_, _, err := Seq2(new(string))
		s.ErrorIs(err, ErrNonObject{Type: reflect.TypeOf(*new(string))})
	})
}

func (s *StructureTestSuite) TestSeq2NonStringKey() {
	type tc struct {
		m any
		k any
		v any
	}

	now := time.Now()

	valid := []tc{
		{m: map[string]any{"a": nil}, k: "a", v: nil},
		{m: map[string]int{"b": 0}, k: "b", v: 0},
		{m: map[string]bool{"c": true}, k: "c", v: true},
		{m: map[string]string{"d": "b"}, k: "d", v: "b"},
		{m: map[string]int8{"e": 3}, k: "e", v: int8(3)},
		{m: map[string]int16{"f": 4}, k: "f", v: int16(4)},
		{m: map[string]int32{"g": 5}, k: "g", v: int32(5)},
		{m: map[string]int64{"h": 6}, k: "h", v: int64(6)},
		{m: map[string]uint{"i": 7}, k: "i", v: uint(7)},
		{m: map[string]uint8{"j": 8}, k: "j", v: uint8(8)},
		{m: map[string]uint16{"k": 9}, k: "k", v: uint16(9)},
		{m: map[string]uint32{"l": 10}, k: "l", v: uint32(10)},
		{m: map[string]uint64{"m": 11}, k: "m", v: uint64(11)},
		{m: map[string]float32{"n": 12}, k: "n", v: float32(12)},
		{m: map[string]float64{"o": 13}, k: "o", v: 13.0},
		{m: map[string]time.Time{"p": now}, k: "p", v: now},
		{m: map[string]*regexp.Regexp{"q": regexp.MustCompile(`o`)}, k: "q", v: regexp.MustCompile(`o`)},
		{m: map[string][]byte{"r": []byte("p")}, k: "r", v: []byte("p")},
	}
	invalid := []tc{
		{m: map[int]any{0: nil}, k: 0},
		{m: map[int8]int{1: 0}, k: 1},
		{m: map[int16]bool{2: true}, k: 2},
		{m: map[int32]string{3: "b"}, k: 3},
		{m: map[int64]int8{4: 3}, k: 4},
		{m: map[uint]int16{5: 4}, k: 5},
		{m: map[uint8]int32{6: 5}, k: 6},
		{m: map[uint16]int64{7: 6}, k: 7},
		{m: map[uint32]uint{8: 7}, k: 8},
		{m: map[uint64]uint8{9: 8}, k: 9},
		{m: map[float32]uint16{10: 9}, k: 10},
		{m: map[float64]uint32{11: 10}, k: 11},
		{m: map[time.Time]uint64{now: 11}, k: now},
		{m: map[bool]float32{false: 12}, k: false},
	}

	s.Run("Valid", func() {
		for _, v := range valid {
			seq2, length, err := Seq2(v.m)
			if !s.NoError(err) {
				continue
			}
			if !s.Equal(1, length, fmt.Sprintf("%T", v.m)) {
				continue
			}
			c := maps.Collect(seq2)
			if !s.Contains(c, v.k) {
				continue
			}
			if !s.Equal(v.v, c[v.k.(string)]) {
				continue
			}
		}
	})

	s.Run("Invalid", func() {
		for _, i := range invalid {
			s.Run(fmt.Sprintf("%T", i.m), func() {
				_, _, err := Seq2(i.m)
				s.ErrorIs(err, ErrNonObject{Type: reflect.TypeOf(i.m)})
			})
		}
	})

}

func (s *StructureTestSuite) TestSeq2Stop() {
	str := struct{ A, B, C string }{}
	mp1 := map[string]any{"a": "b", "c": "d", "e": "f"}
	mp2 := map[string][]int{"a": nil, "c": nil, "e": nil}

	s.Run("Struct", func() {
		seq2, l, err := Seq2(str)
		if !s.NoError(err) {
			return
		}
		s.Equal(3, l)
		if !s.NotNil(seq2) {
			return
		}
		for range seq2 {
			break
		}
	})

	s.Run("KnownMap", func() {
		seq2, l, err := Seq2(mp1)
		if !s.NoError(err) {
			return
		}
		s.Equal(3, l)
		if !s.NotNil(seq2) {
			return
		}
		for range seq2 {
			break
		}
	})

	s.Run("UnknownMap", func() {
		seq2, l, err := Seq2(mp2)
		if !s.NoError(err) {
			return
		}
		s.Equal(3, l)
		if !s.NotNil(seq2) {
			return
		}
		for range seq2 {
			break
		}
	})

}

func (s *StructureTestSuite) TestSeqStop() {
	s1 := []any{"a", "b", "c", "d", "e", "f"}
	a1 := [...]any{"a", "b", "c", "d", "e", "f"}

	s.Run("KnownSlice", func() {
		seq, l, err := Seq(s1)
		if !s.NoError(err) {
			return
		}
		s.Equal(6, l)
		if !s.NotNil(seq) {
			return
		}
		for range seq {
			break
		}
	})

	s.Run("array", func() {
		seq, l, err := Seq(a1)
		if !s.NoError(err) {
			return
		}
		s.Equal(6, l)
		if !s.NotNil(seq) {
			return
		}
		for range seq {
			break
		}
	})

}

func (s *StructureTestSuite) TestSeqSimpleSlice() {
	for n, tc := range simpleSlicesTestCases {
		s.Run(fmt.Sprintf("%T", expectedSimple[n].value), func() {
			seq, l, err := Seq(tc)
			if !s.NoError(err) {
				return
			}
			s.Equal(3, l)
			res := slices.Collect(seq)
			s.Len(res, 3)
			for i, v := range res {
				expected := expectedSimpleSlices[n][i]
				s.Equal(expected, v)
			}
		})
	}
}

func (s *StructureTestSuite) TestSeqSimpleArray() {
	for n, tc := range simpleArraysTestCases {
		s.Run(fmt.Sprintf("%T", expectedSimple[n].value), func() {
			seq, l, err := Seq(tc)
			if !s.NoError(err) {
				return
			}
			s.Equal(3, l)
			res := slices.Collect(seq)
			s.Len(res, 3)
			for i, v := range res {
				expected := expectedSimpleSlices[n][i]
				s.Equal(expected, v)
			}
		})
	}
}

func (s *StructureTestSuite) TestSeqPrimitive() {
	primitives := []any{
		1, int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1),
		uint16(1), uint32(1), uint64(1), float32(1), 1,
		time.Now(), regexp.MustCompile(`^abc`),
	}

	for _, primitive := range primitives {
		s.Run(fmt.Sprintf("%T", primitive), func() {
			seq, length, err := Seq(primitive)
			s.Nil(seq)
			s.Zero(length)
			var e ErrNonList
			if !s.ErrorAs(err, &e) {
				return
			}
			s.Equal(reflect.TypeOf(primitive), e.Type)
		})
	}

}

func (s *StructureTestSuite) TestSeqNilArgument() {
	seq, length, err := Seq(nil)
	s.ErrorIs(err, ErrNilObj)
	s.Zero(length)
	s.Nil(seq)
}

func (s *StructureTestSuite) TestPointerSeq() {
	s.Run("Nil", func() {
		seq, length, err := Seq((*[]any)(nil))
		s.ErrorIs(err, ErrNilObj)
		s.Zero(length)
		s.Nil(seq)
	})

	s.Run("NonNil", func() {
		for n, tc := range slicePointersTestCases {
			s.Run(fmt.Sprintf("%T", expectedSimple[n].value), func() {
				seq, l, err := Seq(tc)
				if !s.NoError(err) {
					return
				}
				s.Equal(3, l)
				res := slices.Collect(seq)
				s.Len(res, 3)
				for i, v := range res {
					expected := expectedSimpleSlices[n][i]
					s.Equal(expected, v)
				}
			})
		}
	})

	s.Run("PointerToNonObj", func() {
		_, _, err := Seq(new(string))
		s.ErrorIs(err, ErrNonList{Type: reflect.TypeOf(*new(string))})
	})
}

func (s *StructureTestSuite) TestAsInteger() {
	valid := []any{
		int(0), int8(1), int16(2), int32(3), int64(4), uint(5),
		uint8(6), uint16(7), uint32(8), uint64(9), float32(10),
		float64(11),
	}

	invalid := []any{float32(10.1), float64(11.2), true, false, "text"}

	s.Run("Valid", func() {
		for n, v := range valid {
			s.Run(fmt.Sprintf("%T", v), func() {
				integer, ok := AsInteger(v)
				if !s.True(ok) {
					return
				}
				s.Equal(n, integer)
			})
		}
	})

	s.Run("Invalid", func() {
		for _, i := range invalid {
			s.Run(fmt.Sprintf("%T", i), func() {
				integer, ok := AsInteger(i)
				if !s.False(ok) {
					return
				}
				s.Zero(integer)
			})
		}
	})

}

func (s *StructureTestSuite) TestContains() {
	contains := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	notContains := []int{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15}
	var fnErr error
	fn := func(a, b int) (bool, error) { return a == b, fnErr }

	s.Run("Existent", func() {
		c, err := Contains(contains, 8, fn)
		if !s.NoError(err) {
			return
		}
		s.True(c)
	})

	s.Run("NonExistent", func() {
		c, err := Contains(notContains, 8, fn)
		if !s.NoError(err) {
			return
		}
		s.False(c)
	})

	fnErr = fmt.Errorf("compare error")

	s.Run("ExistentFail", func() {
		_, err := Contains(contains, 8, fn)
		s.ErrorIs(err, fnErr)
	})

	s.Run("NonExistentFail", func() {
		_, err := Contains(notContains, 8, fn)
		s.ErrorIs(err, fnErr)
	})

}

func (s *StructureTestSuite) TestErrorMessages() {
	e1 := &ErrNonObject{Type: reflect.TypeOf(*new(string))}
	e2 := &ErrNonList{Type: reflect.TypeOf(*new(string))}

	s.Equal("type string is not a valid object", e1.Error())
	s.Equal("type string is not a valid list", e2.Error())
}

func TestStructureTestSuite(t *testing.T) {
	suite.Run(t, new(StructureTestSuite))
}
