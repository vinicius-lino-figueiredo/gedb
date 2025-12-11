package comparer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

type ComparerTestSuite struct {
	suite.Suite
	c *Comparer
}

func (s *ComparerTestSuite) SetupTest() {
	s.c = NewComparer().(*Comparer)
}

func (s *ComparerTestSuite) TestUndefinedIsSmallest() {
	otherStuff := [...]any{nil, "string", -1, 0, uint(12), false, data.M{},
		time.UnixMilli(12345), data.M{"hello": "world"}, []any{}, "",
		[]any{"quite", 5},
	}
	undefined := fieldnavigator.NewGetSetterEmpty()
	for _, stuff := range otherStuff {
		comp, err := s.c.Compare(undefined, stuff)
		s.NoError(err)
		s.Equal(-1, comp)
		comp, err = s.c.Compare(stuff, undefined)
		s.NoError(err)
		s.Equal(1, comp)
	}
	comp, err := s.c.Compare(undefined, undefined)
	s.NoError(err)
	s.Zero(comp)
}

// nil should always be the smallest value.
func (s *ComparerTestSuite) TestNilIsSecondSmallest() {
	otherStuff := [...]any{"string", "", -1, 0, uint(12), false,
		time.UnixMilli(12345), data.M{}, data.M{"hello": "world"},
		[]any{}, []any{"quite", 5},
	}
	for _, stuff := range otherStuff {
		comp, err := s.c.Compare(nil, stuff)
		s.NoError(err)
		s.Equal(-1, comp)
		comp, err = s.c.Compare(stuff, nil)
		s.NoError(err)
		s.Equal(1, comp)
	}
	comp, err := s.c.Compare(nil, nil)
	s.NoError(err)
	s.Zero(comp)
}

// number should by the second smallest type (any number type).
func (s *ComparerTestSuite) TestNumberIsThirdSmallest() {

	testCases := []struct {
		arg1 any
		arg2 any
		res  int
	}{
		{arg1: int64(-12), arg2: int16(0), res: -1},
		{arg1: uint8(0), arg2: int8(-3), res: 1},
		{arg1: 5.7, arg2: uint32(2), res: 1},
		{arg1: 5.7, arg2: float32(12.3), res: -1},
		{arg1: uint64(0), arg2: uint16(0), res: 0},
		{arg1: -2.6, arg2: -2.6, res: 0},
		{arg1: int32(5), arg2: 5, res: 0},
	}

	for _, tc := range testCases {
		comp, err := s.c.Compare(tc.arg1, tc.arg2)
		s.NoError(err)
		s.Equal(tc.res, comp)
	}

	otherStuff := [...]any{"string", "", false, time.UnixMilli(12345),
		data.M{}, data.M{"hello": "world"}, []any{},
		[]any{"quite", 5},
	}
	for _, number := range [...]any{-12, uint(0), 12, 5.7} {
		for _, stuff := range otherStuff {
			comp, err := s.c.Compare(number, stuff)
			s.NoError(err)
			s.Equal(-1, comp)
			comp, err = s.c.Compare(stuff, number)
			s.NoError(err)
			s.Equal(1, comp)
		}
	}
}

// string should be the third smallest type.
func (s *ComparerTestSuite) TestStringIsFourthSmallest() {
	testCases := []struct {
		arg1 string
		arg2 string
		res  int
	}{
		{arg1: "", arg2: "hey", res: -1},
		{arg1: "hey", arg2: "", res: 1},
		{arg1: "hey", arg2: "hew", res: 1},
		{arg1: "hey", arg2: "hey", res: 0},
	}

	for _, tc := range testCases {
		comp, err := s.c.Compare(tc.arg1, tc.arg2)
		s.NoError(err)
		s.Equal(tc.res, comp)
	}

	otherStuff := [...]any{false, time.UnixMilli(12345), data.M{},
		data.M{"hello": "world"}, []any{}, []any{"quite", 5},
	}
	for _, number := range [...]string{"", "string", "hello world"} {
		for _, stuff := range otherStuff {
			comp, err := s.c.Compare(number, stuff)
			s.NoError(err)
			s.Equal(-1, comp)
			comp, err = s.c.Compare(stuff, number)
			s.NoError(err)
			s.Equal(1, comp)
		}
	}
}

// bool should be the fourth smallest type.
func (s *ComparerTestSuite) TestBoolIsFifthSmallest() {
	testCases := []struct {
		arg1 bool
		arg2 bool
		res  int
	}{
		{arg1: true, arg2: true, res: 0},
		{arg1: false, arg2: false, res: 0},
		{arg1: true, arg2: false, res: 1},
		{arg1: false, arg2: true, res: -1},
	}

	for _, tc := range testCases {
		comp, err := s.c.Compare(tc.arg1, tc.arg2)
		s.NoError(err)
		s.Equal(tc.res, comp)
	}

	otherStuff := [...]any{time.UnixMilli(12345), data.M{},
		data.M{"hello": "world"}, []any{}, []any{"quite", 5},
	}
	for _, number := range [...]bool{true, false} {
		for _, stuff := range otherStuff {
			comp, err := s.c.Compare(number, stuff)
			s.NoError(err)
			s.Equal(-1, comp)
			comp, err = s.c.Compare(stuff, number)
			s.NoError(err)
			s.Equal(1, comp)
		}
	}
}

// date should be the fifth smallest type.
func (s *ComparerTestSuite) TestDateIsSixthSmallest() {
	now := time.Now()
	testCases := []struct {
		arg1 time.Time
		arg2 time.Time
		res  int
	}{
		{arg1: now, arg2: now, res: 0},
		{arg1: time.UnixMilli(54341), arg2: now, res: -1},
		{arg1: now, arg2: time.UnixMilli(54341), res: 1},
		{arg1: time.UnixMilli(0), arg2: time.UnixMilli(-54341), res: 1},
		{arg1: time.UnixMilli(123), arg2: time.UnixMilli(4341), res: -1},
	}

	for _, tc := range testCases {
		comp, err := s.c.Compare(tc.arg1, tc.arg2)
		s.NoError(err)
		s.Equal(tc.res, comp)
	}

	otherStuff := [...]any{data.M{}, data.M{"hello": "world"},
		[]any{}, []any{"quite", 5},
	}
	for _, number := range [...]time.Time{time.UnixMilli(-123), now, time.UnixMilli(5555), time.UnixMilli(0)} {
		for _, stuff := range otherStuff {
			comp, err := s.c.Compare(number, stuff)
			s.NoError(err)
			s.Equal(-1, comp)
			comp, err = s.c.Compare(stuff, number)
			s.NoError(err)
			s.Equal(1, comp)
		}
	}
}

// []any should be the sixth smallest type.
func (s *ComparerTestSuite) TestSliceIsSeventhSmallest() {
	testCases := []struct {
		arg1 []any
		arg2 []any
		res  int
	}{
		{arg1: []any{}, arg2: []any{}, res: 0},
		{arg1: []any{"hello"}, arg2: []any{}, res: 1},
		{arg1: []any{}, arg2: []any{"hello"}, res: -1},
		{arg1: []any{"hello"}, arg2: []any{"hello", "world"}, res: -1},
		{arg1: []any{"hello", "earth"}, arg2: []any{"hello", "world"}, res: -1},
		{arg1: []any{"hello", "zzz"}, arg2: []any{"hello", "world"}, res: 1},
		{arg1: []any{"hello", "world"}, arg2: []any{"hello", "world"}, res: 0},
	}

	for _, tc := range testCases {
		comp, err := s.c.Compare(tc.arg1, tc.arg2)
		s.NoError(err)
		s.Equal(tc.res, comp)
	}

	otherStuff := [...]any{data.M{}, data.M{"hello": "world"}}
	for _, number := range [...][]any{{}, {"yes"}, {"hello", 5}} {
		for _, stuff := range otherStuff {
			comp, err := s.c.Compare(number, stuff)
			s.NoError(err)
			s.Equal(-1, comp)
			comp, err = s.c.Compare(stuff, number)
			s.NoError(err)
			s.Equal(1, comp)
		}
	}
}

// Document should be the greatest type.
func (s *ComparerTestSuite) TestDocumentIsEighthSmallest() {
	testCases := []struct {
		arg1 any
		arg2 any
		res  int
	}{
		{arg1: data.M{"a": 42}, arg2: data.M{"a": 312}, res: -1},
		{arg1: data.M{"a": "42"}, arg2: data.M{"a": "312"}, res: 1},
		{arg1: data.M{"a": 42, "b": 312}, arg2: data.M{"b": 312, "a": 42}, res: 0},
		{arg1: data.M{"a": 42, "b": 312, "c": 54}, arg2: data.M{"b": 313, "a": 42}, res: -1},
		{arg1: data.M{"a": 42, "b": 312, "c": 54}, arg2: struct{}{}, res: -1},
		{arg1: struct{}{}, arg2: data.M{"b": 313, "a": 42}, res: 1},
	}

	for _, tc := range testCases {
		comp, err := s.c.Compare(tc.arg1, tc.arg2)
		s.NoError(err)
		s.Equal(tc.res, comp)
	}
}

// comparison between two unknown types should return errors.
func (s *ComparerTestSuite) TestErrorOnUnknownPair() {
	testCases := []struct {
		arg1 any
		arg2 any
	}{
		{arg1: struct{}{}, arg2: []byte{}},
		{arg1: make(map[string]any), arg2: []string{}},
		{arg1: data.M{"nested": []string{"invalid"}}, arg2: data.M{"invalid": []int{}}},
		{arg1: []any{[]string{"invalid"}}, arg2: []any{[]string{"invalid too"}}},
	}

	for _, tc := range testCases {
		_, err := s.c.Compare(tc.arg1, tc.arg2)
		s.ErrorAs(err, &domain.ErrCannotCompare{})
	}
}

func (s *ComparerTestSuite) TestComparable() {
	undefined := fieldnavigator.NewGetSetterEmpty()
	now := time.Now()
	arr := []any{1, "2", 3.0}
	doc := data.M{"key": "value", "number": 123}

	values := []any{nil, false, true, 1, now, "abc", arr, doc}

	cannotCompare := func(v any) func() {
		return func() {
			s.False(s.c.Comparable(v, undefined))
			for _, value := range values {
				s.False(s.c.Comparable(v, value))
			}
		}
	}

	canCompareSelf := func(v any) func() {
		return func() {
			s.False(s.c.Comparable(v, undefined))
			for _, value := range values {
				b, _ := json.Marshal(v)
				b2, _ := json.Marshal(value)
				if string(b) == string(b2) {
					s.True(s.c.Comparable(v, value))
				} else {
					s.False(s.c.Comparable(v, value))
				}
			}
		}
	}

	s.Run("Undefined", cannotCompare(undefined))
	s.Run("Nil", cannotCompare(nil))
	s.Run("Bool", func() {
		s.Run("False", cannotCompare(false))
		s.Run("True", cannotCompare(true))
	})
	s.Run("Number", func() {
		s.Run("Int", canCompareSelf(int(1)))
		s.Run("Int8", canCompareSelf(int8(1)))
		s.Run("Int16", canCompareSelf(int16(1)))
		s.Run("Int32", canCompareSelf(int32(1)))
		s.Run("Int64", canCompareSelf(int64(1)))
		s.Run("Uint", canCompareSelf(uint(1)))
		s.Run("Uint8", canCompareSelf(uint8(1)))
		s.Run("Uint16", canCompareSelf(uint16(1)))
		s.Run("Uint32", canCompareSelf(uint32(1)))
		s.Run("Uint64", canCompareSelf(uint64(1)))
		s.Run("Float32", canCompareSelf(float32(1)))
		s.Run("Float64", canCompareSelf(float64(1)))
	})
	s.Run("String", canCompareSelf("abc"))
	s.Run("Time", canCompareSelf(now))
	s.Run("Array", cannotCompare(arr))
	s.Run("Document", cannotCompare(doc))
}

func (s *ComparerTestSuite) TestGetSetterDefinedValues() {
	a := fieldnavigator.NewGetSetterWithDoc(data.M{
		"a": data.M{"a": 1, "b": 2},
	}, "a")
	b := fieldnavigator.NewGetSetterWithDoc(data.M{
		"a": data.M{"a": 1, "b": 2, "c": 3},
	}, "a")
	comp, err := s.c.Compare(a, b)
	s.NoError(err)
	s.Less(comp, 0)
	comp, err = s.c.Compare(b, a)
	s.NoError(err)
	s.Greater(comp, 0)
}

func TestComparerTestSuite(t *testing.T) {
	suite.Run(t, new(ComparerTestSuite))
}
