package decoder

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

type M = data.M

type DecoderTestSuite struct {
	suite.Suite
	d *Decoder
}

func (s *DecoderTestSuite) SetupTest() {
	s.d = NewDecoder().(*Decoder)
}

func (s *DecoderTestSuite) TestSimpleStruct() {
	type SimpleStruct struct {
		Name  string
		Age   int
		Human bool
	}

	var tgt SimpleStruct
	err := s.d.Decode(M{"name": "Jonathan", "age": 18, "human": true}, &tgt)
	s.NoError(err)
	s.Equal("Jonathan", tgt.Name)
	s.Equal(18, tgt.Age)
	s.Equal(true, tgt.Human)
}

func (s *DecoderTestSuite) TestLists() {
	type ListStruct struct {
		Booleans []bool
		Strings  []string
		Numbers  []int
	}

	data := M{
		"booleans": []any{true, false},
		"strings":  []any{"one", "two"},
		"numbers":  []any{1, uint(2), 3.0},
	}

	var tgt ListStruct
	err := s.d.Decode(data, &tgt)
	s.NoError(err)
	s.Equal([]bool{true, false}, tgt.Booleans)
	s.Equal([]string{"one", "two"}, tgt.Strings)
	s.Equal([]int{1, 2, 3}, tgt.Numbers)
}

func (s *DecoderTestSuite) TestNested() {
	type NestedStruct struct {
		Nested struct {
			Text   string
			Number float64
		}
	}

	data := M{
		"nested": M{
			"text":   "str",
			"number": 1,
		},
	}

	var tgt NestedStruct
	err := s.d.Decode(data, &tgt)
	s.NoError(err)
	s.Equal("str", tgt.Nested.Text)
	s.Equal(1.0, tgt.Nested.Number)
}

func (s *DecoderTestSuite) TestIncompleteData() {
	type IncompleteStruct struct {
		Number  int
		Boolean bool
		Text    string
	}

	tgt := IncompleteStruct{}
	err := s.d.Decode(M{"number": 2}, &tgt)
	s.NoError(err)
	s.Equal(2, tgt.Number)
	s.Zero(tgt.Boolean)
	s.Zero(tgt.Text)

	tgt = IncompleteStruct{}
	err = s.d.Decode(M{"boolean": true}, &tgt)
	s.NoError(err)
	s.Zero(tgt.Number)
	s.Equal(true, tgt.Boolean)
	s.Zero(tgt.Text)

	tgt = IncompleteStruct{}
	err = s.d.Decode(M{"text": "str"}, &tgt)
	s.NoError(err)
	s.Zero(tgt.Number)
	s.Zero(tgt.Boolean)
	s.Equal("str", tgt.Text)
}

func (s *DecoderTestSuite) TestExtraFields() {
	type ExtraFieldsStruct struct {
		Number  int
		Boolean bool
	}

	tgt := ExtraFieldsStruct{}
	err := s.d.Decode(M{"number": 2, "boolean": true, "text": "str"}, &tgt)
	s.NoError(err)
	s.Equal(2, tgt.Number)
	s.Equal(true, tgt.Boolean)
}

func (s *DecoderTestSuite) TestExtraAndMissingFields() {
	type ExtraAndMissingFieldsStruct struct {
		Number  int
		Boolean bool
	}

	tgt := ExtraAndMissingFieldsStruct{}
	err := s.d.Decode(M{"number": 2, "text": "str"}, &tgt)
	s.NoError(err)
	s.Equal(2, tgt.Number)
	s.Zero(tgt.Boolean)
}

func (s *DecoderTestSuite) TestWeaklyTyped() {
	type IncompatibleStruct struct {
		Number  uint
		Boolean bool
		Text    string
	}

	var tgt IncompatibleStruct

	s.ErrorAs(s.d.Decode(M{"number": -1}, &tgt), &domain.ErrDecode{})
	s.ErrorAs(s.d.Decode(M{"boolean": 1}, &tgt), &domain.ErrDecode{})
	s.ErrorAs(s.d.Decode(M{"text": 123}, &tgt), &domain.ErrDecode{})
}

func (s *DecoderTestSuite) TestInvalidPointer() {
	type InvalidPointerStruct struct{}

	var tgt InvalidPointerStruct
	err := s.d.Decode(M{}, tgt)
	s.ErrorIs(err, domain.ErrNonPointer)
}

func TestDecoderTestSuite(t *testing.T) {
	suite.Run(t, new(DecoderTestSuite))
}
