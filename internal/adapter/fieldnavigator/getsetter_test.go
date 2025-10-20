package fieldnavigator

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

type GetSetterTestSuite struct {
	suite.Suite
}

// Can read and modify data at a valid given index of a []any with result of
// [NewGetSetterWithArrayIndex].
func (s *GetSetterTestSuite) TestNewGetSetterWithArrayIndex() {
	arr := []any{0, 1, 2, "3"}
	gs := NewGetSetterWithArrayIndex(arr, 3)

	value, defined := gs.Get()
	s.True(defined)
	s.Equal("3", value)

	gs.Set(3)
	s.Equal(3, arr[3])

	gs.Unset()
	s.Nil(arr[3])
}

// Cannot find a defined value nor modify an out-of-bounds index with
// [NewGetSetterWithArrayIndex].
func (s *GetSetterTestSuite) TestNewGetSetterWithArrayIndexOutOfBounds() {
	arr := []any{0, 1, 2, "3"}
	gs := NewGetSetterWithArrayIndex(arr, 4)

	value, defined := gs.Get()
	s.False(defined)
	s.Nil(value)

	gs.Set(3)
	s.Equal([]any{0, 1, 2, "3"}, arr)

	gs.Unset()
	s.Equal([]any{0, 1, 2, "3"}, arr)
}

// Can get, set and unset a field with [NewGetSetterWithDoc].
func (s *GetSetterTestSuite) TestNewGetSetterWithDoc() {
	obj := data.M{"oh": "no"}
	gs := NewGetSetterWithDoc(obj, "oh")

	value, defined := gs.Get()
	s.True(defined)
	s.Equal("no", value)

	gs.Set("yes")
	s.Equal("yes", obj["oh"])

	gs.Unset()
	s.Equal(nil, obj["oh"])

	value, defined = gs.Get()
	s.False(defined)
	s.Nil(value)
}

// Can only get data with [NewReadOnlyGetSetter].
func (s *GetSetterTestSuite) TestNewReadOnlyGetSetter() {
	data := 1
	gs := NewReadOnlyGetSetter(&data)

	value, defined := gs.Get()
	s.True(defined)
	s.Equal(&data, value)

	gs.Set(2)
	s.Equal(1, data)

	gs.Unset()
	s.Equal(1, data)
}

// Cannot get valid data nor modify a GetSetter from [NewGetSetterEmpty].
func (s *GetSetterTestSuite) NewGetSetterEmpty() {
	gs := NewGetSetterEmpty()

	value, defined := gs.Get()
	s.False(defined)
	s.Nil(value)

	gs.Set(2)
	value, defined = gs.Get()
	s.False(defined)
	s.Nil(value)

	gs.Unset()
	value, defined = gs.Get()
	s.False(defined)
	s.Nil(value)
}

func TestGetSetterTestSuite(t *testing.T) {
	suite.Run(t, new(GetSetterTestSuite))
}
