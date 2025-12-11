package uncomparable

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/hasher"
)

type hasherMock struct{ mock.Mock }

// Hash implements domain.Hasher.
func (h *hasherMock) Hash(v any) (uint64, error) {
	call := h.Called(v)
	return uint64(call.Int(0)), call.Error(1)
}

type comparerMock struct{ mock.Mock }

// Comparable implements domain.Comparer.
func (c *comparerMock) Comparable(a any, b any) bool {
	return c.Called(a, b).Bool(0)
}

// Compare implements domain.Comparer.
func (c *comparerMock) Compare(a any, b any) (int, error) {
	call := c.Called(a, b)
	return call.Int(0), call.Error(1)
}

type MapTestSuite struct {
	suite.Suite
	m *Map[any]
}

func (s *MapTestSuite) SetupTest() {
	s.m = New[any](hasher.NewHasher(), comparer.NewComparer())
}

func (s *MapTestSuite) TestSet() {
	s.NoError(s.m.Set("key", "value"))
	i, err := s.m.getBucketIndex("key")
	s.NoError(err)
	s.Equal("value", s.m.buckets[i][0].value)

	s.NoError(s.m.Set("key", "another"))
	i, err = s.m.getBucketIndex("key")
	s.NoError(err)
	s.Equal("another", s.m.buckets[i][0].value)

	// set depends on comparer to work
	c := new(comparerMock)
	s.m.comparer = c
	comparisonErr := fmt.Errorf("comparison error")
	c.On("Compare", "key", "key").Return(0, comparisonErr).Once()
	s.ErrorIs(s.m.Set("key", "value"), comparisonErr)

	// if cannot hash, does not reach comparison
	h := new(hasherMock)
	s.m.hasher = h
	hashErr := fmt.Errorf("hash error")
	h.On("Hash", "key").Return(0, hashErr).Once()
	s.ErrorIs(s.m.Set("key", "value"), hashErr)
}

func (s *MapTestSuite) TestGet() {
	s.NoError(s.m.Set("key", "value"))
	value, exists, err := s.m.Get("key")
	s.NoError(err)
	s.True(exists)
	s.Equal("value", value)

	value, exists, err = s.m.Get("nope")
	s.NoError(err)
	s.False(exists)
	s.Nil(value)

	// get depends on comparer to work
	c := new(comparerMock)
	s.m.comparer = c
	comparisonErr := fmt.Errorf("comparison error")
	c.On("Compare", "key", "key").Return(0, comparisonErr).Once()
	val, ok, err := s.m.Get("key")
	s.ErrorIs(err, comparisonErr)
	s.False(ok)
	s.Nil(val)

	// if cannot hash, does not reach comparison
	h := new(hasherMock)
	s.m.hasher = h
	hashErr := fmt.Errorf("hash error")
	h.On("Hash", "key").Return(0, hashErr).Once()
	val, ok, err = s.m.Get("key")
	s.ErrorIs(err, hashErr)
	s.False(ok)
	s.Nil(val)
}

func (s *MapTestSuite) TestDelete() {
	s.NoError(s.m.Set("key", "value"))
	value, exists, err := s.m.Get("key")
	s.NoError(err)
	s.True(exists)
	s.Equal("value", value)

	s.NoError(s.m.Set("another", "value"))
	value, exists, err = s.m.Get("another")
	s.NoError(err)
	s.True(exists)
	s.Equal("value", value)

	s.NoError(s.m.Delete("key"))
	value, exists, err = s.m.Get("key")
	s.NoError(err)
	s.False(exists)
	s.Nil(value)

	s.NoError(s.m.Delete("nope"))
	value, exists, err = s.m.Get("key")
	s.NoError(err)
	s.False(exists)
	s.Nil(value)

	// delete depends on comparer to work
	c := new(comparerMock)
	s.m.comparer = c
	comparisonErr := fmt.Errorf("comparison error")
	c.On("Compare", "key", "another").Return(0, comparisonErr).Once()
	s.ErrorIs(s.m.Delete("key"), comparisonErr)

	// if cannot hash, does not reach comparison
	h := new(hasherMock)
	s.m.hasher = h
	hashErr := fmt.Errorf("hash error")
	h.On("Hash", "key").Return(0, hashErr).Once()
	s.ErrorIs(s.m.Delete("key"), hashErr)
}

func (s *MapTestSuite) TestKeys() {
	s.Len(slices.Collect(s.m.Keys()), 0)

	mp := map[any]any{
		"key0": "value0",
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}
	for k, v := range mp {
		s.NoError(s.m.Set(k, v))
	}

	s.Subset(slices.Collect(maps.Keys(mp)), slices.Collect(s.m.Keys()))
	s.Len(slices.Collect(s.m.Keys()), len(mp))
}

func (s *MapTestSuite) TestIter() {
	s.Len(maps.Collect(s.m.Iter()), 0)
	expected := map[any]any{
		"key0": "value0",
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}
	for k, v := range expected {
		s.NoError(s.m.Set(k, v))
	}

	s.Equal(expected, maps.Collect(s.m.Iter()))
}

func (s *MapTestSuite) TestValues() {
	s.Len(slices.Collect(s.m.Values()), 0)

	mp := map[any]any{
		"key0": "value0",
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}
	for k, v := range mp {
		s.NoError(s.m.Set(k, v))
	}

	s.Subset(slices.Collect(maps.Values(mp)), slices.Collect(s.m.Values()))
	s.Len(slices.Collect(s.m.Values()), len(mp))
}

func (s *MapTestSuite) TestBreakIterations() {

	s.Len(slices.Collect(s.m.Values()), 0)
	s.Len(maps.Collect(s.m.Iter()), 0)
	s.Len(slices.Collect(s.m.Keys()), 0)

	mp := map[any]any{
		"key0": "value0",
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}
	for k, v := range mp {
		s.NoError(s.m.Set(k, v))
	}

	for range s.m.Keys() {
		break
	}
	for range s.m.Iter() {
		break
	}
	for range s.m.Values() {
		break
	}

	s.Subset(slices.Collect(maps.Keys(mp)), slices.Collect(s.m.Keys()))
	s.Len(slices.Collect(s.m.Keys()), len(mp))

	s.Equal(mp, maps.Collect(s.m.Iter()))

	s.Subset(slices.Collect(maps.Values(mp)), slices.Collect(s.m.Values()))
	s.Len(slices.Collect(s.m.Values()), len(mp))

}

func TestMapTestSuite(t *testing.T) {
	suite.Run(t, new(MapTestSuite))
}
