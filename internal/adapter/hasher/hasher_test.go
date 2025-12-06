package hasher

import (
	"encoding/json"
	"fmt"
	"iter"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

type M = data.M
type A = []any

type docMock struct {
	M
	reverse bool
}

// D implements [domain.Document].
func (d *docMock) D(string) domain.Document { panic("unimplemented") }

// Get implements [domain.Document].
func (d *docMock) Get(string) any { panic("unimplemented") }

// Has implements [domain.Document].
func (d *docMock) Has(string) bool { panic("unimplemented") }

// ID implements [domain.Document].
func (d *docMock) ID() any { panic("unimplemented") }

// Iter implements [domain.Document].
func (d *docMock) Iter() iter.Seq2[string, any] {
	keys := slices.Collect(d.M.Keys())
	slices.Sort(keys)
	if d.reverse {
		slices.Reverse(keys)
	}
	return func(yield func(string, any) bool) {
		for _, k := range keys {
			if !yield(k, d.M[k]) {
				return
			}
		}
	}
}

// Keys implements [domain.Document].
func (d *docMock) Keys() iter.Seq[string] { panic("unimplemented") }

// Len implements [domain.Document].
func (d *docMock) Len() int {
	return d.M.Len()
}

// Set implements [domain.Document].
func (d *docMock) Set(string, any) { panic("unimplemented") }

// Unset implements [domain.Document].
func (d *docMock) Unset(string) { panic("unimplemented") }

// Values implements [domain.Document].
func (d *docMock) Values() iter.Seq[any] { panic("unimplemented") }

type HasherTestSuite struct {
	suite.Suite
	hasher *Hasher
}

func (s *HasherTestSuite) SetupTest() {
	s.hasher = NewHasher().(*Hasher)
}

// Can hash primitive types.
func (s *HasherTestSuite) TestPrimitiveTypes() {
	h, err := s.hasher.Hash("primitive")
	s.NoError(err)
	s.NotZero(h)

	h, err = s.hasher.Hash(971317123)
	s.NoError(err)
	s.NotZero(h)

	h, err = s.hasher.Hash(false)
	s.NoError(err)
	s.NotZero(h)

	h, err = s.hasher.Hash(nil)
	s.NoError(err)
	s.NotZero(h)

	h, err = s.hasher.Hash(3.14)
	s.NoError(err)
	s.NotZero(h)
}

// Will produce the same value when hashing alike types.
func (s *HasherTestSuite) TestAlikeTypes() {
	values := []any{
		uint(3), uint8(3), uint16(3), uint32(3), uint64(3), int(3),
		int8(3), int16(3), int32(3), int64(3), float32(3), float64(3),
	}
	ref, err := s.hasher.Hash(values[0])
	s.NoError(err)
	s.NotZero(ref)

	for _, value := range values {
		s.Run(fmt.Sprintf("%T", value), func() {
			h, err := s.hasher.Hash(value)
			s.NoError(err)
			s.Equal(ref, h)
		})
	}
}

func (s *HasherTestSuite) TestSliceItemOrder() {
	data := A{"one", "two", "three"}
	rev := A{"three", "two", "one"}

	dataHash, err := s.hasher.Hash(data)
	s.NoError(err)

	revHash, err := s.hasher.Hash(rev)
	s.NoError(err)

	s.NotEqual(dataHash, revHash)
}

// Document hash value will not be affected by key/value order.
func (s *HasherTestSuite) TestDocKeyOrder() {
	data := M{"fish": "u", "bone": "s", "flower": "f"}

	doc1 := &docMock{M: data, reverse: false}
	doc2 := &docMock{M: data, reverse: true}

	h1, err := s.hasher.Hash(doc1)
	s.NoError(err)

	h2, err := s.hasher.Hash(doc2)
	s.NoError(err)

	s.Equal(h1, h2)
}

// Different documents will not have the same hash.
func (s *HasherTestSuite) TestNotEqualComplexTypes() {
	bestOption := M{"flavor": "butterscotch", "pie": true}
	worstOption := M{"flavor": "cinnamon", "pie": true}

	h1, err := s.hasher.Hash(bestOption)
	s.NoError(err)
	s.NotZero(h1)

	h2, err := s.hasher.Hash(worstOption)
	s.NoError(err)
	s.NotZero(h2)

	s.NotEqual(h1, h2)
}

// Different values will have different hashes.
func (s *HasherTestSuite) TestNotEqual() {
	h1, err := s.hasher.Hash("Jorge Ben")
	s.NoError(err)
	s.NotZero(h1)

	h2, err := s.hasher.Hash("Jorge Ben Jor")
	s.NoError(err)
	s.NotZero(h2)

	s.NotEqual(h1, h2)
}

// for non marshable types, use their pointer to hash.
func (s *HasherTestSuite) TestNonMarshableTypes() {
	s.Run("function", func() {
		value := func() {}
		valPtr := reflect.ValueOf(value).Pointer()
		value2 := func() {}

		valueHash1, err := s.hasher.Hash(value)
		s.NoError(err)

		valueHash2, err := s.hasher.Hash(value2)
		s.NoError(err)

		pointerHash, err := s.hasher.Hash(valPtr)
		s.NoError(err)

		s.NotEqual(valueHash1, valueHash2)
		s.Equal(pointerHash, valueHash1)
	})

	s.Run("channel", func() {
		value := make(chan int)
		valPtr := reflect.ValueOf(value).Pointer()
		value2 := make(chan int)

		valueHash1, err := s.hasher.Hash(value)
		s.NoError(err)

		valueHash2, err := s.hasher.Hash(value2)
		s.NoError(err)

		pointerHash, err := s.hasher.Hash(valPtr)
		s.NoError(err)

		s.NotEqual(valueHash1, valueHash2)
		s.Equal(pointerHash, valueHash1)
	})
}

// nil references/pointers are hashed the as if they were any(nil).
func (s *HasherTestSuite) TestNilNonMarshable() {
	exp, err := s.hasher.Hash(nil)
	s.NoError(err)

	var pointer *int
	var channel chan struct{}
	var function func()

	s.Run("pointer", func() {
		h, err := s.hasher.Hash(pointer)
		s.NoError(err)
		s.Equal(exp, h)
	})

	s.Run("channel", func() {
		h, err := s.hasher.Hash(channel)
		s.NoError(err)
		s.Equal(exp, h)
	})

	s.Run("function", func() {
		h, err := s.hasher.Hash(function)
		s.NoError(err)
		s.Equal(exp, h)
	})
}

// Pointers will be hashed as the uintptr, not the pointed values.
func (s *HasherTestSuite) TestPointer() {
	var a int
	var b int

	ha, err := s.hasher.Hash(&a)
	s.NoError(err)

	hb, err := s.hasher.Hash(&b)
	s.NoError(err)

	s.NotEqual(ha, hb)
}

// Marshal errors should be returned.
func (s *HasherTestSuite) TestMarshalError() {
	// Infinity is not supported by JSON standard
	_, err := s.hasher.Hash(math.Inf(1))
	var jsonErr *json.UnsupportedValueError
	s.ErrorAs(err, &jsonErr)
	s.Equal("+Inf", jsonErr.Str)
	s.True(jsonErr.Value.Equal(reflect.ValueOf(math.Inf(1))))
}

// Marshal errors in nested structures should be returned.
func (s *HasherTestSuite) TestMarshalErrorInDocument() {
	doc := &docMock{
		M: M{"value": math.Inf(1)}, // Infinity in document
	}
	_, err := s.hasher.Hash(doc)
	var jsonErr *json.UnsupportedValueError
	s.ErrorAs(err, &jsonErr)
	s.Equal("+Inf", jsonErr.Str)
}

// Marshal errors in arrays should be returned.
func (s *HasherTestSuite) TestMarshalErrorInArray() {
	_, err := s.hasher.Hash(math.Inf(1))
	var jsonErr *json.UnsupportedValueError
	s.ErrorAs(err, &jsonErr)
	s.Equal("+Inf", jsonErr.Str)
}

func TestHasherTestSuite(t *testing.T) {
	suite.Run(t, new(HasherTestSuite))
}
