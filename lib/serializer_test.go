package lib

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb"
)

type SerializerTestSuite struct {
	suite.Suite
	s *Serializer
}

func (s *SerializerTestSuite) SetupTest() {
	comparer := NewComparer()
	docFac := NewDocument
	s.s = NewSerializer(comparer, docFac).(*Serializer)
}

// Can serialize strings
func (s *SerializerTestSuite) TestString() {
	a := Document{"test": "Some string"}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal("Some string", r["test"])

}

// Can serialize booleans
func (s *SerializerTestSuite) TestBool() {
	a := Document{"test1": true, "test2": false}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.True(true, r["test1"])
	s.False(false, r["test2"])
}

// Can serialize numbers
func (s *SerializerTestSuite) TestNumber() {
	a := Document{"test1": 5, "test2": 6.2}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(5.0, r["test1"])
	s.Equal(6.2, r["test2"])
}

// Can serialize null
func (s *SerializerTestSuite) TestNil() {
	a := Document{"test": nil}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(nil, r["test"])
}

// Can serialize time.Time
func (s *SerializerTestSuite) TestDate() {
	d := time.Now()

	a := Document{"test": d}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(map[string]any{"$$date": float64(d.UnixMilli())}, r["test"])
}

// Can serialize sub objects
func (s *SerializerTestSuite) TestNestedDocs() {
	d := time.Now()

	a := Document{"test": Document{"something": 39, "also": d, "yes": Document{"again": "yes"}}}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r Document
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(float64(d.UnixMilli()), r.D("test").D("also").Get("$$date"))
	s.Equal("yes", r.D("test").D("yes").Get("again"))
}

// Can serialize sub arrays
func (s *SerializerTestSuite) TestNestedSlices() {
	d := time.Now()

	a := Document{"test": []any{39, d, Document{"again": "yes"}}}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(39.0, r["test"].([]any)[0])
	s.Equal(float64(d.UnixMilli()), r["test"].([]any)[1].(map[string]any)["$$date"])
	s.Equal("yes", r["test"].([]any)[2].(map[string]any)["again"])
}

// Reject field names beginning with a $ sign or containing a dot, except the four edge cases
func (s *SerializerTestSuite) TestRejectBadDollarFields() {
	a := []Document{
		{"$$date": nil},
		{"$$date": "nope"},
		{"$$deleted": nil},
		{"$$deleted": "nope"},
		{"$something": "totest"},
		{"with.dot": "totest"},
	}
	e := []Document{
		{"$$date": 4321},
		{"$$deleted": true},
		{"$$indexCreated": "indexName"},
		{"$$indexRemoved": "indexName"},
	}

	for _, v := range a {
		_, err := s.s.Serialize(ctx, v)
		s.Error(err)
	}

	for _, v := range e {
		_, err := s.s.Serialize(ctx, v)
		s.NoError(err)
	}

}

// Can serialize strings despite of line breaks
func (s *SerializerTestSuite) TestStringWithLineBreak() {
	badString := "world\r\nearth\nother\rline"
	a := Document{"test": badString}
	b, err := s.s.Serialize(ctx, a)
	s.NotContains(b, byte('\n'))
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(badString, r["test"])
}

// NOTE: No tests for numeric keys in serialization. JavaScript-only feature
// not worth the development cost and performance impact.

// Serialization with a canceled context should fail
func (s *SerializerTestSuite) TestContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b, err := s.s.Serialize(ctx, nil)
	s.ErrorIs(err, context.Canceled)
	s.ErrorIs(context.Cause(ctx), context.Canceled)
	s.Nil(b)

	ctx, cancel = context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	time.Sleep(2 * time.Nanosecond)
	b, err = s.s.Serialize(ctx, nil)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Nil(b)
}

// Serialization should not succeed if document factory fails at least once
func (s *SerializerTestSuite) TestInvalidDocFactory() {
	count := 0
	// failing on second call so it can fail at nested doc in copyAny
	s.s.documentFactory = func(any) (gedb.Document, error) {
		if count == 0 {
			count++
			return Document{}, nil
		}
		return nil, errors.New("some error")
	}
	a := Document{"test": []any{Document{}}}
	b, err := s.s.Serialize(ctx, a)
	s.Error(err)
	s.Nil(b)
}

func TestSerializerTestSuite(t *testing.T) {
	suite.Run(t, new(SerializerTestSuite))
}
