package serializer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var ctx = context.Background()

type SerializerTestSuite struct {
	suite.Suite
	s *Serializer
}

func (s *SerializerTestSuite) SetupTest() {
	comparer := comparer.NewComparer()
	docFac := data.NewDocument
	s.s = NewSerializer(comparer, docFac).(*Serializer)
}

// Can serialize strings.
func (s *SerializerTestSuite) TestString() {
	a := data.M{"test": "Some string"}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal("Some string", r["test"])

}

// Can serialize booleans.
func (s *SerializerTestSuite) TestBool() {
	a := data.M{"test1": true, "test2": false}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.True(true, r["test1"])
	s.False(false, r["test2"])
}

// Can serialize numbers.
func (s *SerializerTestSuite) TestNumber() {
	a := data.M{"test1": 5, "test2": 6.2}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(5.0, r["test1"])
	s.Equal(6.2, r["test2"])
}

// Can serialize null.
func (s *SerializerTestSuite) TestNil() {
	a := data.M{"test": nil}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(nil, r["test"])
}

// Can serialize time.Time.
func (s *SerializerTestSuite) TestDate() {
	d := time.Now()

	a := data.M{"test": d}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(map[string]any{"$$date": float64(d.UnixMilli())}, r["test"])
}

// Can serialize sub objects.
func (s *SerializerTestSuite) TestNestedDocs() {
	d := time.Now()

	a := data.M{"test": data.M{"something": 39, "also": d, "yes": data.M{"again": "yes"}}}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r data.M
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(float64(d.UnixMilli()), r.D("test").D("also").Get("$$date"))
	s.Equal("yes", r.D("test").D("yes").Get("again"))
}

// Can serialize sub arrays.
func (s *SerializerTestSuite) TestNestedSlices() {
	d := time.Now()

	a := data.M{"test": []any{39, d, data.M{"again": "yes"}}}
	b, err := s.s.Serialize(ctx, a)
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(39.0, r["test"].([]any)[0])
	s.Equal(float64(d.UnixMilli()), r["test"].([]any)[1].(map[string]any)["$$date"])
	s.Equal("yes", r["test"].([]any)[2].(map[string]any)["again"])
}

// Reject field names beginning with a $ sign or containing a dot, except the
// four edge cases.
func (s *SerializerTestSuite) TestRejectBadDollarFields() {
	a := []data.M{
		{"$$date": nil},
		{"$$date": "nope"},
		{"$$deleted": nil},
		{"$$deleted": "nope"},
		{"$something": "totest"},
		{"with.dot": "totest"},
	}
	e := []data.M{
		{"$$date": 4321},
		{"$$deleted": true},
		{"$$indexCreated": "indexName"},
		{"$$indexRemoved": "indexName"},
	}

	for _, v := range a {
		_, err := s.s.Serialize(ctx, v)
		s.ErrorAs(err, &domain.ErrFieldName{})
	}

	for _, v := range e {
		_, err := s.s.Serialize(ctx, v)
		s.NoError(err)
	}

}

// Can serialize strings despite of line breaks.
func (s *SerializerTestSuite) TestStringWithLineBreak() {
	badString := "world\r\nearth\nother\rline"
	a := data.M{"test": badString}
	b, err := s.s.Serialize(ctx, a)
	s.NotContains(b, byte('\n'))
	s.NoError(err)
	var r map[string]any
	s.NoError(json.Unmarshal(b, &r))
	s.Equal(badString, r["test"])
}

// NOTE: No tests for numeric keys in serialization. JavaScript-only feature
// not worth the development cost and performance impact.

// Serialization with a canceled context should fail.
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

// Serialization should not succeed if document factory fails at least once.
func (s *SerializerTestSuite) TestInvalidDocFactory() {
	count := 0

	errDocFac := errors.New("errDocFac error")

	// failing on second call so it can fail at nested doc in copyAny
	s.s.documentFactory = func(any) (domain.Document, error) {
		if count == 0 {
			count++
			return data.M{}, nil
		}
		return nil, errDocFac
	}
	a := data.M{"test": []any{data.M{}}}
	b, err := s.s.Serialize(ctx, a)
	s.ErrorIs(err, errDocFac)
	s.Nil(b)
}

func TestSerializerTestSuite(t *testing.T) {
	suite.Run(t, new(SerializerTestSuite))
}
