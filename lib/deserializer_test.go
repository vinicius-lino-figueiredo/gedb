package lib

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type DeserializerTestSuite struct {
	suite.Suite
	d *Deserializer
}

func (s *DeserializerTestSuite) SetupTest() {
	decoder := NewDecoder()
	s.d = NewDeserializer(decoder).(*Deserializer)
}

// Can deserialize strings
func (s *DeserializerTestSuite) TestString() {
	a := Document{"test": "Some string"}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal("Some string", r["test"])

}

// Can deserialize booleans
func (s *DeserializerTestSuite) TestBool() {
	a := Document{"test1": true, "test2": false}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.True(true, r["test1"])
	s.False(false, r["test2"])
}

// Can deserialize numbers
func (s *DeserializerTestSuite) TestNumber() {
	a := Document{"test1": 5, "test2": 6.2}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(5.0, r["test1"])
	s.Equal(6.2, r["test2"])
}

// Can deserialize null
func (s *DeserializerTestSuite) TestNil() {
	a := Document{"test": nil}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(nil, r["test"])
}

// Can deserialize time.Time
func (s *DeserializerTestSuite) TestDate() {
	d := time.Now()

	a := Document{"test": Document{"$$date": d.UnixMilli()}}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(d.Truncate(time.Millisecond), r["test"])
}

// Can deserialize sub objects
func (s *DeserializerTestSuite) TestNestedDocs() {
	d := time.Now()

	a := Document{"test": Document{"something": 39, "also": Document{"$$date": d.UnixMilli()}, "yes": Document{"again": "yes"}}}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r Document
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(d.Truncate(time.Millisecond), r.D("test").Get("also"))
	s.Equal("yes", r.D("test").D("yes").Get("again"))
}

// Can deserialize sub arrays
func (s *DeserializerTestSuite) TestNestedSlices() {
	d := time.Now()

	a := Document{"test": []any{39, Document{"$$date": d.UnixMilli()}, Document{"again": "yes"}}}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(39.0, r["test"].([]any)[0])
	s.Equal(d.Truncate(time.Millisecond), r["test"].([]any)[1])
	s.Equal("yes", r["test"].([]any)[2].(Document)["again"])
}

// Can deserialize strings despite of line breaks
func (s *DeserializerTestSuite) TestStringWithLineBreak() {
	badString := "world\r\nearth\nother\rline"
	a := Document{"test": badString}
	b, err := json.Marshal(a)
	s.NotContains(b, byte('\n'))
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(badString, r["test"])
}

// Deserialization with a canceled context should fail
func (s *DeserializerTestSuite) TestContext() {

	b, err := json.Marshal(Document{"hello": "world"})
	s.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())

	var v1 any

	err = s.d.Deserialize(ctx, b, &v1)
	s.NoError(err)
	s.Equal(Document{"hello": "world"}, v1)

	cancel()

	var v2 any

	err = s.d.Deserialize(ctx, b, &v2)
	s.ErrorIs(err, context.Canceled)
	s.Nil(v2)

	ctx, cancel = context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	time.Sleep(2 * time.Nanosecond)

	var v3 any

	err = s.d.Deserialize(ctx, b, &v3)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Nil(v3)
}

// Deserialize returns error if target is nil
func (s *DeserializerTestSuite) TestNilTarget() {
	err := s.d.Deserialize(ctx, []byte(`{"a":1}`), nil)
	s.Error(err)
}

func TestDeserializerTestSuite(t *testing.T) {
	suite.Run(t, new(DeserializerTestSuite))
}
