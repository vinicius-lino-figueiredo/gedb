package deserializer

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var ctx = context.Background()

type DeserializerTestSuite struct {
	suite.Suite
	d *Deserializer
}

func (s *DeserializerTestSuite) SetupTest() {
	decoder := decoder.NewDecoder()
	s.d = NewDeserializer(decoder).(*Deserializer)
}

// Can deserialize strings.
func (s *DeserializerTestSuite) TestString() {
	a := data.M{"test": "Some string"}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal("Some string", r["test"])

}

// Can deserialize booleans.
func (s *DeserializerTestSuite) TestBool() {
	a := data.M{"test1": true, "test2": false}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.True(true, r["test1"])
	s.False(false, r["test2"])
}

// Can deserialize numbers.
func (s *DeserializerTestSuite) TestNumber() {
	a := data.M{"test1": 5, "test2": 6.2}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(5.0, r["test1"])
	s.Equal(6.2, r["test2"])
}

// Can deserialize null.
func (s *DeserializerTestSuite) TestNil() {
	a := data.M{"test": nil}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(nil, r["test"])
}

// Can deserialize time.Time.
func (s *DeserializerTestSuite) TestDate() {
	d := time.Now()

	a := data.M{"test": data.M{"$$date": d.UnixMilli()}}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(d.Truncate(time.Millisecond), r["test"])
}

// Can deserialize sub objects.
func (s *DeserializerTestSuite) TestNestedDocs() {
	d := time.Now()

	a := data.M{"test": data.M{"something": 39, "also": data.M{"$$date": d.UnixMilli()}, "yes": data.M{"again": "yes"}}}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r data.M
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(d.Truncate(time.Millisecond), r.D("test").Get("also"))
	s.Equal("yes", r.D("test").D("yes").Get("again"))
}

// Can deserialize sub arrays.
func (s *DeserializerTestSuite) TestNestedSlices() {
	d := time.Now()

	a := data.M{"test": []any{39, data.M{"$$date": d.UnixMilli()}, data.M{"again": "yes"}}}
	b, err := json.Marshal(a)
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(39.0, r["test"].([]any)[0])
	s.Equal(d.Truncate(time.Millisecond), r["test"].([]any)[1])
	s.Equal("yes", r["test"].([]any)[2].(data.M)["again"])
}

// Can deserialize strings despite of line breaks.
func (s *DeserializerTestSuite) TestStringWithLineBreak() {
	badString := "world\r\nearth\nother\rline"
	a := data.M{"test": badString}
	b, err := json.Marshal(a)
	s.NotContains(b, byte('\n'))
	s.NoError(err)
	var r map[string]any
	s.NoError(s.d.Deserialize(ctx, b, &r))
	s.Equal(badString, r["test"])
}

// Deserialization with a canceled context should fail.
func (s *DeserializerTestSuite) TestContext() {

	b, err := json.Marshal(data.M{"hello": "world"})
	s.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())

	var v1 any

	err = s.d.Deserialize(ctx, b, &v1)
	s.NoError(err)
	s.Equal(map[string]any{"hello": "world"}, v1)

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

// Deserialize returns error if target is nil.
func (s *DeserializerTestSuite) TestNilTarget() {
	err := s.d.Deserialize(ctx, []byte(`{"a":1}`), nil)
	s.ErrorIs(err, domain.ErrTargetNil)
}

func (s *DeserializerTestSuite) TestInvalidSyntax() {
	target := data.M{}
	err := s.d.Deserialize(context.Background(), []byte("{"), &target)
	s.ErrorIs(err, io.ErrUnexpectedEOF)
}

func TestDeserializerTestSuite(t *testing.T) {
	suite.Run(t, new(DeserializerTestSuite))
}
