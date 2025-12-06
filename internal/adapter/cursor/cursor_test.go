package cursor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

type M = data.M

type decoderMock struct{ mock.Mock }

// Decode implements [domain.Decoder].
func (d *decoderMock) Decode(src any, tgt any) error {
	return d.Called(src, tgt).Error(0)
}

type Obj struct {
	A int
}

type CursorTestSuite struct {
	suite.Suite
	data []domain.Document
}

func (s *CursorTestSuite) SetupSuite() {
	s.data = make([]domain.Document, 1000)
	for n := range 1000 {
		s.data[n] = M{"a": n}
	}
}

func (s *CursorTestSuite) TestNilData() {
	cur, err := NewCursor(context.Background(), nil)
	s.NoError(err)
	count := 0
	for cur.Next() {
		count++
	}
	s.Zero(count)
	s.NoError(cur.Err())
}

func (s *CursorTestSuite) TestNoData() {
	cur, err := NewCursor(context.Background(), []domain.Document{})
	s.NoError(err)
	count := 0
	for cur.Next() {
		count++
	}
	s.Zero(count)
	s.NoError(cur.Err())
}

func (s *CursorTestSuite) TestStructs() {

	cur, err := NewCursor(context.Background(), s.data)
	s.NoError(err)

	count := 0
	for cur.Next() {
		var obj Obj
		err := cur.Scan(context.Background(), &obj)
		s.NoError(err)
		s.Equal(count, obj.A)
		count++
	}
	s.Equal(1000, count)
	s.NoError(cur.Err())
}

func (s *CursorTestSuite) TestReadClosed() {
	cur, err := NewCursor(context.Background(), s.data)
	s.NoError(err)

	s.NoError(cur.Close())

	s.False(cur.Next())

	s.ErrorIs(cur.Err(), domain.ErrCursorClosed)
}

func (s *CursorTestSuite) TestCreateClosedContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cur, err := NewCursor(ctx, s.data)
	s.ErrorIs(err, context.Canceled)
	s.Nil(cur)
}

func (s *CursorTestSuite) TestCloseAfterCreation() {
	ctx, cancel := context.WithCancel(context.Background())

	cur, err := NewCursor(ctx, s.data)
	s.NoError(err)
	s.NotNil(cur)

	cancel()
	<-ctx.Done()

	count := 0
	for cur.Next() {
		count++
	}
	s.Equal(0, count)
}

func (s *CursorTestSuite) TestCloseBeforeScan() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cur, err := NewCursor(ctx, s.data)
	s.NoError(err)
	s.NotNil(cur)

	count := 0
	for cur.Next() {
		var obj Obj

		cancel()
		<-ctx.Done()

		err := cur.Scan(context.Background(), &obj)
		s.ErrorIs(err, context.Canceled)
		count++
	}
	s.Equal(1, count)
}

func (s *CursorTestSuite) TestScanClosedContext() {
	ctx := context.Background()

	cur, err := NewCursor(ctx, s.data)
	s.NoError(err)
	s.NotNil(cur)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	<-ctx.Done()

	count := 0
	for cur.Next() {
		var obj Obj
		err := cur.Scan(ctx, &obj)
		s.ErrorIs(err, context.Canceled)
		count++
	}
	s.Equal(1000, count)
}

func (s *CursorTestSuite) TestScanWithoutNext() {
	ctx := context.Background()

	cur, err := NewCursor(ctx, s.data)
	s.NoError(err)
	s.NotNil(cur)

	err = cur.Scan(context.Background(), new(struct{}))
	s.ErrorIs(err, domain.ErrScanBeforeNext)
}

func (s *CursorTestSuite) TestCloseClosed() {
	ctx := context.Background()

	cur, err := NewCursor(ctx, s.data)
	s.NoError(err)
	s.NotNil(cur)

	s.NoError(cur.Close())
	s.ErrorIs(cur.Close(), domain.ErrCursorClosed)
}

func (s *CursorTestSuite) TestCustomDecoder() {
	ctx := context.Background()

	dec := new(decoderMock)

	dec.On("Decode", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			args[1].(*Obj).A = -1
		}).
		Return(nil)

	cur, err := NewCursor(ctx, s.data, domain.WithCursorDecoder(dec))
	s.NoError(err)
	s.NotNil(cur)

	for cur.Next() {
		var obj Obj
		s.NoError(cur.Scan(context.Background(), &obj))
		s.Equal(-1, obj.A)
	}

	dec.AssertExpectations(s.T())
}

func TestCursorTestSuite(t *testing.T) {
	suite.Run(t, new(CursorTestSuite))
}
