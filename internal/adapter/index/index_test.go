package index

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

type comparerMock struct{ mock.Mock }

// Comparable implements [domain.Comparer].
func (c *comparerMock) Comparable(a any, b any) bool {
	return c.Called(a, b).Bool(0)
}

// Compare implements [domain.Comparer].
func (c *comparerMock) Compare(a any, b any) (int, error) {
	call := c.Called(a, b)
	return call.Int(0), call.Error(1)
}

type contextMock struct{ mock.Mock }

// Deadline implements [context.Context].
func (c *contextMock) Deadline() (deadline time.Time, ok bool) {
	call := c.Called()
	return call.Get(0).(time.Time), call.Bool(1)
}

// Done implements [context.Context].
func (c *contextMock) Done() <-chan struct{} {
	return c.Called().Get(0).(<-chan struct{})
}

// Err implements [context.Context].
func (c *contextMock) Err() error {
	return c.Called().Error(0)
}

// Value implements [context.Context].
func (c *contextMock) Value(key any) any {
	return c.Called(key).Get(0)
}

// fieldNavigatorMock implements [domain.FieldNavigator].
type fieldNavigatorMock struct {
	mock.Mock
}

// EnsureField implements [domain.FieldNavigator].
func (f *fieldNavigatorMock) EnsureField(obj any, addr ...string) ([]domain.GetSetter, error) {
	call := f.Called(obj, addr)
	return call.Get(0).([]domain.GetSetter), call.Error(1)
}

// GetAddress implements [domain.FieldNavigator].
func (f *fieldNavigatorMock) GetAddress(field string) ([]string, error) {
	call := f.Called(field)
	return call.Get(0).([]string), call.Error(1)
}

// GetField implements [domain.FieldNavigator].
func (f *fieldNavigatorMock) GetField(obj any, addr ...string) ([]domain.GetSetter, bool, error) {
	call := f.Called(obj, addr)
	return call.Get(0).([]domain.GetSetter), call.Bool(1), call.Error(2)
}

// SplitFields implements [domain.FieldNavigator].
func (f *fieldNavigatorMock) SplitFields(value string) ([]string, error) {
	call := f.Called(value)
	return call.Get(0).([]string), call.Error(1)
}

type hasherMock struct{ mock.Mock }

// Hash implements [domain.Hasher].
func (h *hasherMock) Hash(v any) (uint64, error) {
	call := h.Called(v)
	return uint64(call.Int(0)), call.Error(1)
}

type IndexesTestSuite struct {
	suite.Suite
	comparer domain.Comparer
}

func (s *IndexesTestSuite) SetupSuite() {
	s.comparer = comparer.NewComparer()
}

func (s *IndexesTestSuite) TestNewIndexFailSplitFields() {
	fn := new(fieldNavigatorMock)
	fn.On("SplitFields", "").Return([]string{}, fmt.Errorf("error")).Once()
	idx, err := NewIndex(domain.WithIndexFieldNavigator(fn))
	s.Error(err)
	s.Nil(idx)
}

func (s *IndexesTestSuite) TestInsertion() {
	// Can insert pointers to documents in the index correctly when they have the field
	s.Run("Pointers", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		// The underlying BST now has 3 nodes which contain the docs where it's expected
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{data.M{"a": 5, "tf": "hello"}}, idx.Tree.Search("hello"))
		s.Equal([]any{data.M{"a": 8, "tf": "world"}}, idx.Tree.Search("world"))
		s.Equal([]any{data.M{"a": 2, "tf": "bloup"}}, idx.Tree.Search("bloup"))

		// The nodes contain pointers to the actual documents
		s.Equal(reflect.ValueOf(doc2).Pointer(), reflect.ValueOf(idx.Tree.Search("world")[0].(data.M)).Pointer())
		idx.Tree.Search("bloup")[0].(domain.Document).Set("a", 42)
		s.Equal(42, doc3.Get("a"))
	})

	// Can insert pointers to documents in the index correctly when they have compound fields
	s.Run("PointersCompound", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf,tg"))
		s.Equal("tf,tg", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello", "tg": "world"}
		doc2 := data.M{"a": 8, "tf": "hello", "tg": "bloup"}
		doc3 := data.M{"a": 2, "tf": "bloup", "tg": "bloup"}

		ctx := context.Background()
		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		// The underlying BST now has 3 nodes which contain the docs where it's expected
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{data.M{"a": 5, "tf": "hello", "tg": "world"}}, idx.Tree.Search(data.M{"tf": "hello", "tg": "world"}))
		s.Equal([]any{data.M{"a": 8, "tf": "hello", "tg": "bloup"}}, idx.Tree.Search(data.M{"tf": "hello", "tg": "bloup"}))
		s.Equal([]any{data.M{"a": 2, "tf": "bloup", "tg": "bloup"}}, idx.Tree.Search(data.M{"tf": "bloup", "tg": "bloup"}))

		// The nodes contain pointers to the actual documents
		s.Equal(reflect.ValueOf(doc2).Pointer(), reflect.ValueOf(idx.Tree.Search(data.M{"tf": "hello", "tg": "bloup"})[0].(data.M)).Pointer())
		idx.Tree.Search(data.M{"tf": "bloup", "tg": "bloup"})[0].(domain.Document).Set("a", 42)
		s.Equal(42, doc3.Get("a"))
	})

	// Inserting twice for the same fieldName in a unique index will result in an error thrown
	s.Run("InsertionFieldNameTwice", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.Equal(1, idx.GetNumberOfKeys())
		s.Error(idx.Insert(ctx, doc1))
	})

	// Inserting twice for a fieldName the docs don't have with a unique
	// index results in an error thrown
	s.Run("InsertTwiceNonUnique", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("nope"),
			domain.WithIndexUnique(true),
		)
		s.Equal("nope", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 5, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.Equal(1, idx.GetNumberOfKeys())
		s.Error(idx.Insert(ctx, doc2))
	})

	// Inserting twice for a fieldName the docs don't have with a unique and
	// sparse index will not throw, since the docs will be non indexed
	s.Run("InsertTwiceSparse", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("nope"),
			domain.WithIndexUnique(true),
			domain.WithIndexSparse(true),
		)
		s.Equal("nope", i.FieldName())
		s.True(i.Unique())
		s.True(i.Sparse())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 5, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.Equal(0, idx.GetNumberOfKeys()) // Docs are not indexed
	})

	// Inserting twice for the same compound fieldName in a unique index will result in an error thrown
	s.Run("InsertTwiceSameCompound", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf,tg"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf,tg", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello", "tg": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.Equal(1, idx.GetNumberOfKeys())
		s.Error(idx.Insert(ctx, doc1))
	})

	// Inserting twice for a compound fieldName the docs dont have with a unique and sparse index will not throw, since the docs will be non indexed
	s.Run("InsertTwiceCompoundSparse", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("nope,nopeNope"),
			domain.WithIndexUnique(true),
			domain.WithIndexSparse(true),
		)
		s.Equal("nope,nopeNope", i.FieldName())
		s.True(i.Unique())
		s.True(i.Sparse())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 5, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.Equal(0, idx.GetNumberOfKeys()) // Docs are not indexed
	})

	// Works with dot notation
	s.Run("DotNotation", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf.nested"))
		s.Equal("tf.nested", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": data.M{"nested": "hello"}}
		doc2 := data.M{"a": 8, "tf": data.M{"nested": "world", "additional": true}}
		doc3 := data.M{"a": 2, "tf": data.M{"nested": "bloup", "age": 42}}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		// The underlying BST now has 3 nodes which contain the docs where it's expected
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc2}, idx.Tree.Search("world"))
		s.Equal([]any{doc3}, idx.Tree.Search("bloup"))

		// The nodes contain pointers to the actual documents
		idx.Tree.Search("bloup")[0].(domain.Document).Set("a", 42)
		s.Equal(42, doc3.Get("a"))
	})

	// Can insert an array of documents
	s.Run("ArrayOfDoc", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1, doc2, doc3))
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc2}, idx.Tree.Search("world"))
		s.Equal([]any{doc3}, idx.Tree.Search("bloup"))
	})

	// When inserting an array of elements, if an error is thrown all inserts need to be rolled back
	s.Run("ArrayRollback", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc2b := data.M{"a": 84, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		err = idx.Insert(ctx, doc1, doc2, doc2b, doc3)
		e := &bst.ErrViolated{}
		s.ErrorAs(err, &e)

		s.Equal(0, idx.GetNumberOfKeys())
		s.Equal([]any{}, idx.Tree.Search("hello"))
		s.Equal([]any{}, idx.Tree.Search("world"))
		s.Equal([]any{}, idx.Tree.Search("bloup"))
	})

	s.Run("ArrayFields", func() {
		// Inserts one entry per array element in the index
		s.Run("OneEntryPerElement", func() {
			obj := data.M{"tf": []any{"aa", "bb"}, "really": "yeah"}
			obj2 := data.M{"tf": "normal", "yes": "indeed"}
			i, err := NewIndex(domain.WithIndexFieldName("tf"))
			s.Equal("tf", i.FieldName())
			s.NoError(err)
			idx := i.(*Index)

			ctx := context.Background()

			s.NoError(idx.Insert(ctx, obj))
			s.Len(idx.GetAll(), 2)
			s.Equal(obj, idx.GetAll()[0])
			s.Equal(obj, idx.GetAll()[1])

			s.NoError(idx.Insert(ctx, obj2))
			s.Len(idx.GetAll(), 3)
		})

		// Inserts one entry per array element in the index, type-checked
		s.Run("OneEntryPerElementTypeChecked", func() {
			obj := data.M{"tf": []any{"42", int64(42), time.Unix(42, 0), int64(42)}, "really": "yeah"}
			i, err := NewIndex(domain.WithIndexFieldName("tf"))
			s.Equal("tf", i.FieldName())
			s.NoError(err)
			idx := i.(*Index)

			ctx := context.Background()

			s.NoError(idx.Insert(ctx, obj))
			s.Equal(obj, idx.GetAll()[0])
			s.Equal(obj, idx.GetAll()[1])
			s.Equal(obj, idx.GetAll()[2])
		})

		// Inserts one entry per unique array element in the index, the unique constraint only holds across documents
		s.Run("OnePerUniqueElement", func() {
			obj := data.M{"tf": []any{"aa", "aa"}, "really": "yeah"}
			obj2 := data.M{"tf": []any{"cc", "yy", "cc"}, "yes": "indeed"}
			i, err := NewIndex(
				domain.WithIndexFieldName("tf"),
				domain.WithIndexUnique(true),
			)
			s.Equal("tf", i.FieldName())
			s.True(i.Unique())
			s.NoError(err)
			idx := i.(*Index)

			ctx := context.Background()

			s.NoError(idx.Insert(ctx, obj))
			s.Len(idx.GetAll(), 1)
			s.Equal(obj, idx.GetAll()[0])

			s.NoError(idx.Insert(ctx, obj2))
			s.Len(idx.GetAll(), 3)
		})

		//  The unique constraint holds across documents
		s.Run("UniqueHeldAcrossDocuments", func() {
			obj := data.M{"tf": []any{"aa", "aa"}, "really": "yeah"}
			obj2 := data.M{"tf": []any{"cc", "aa", "cc"}, "yes": "indeed"}
			i, err := NewIndex(
				domain.WithIndexFieldName("tf"),
				domain.WithIndexUnique(true),
			)
			s.Equal("tf", i.FieldName())
			s.True(i.Unique())
			s.NoError(err)
			idx := i.(*Index)

			ctx := context.Background()

			s.NoError(idx.Insert(ctx, obj))
			s.Len(idx.GetAll(), 1)
			s.Equal(obj, idx.GetAll()[0])

			s.Error(idx.Insert(ctx, obj2))
		})

		// When removing a document, remove it from the index at all unique array elements
		s.Run("RemoveIndexAtAllUniqueElements", func() {
			obj := data.M{"tf": []any{"aa", "aa"}, "really": "yeah"}
			obj2 := data.M{"tf": []any{"cc", "aa", "cc"}, "yes": "indeed"}
			i, err := NewIndex(domain.WithIndexFieldName("tf"))
			s.Equal("tf", i.FieldName())
			s.NoError(err)
			idx := i.(*Index)

			ctx := context.Background()

			s.NoError(idx.Insert(ctx, obj))
			s.NoError(idx.Insert(ctx, obj2))
			aaMatches, err := idx.GetMatching("aa")
			s.NoError(err)
			s.Len(aaMatches, 2)
			s.NotEqual(-1, slices.IndexFunc(aaMatches, func(d domain.Document) bool { return s.compareThings(obj, d) == 0 }))
			s.NotEqual(-1, slices.IndexFunc(aaMatches, func(d domain.Document) bool { return s.compareThings(obj2, d) == 0 }))
			ccMatches, err := idx.GetMatching("cc")
			s.NoError(err)
			s.Len(ccMatches, 1)

			s.NoError(idx.Remove(ctx, obj2))
			aaMatches, err = idx.GetMatching("aa")
			s.NoError(err)
			s.Len(aaMatches, 1)
			s.NotEqual(-1, slices.IndexFunc(aaMatches, func(d domain.Document) bool { return s.compareThings(obj, d) == 0 }))
			s.Equal(-1, slices.IndexFunc(aaMatches, func(d domain.Document) bool { return s.compareThings(obj2, d) == 0 }))
			ccMatches, err = idx.GetMatching("cc")
			s.NoError(err)
			s.Len(ccMatches, 0)
		})

		// If a unique constraint is violated when inserting an array key, roll back all inserts before the key', function () {
		s.Run("RollBackAllOnConstraintViolated", func() {
			obj := data.M{"tf": []any{"aa", "bb"}, "really": "yeah"}
			obj2 := data.M{"tf": []any{"cc", "dd", "aa", "ee"}, "yes": "indeed"}
			i, err := NewIndex(
				domain.WithIndexFieldName("tf"),
				domain.WithIndexUnique(true),
			)
			s.Equal("tf", i.FieldName())
			s.True(i.Unique())
			s.NoError(err)
			idx := i.(*Index)

			ctx := context.Background()

			s.NoError(idx.Insert(ctx, obj))
			s.Len(idx.GetAll(), 2)
			aaMatches, err := idx.GetMatching("aa")
			s.NoError(err)
			s.Len(aaMatches, 1)
			bbMatches, err := idx.GetMatching("bb")
			s.NoError(err)
			s.Len(bbMatches, 1)
			ccMatches, err := idx.GetMatching("cc")
			s.NoError(err)
			s.Len(ccMatches, 0)
			ddMatches, err := idx.GetMatching("dd")
			s.NoError(err)
			s.Len(ddMatches, 0)
			eeMatches, err := idx.GetMatching("ee")
			s.NoError(err)
			s.Len(eeMatches, 0)

			s.Error(idx.Insert(ctx, obj2))
			s.Len(idx.GetAll(), 2)
			aaMatches, err = idx.GetMatching("aa")
			s.NoError(err)
			s.Len(aaMatches, 1)
			bbMatches, err = idx.GetMatching("bb")
			s.NoError(err)
			s.Len(bbMatches, 1)
			ccMatches, err = idx.GetMatching("cc")
			s.NoError(err)
			s.Len(ccMatches, 0)
			ddMatches, err = idx.GetMatching("dd")
			s.NoError(err)
			s.Len(ddMatches, 0)
			eeMatches, err = idx.GetMatching("ee")
			s.NoError(err)
			s.Len(eeMatches, 0)
		})
	}) // ==== End of 'Array fields' ==== //

	s.Run("CompoundIndexes", func() {
		// Supports field names separated by commas
		s.Run("SupportFieldNameSeparatedByComma", func() {
			i, err := NewIndex(domain.WithIndexFieldName("tf,tf2"))
			s.Equal("tf,tf2", i.FieldName())
			s.NoError(err)
			idx := i.(*Index)
			doc1 := data.M{"a": int64(5), "tf": "hello", "tf2": int64(7)}
			doc2 := data.M{"a": int64(8), "tf": "hello", "tf2": int64(6)}
			doc3 := data.M{"a": int64(2), "tf": "bloup", "tf2": int64(3)}

			ctx := context.Background()

			s.NoError(idx.Insert(ctx, doc1))
			s.NoError(idx.Insert(ctx, doc2))
			s.NoError(idx.Insert(ctx, doc3))

			// The underlying BST now has 3 nodes which contain the docs where it's expected
			s.Equal(3, idx.GetNumberOfKeys())
			s.Equal([]any{data.M{"a": int64(5), "tf": "hello", "tf2": int64(7)}}, idx.Tree.Search(data.M{"tf": "hello", "tf2": int64(7)}))
			s.Equal([]any{data.M{"a": int64(8), "tf": "hello", "tf2": int64(6)}}, idx.Tree.Search(data.M{"tf": "hello", "tf2": int64(6)}))
			s.Equal([]any{data.M{"a": int64(2), "tf": "bloup", "tf2": int64(3)}}, idx.Tree.Search(data.M{"tf": "bloup", "tf2": int64(3)}))

			// The nodes contain pointers to the actual documents
			s.Equal(doc2, idx.Tree.Search(data.M{"tf": "hello", "tf2": int64(6)})[0])
			idx.Tree.Search(data.M{"tf": "bloup", "tf2": int64(3)})[0].(domain.Document).Set("a", 42)
			s.Equal(42, doc3.Get("a"))
		})
	})

	s.Run("GetKeysError", func() {
		fn := new(fieldNavigatorMock)

		fn.On("SplitFields", "tf").Return([]string{"tf"}, nil).Once()

		i, err := NewIndex(
			domain.WithIndexFieldNavigator(fn),
			domain.WithIndexFieldName("tf"),
		)
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		fn.AssertExpectations(s.T())

		e := fmt.Errorf("error")

		fn.On("GetAddress", "tf").Return([]string{}, e).Once()

		s.ErrorIs(i.Insert(context.Background(), data.M{"tf": 1}), e)
		fn.AssertExpectations(s.T())
	})

	s.Run("FailedHasing", func() {

		h := new(hasherMock)

		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexHasher(h),
		)
		s.Equal("tf", i.FieldName())
		s.NoError(err)

		doc := data.M{"tf": 1}

		e := fmt.Errorf("error")

		h.On("Hash", 1).Return(0, e).Once()

		s.ErrorIs(i.Insert(context.Background(), doc), e)
		s.Zero(i.GetNumberOfKeys())

	})

} // ==== End of 'Insertion' ==== //

func (s *IndexesTestSuite) TestRemoval() {
	// Can remove pointers from the index, even when multiple documents have the same key
	s.Run("PointersMultipleDocsSameKey", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		doc4 := data.M{"a": 23, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.NoError(idx.Insert(ctx, doc4))
		s.Equal(3, idx.GetNumberOfKeys())

		s.NoError(idx.Remove(ctx, doc1))
		s.Equal(2, idx.GetNumberOfKeys())
		s.Len(idx.Tree.Search("hello"), 0)

		s.NoError(idx.Remove(ctx, doc2))
		s.Equal(2, idx.GetNumberOfKeys())
		s.Len(idx.Tree.Search("world"), 1)
		s.Equal(doc4, idx.Tree.Search("world")[0])
	})

	// If we have a sparse index, removing a non indexed doc has no effect
	s.Run("SparseIndexNonIndexedDoc", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("nope"),
			domain.WithIndexSparse(true),
		)
		s.Equal("nope", i.FieldName())
		s.True(i.Sparse())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 5, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.Equal(0, idx.GetNumberOfKeys())

		s.NoError(idx.Remove(ctx, doc1))
		s.Equal(0, idx.GetNumberOfKeys())
	})

	// Works with dot notation
	s.Run("DotNotation", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf.nested"))
		s.Equal("tf.nested", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": data.M{"nested": "hello"}}
		doc2 := data.M{"a": 8, "tf": data.M{"nested": "world", "additional": true}}
		doc3 := data.M{"a": 2, "tf": data.M{"nested": "bloup", "age": 42}}
		doc4 := data.M{"a": 2, "tf": data.M{"nested": "world", "fruits": []any{"apple", "carrot"}}}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.NoError(idx.Insert(ctx, doc4))
		s.Equal(3, idx.GetNumberOfKeys())

		s.NoError(idx.Remove(ctx, doc1))
		s.Equal(2, idx.GetNumberOfKeys())
		s.Len(idx.Tree.Search("hello"), 0)

		s.NoError(idx.Remove(ctx, doc2))
		s.Equal(2, idx.GetNumberOfKeys())
		s.Len(idx.Tree.Search("world"), 1)
		s.Equal(doc4, idx.Tree.Search("world")[0])
	})

	// Can remove an array of documents
	s.Run("ArrayOfDocuments", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1, doc2, doc3))
		s.Equal(3, idx.GetNumberOfKeys())
		s.NoError(idx.Remove(ctx, doc1, doc3))
		s.Equal(1, idx.GetNumberOfKeys())
		s.Equal([]any{}, idx.Tree.Search("hello"))
		s.Equal([]any{doc2}, idx.Tree.Search("world"))
		s.Equal([]any{}, idx.Tree.Search("bloup"))
	})

	// Can remove from multi field
	// s.Run("ArrayOfDocuments", func() {
	// 	idx := NewIndex(domain.WithIndexFieldName("tf,a")).(*Index)
	// 	doc1 := document.Document{"a": 5, "tf": "hello"}
	// 	doc2 := document.Document{"a": 8, "tf": "world"}
	// 	doc3 := document.Document{"a": 2, "tf": "bloup"}
	//
	// 	ctx := context.Background()
	//
	// 	s.NoError(idx.Insert(ctx, doc1, doc2, doc3))
	// 	s.Equal(3, idx.tree.GetNumberOfKeys())
	// 	s.NoError(idx.Remove(ctx, doc1, doc3))
	// 	s.Equal(1, idx.tree.GetNumberOfKeys())
	// 	s.Equal([]any{}, idx.tree.Search("hello"))
	// 	s.Equal([]any{doc2}, idx.tree.Search("world"))
	// 	s.Equal([]any{}, idx.tree.Search("bloup"))
	// })

	s.Run("SparseNoValid", func() {
		i, err := NewIndex(
			domain.WithIndexSparse(true),
			domain.WithIndexFieldName("tf"),
		)
		s.Equal("tf", i.FieldName())
		s.NoError(err)

		s.NoError(i.Insert(context.Background(), data.M{"tg": 1}))
		s.NoError(i.Remove(context.Background(), data.M{"tf": 1}))

	})

} // ==== End of 'Removal' ==== //

func (s *IndexesTestSuite) TestRemoveFailedNavigation() {
	fn := new(fieldNavigatorMock)
	fn.On("SplitFields", "tf").Return([]string{"tf"}, nil).Once()
	idx, err := NewIndex(
		domain.WithIndexFieldName("tf"),
		domain.WithIndexFieldNavigator(fn),
	)
	s.NoError(err)

	s.Run("GetAddress", func() {
		expectedErr := fmt.Errorf("error")
		fn.On("GetAddress", "tf").Return([]string{}, expectedErr).Once()

		err := idx.(*Index).Remove(context.Background(), nil)
		s.ErrorIs(err, expectedErr)

		fn.AssertExpectations(s.T())
	})

	s.Run("GetField", func() {
		expectedErr := fmt.Errorf("error")
		fn.On("GetAddress", "tf").Return([]string{"tf"}, nil).Once()
		fn.On("GetField", data.M{"doc": false}, []string{"tf"}).
			Return([]domain.GetSetter{}, false, expectedErr).
			Once()

		err := idx.(*Index).Remove(context.Background(), data.M{"doc": false})
		s.ErrorIs(err, expectedErr)

		fn.AssertExpectations(s.T())
	})
}

func (s *IndexesTestSuite) TestUpdate() {
	s.Run("UpdateChangedOrUnchangedKey", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		doc4 := data.M{"a": 23, "tf": "world"}
		doc5 := data.M{"a": 1, "tf": "changed"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{doc2}, idx.Tree.Search("world"))

		s.NoError(idx.Update(ctx, doc2, doc4))
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{doc4}, idx.Tree.Search("world"))

		s.NoError(idx.Update(ctx, doc1, doc5))
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{}, idx.Tree.Search("hello"))
		s.Equal([]any{doc5}, idx.Tree.Search("changed"))
	})

	// If a simple update violates a unique constraint, changes are rolled back and an error thrown
	s.Run("RollbackAndError", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		bad := data.M{"a": 23, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))

		e := &bst.ErrViolated{}
		s.ErrorAs(idx.Update(ctx, doc3, bad), &e)

		// No change
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
	})

	// Can update an array of documents
	s.Run("ArrayOfDocuments", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		doc1b := data.M{"a": 23, "tf": "world"}
		doc2b := data.M{"a": 1, "tf": "changed"}
		doc3b := data.M{"a": 44, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.Equal(3, idx.GetNumberOfKeys())

		s.NoError(idx.UpdateMultipleDocs(ctx, []domain.Update{{OldDoc: doc1, NewDoc: doc1b}, {OldDoc: doc2, NewDoc: doc2b}, {OldDoc: doc3, NewDoc: doc3b}}...))

		s.Equal(3, idx.GetNumberOfKeys())
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 1)
		s.Equal(worldMatches[0], doc1b)
		changedMatches, err := idx.GetMatching("changed")
		s.NoError(err)
		s.Len(changedMatches, 1)
		s.Equal(changedMatches[0], doc2b)
		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)
		s.Equal(blopMatches[0], doc3b)
	})

	// If a unique constraint is violated during an array-update, all changes are rolled back and an error thrown
	s.Run("RollbackArrayUpdate", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		doc1b := data.M{"a": 23, "tf": "changed"}
		doc2b := data.M{"a": 1, "tf": "changed"}
		doc3b := data.M{"a": 44, "tf": "alsochanged"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.Equal(3, idx.GetNumberOfKeys())

		e := &bst.ErrViolated{}
		s.ErrorAs(idx.UpdateMultipleDocs(ctx, []domain.Update{{OldDoc: doc1, NewDoc: doc1b}, {OldDoc: doc2, NewDoc: doc2b}, {OldDoc: doc3, NewDoc: doc3b}}...), &e)

		s.Equal(3, idx.GetNumberOfKeys())
		helloMatches, err := idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 1)
		s.Equal(doc1, helloMatches[0])
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 1)
		s.Equal(doc2, worldMatches[0])
		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)
		s.Equal(doc3, blopMatches[0])

		// Don't know why this is repeated in the original code, but added just in case
		e = &bst.ErrViolated{}
		s.ErrorAs(idx.UpdateMultipleDocs(ctx, []domain.Update{{OldDoc: doc1, NewDoc: doc1b}, {OldDoc: doc2, NewDoc: doc2b}, {OldDoc: doc3, NewDoc: doc3b}}...), &e)

		s.Equal(3, idx.GetNumberOfKeys())
		helloMatches2, err := idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches2, 1)
		s.Equal(doc1, helloMatches2[0])
		worldMatches2, err := idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches2, 1)
		s.Equal(doc2, worldMatches2[0])
		blopMatches2, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches2, 1)
		s.Equal(doc3, blopMatches2[0])
	})

	// If an update doesn't change a document, the unique constraint is not
	// violated
	s.Run("NoChangeNoError", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		noChange := data.M{"a": 8, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{doc2}, idx.Tree.Search("world"))

		s.NoError(idx.Update(ctx, doc2, noChange)) // No error returned
		s.Equal(3, idx.GetNumberOfKeys())
		s.Equal([]any{noChange}, idx.Tree.Search("world"))
	})

	// Can revert simple and batch updates
	s.Run("ReverSimpleAndBatch", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		doc1b := data.M{"a": 23, "tf": "world"}
		doc2b := data.M{"a": 1, "tf": "changed"}
		doc3b := data.M{"a": 44, "tf": "bloup"}
		batchUpdate := []domain.Update{{OldDoc: doc1, NewDoc: doc1b}, {OldDoc: doc2, NewDoc: doc2b}, {
			OldDoc: doc3,
			NewDoc: doc3b,
		}}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.Equal(3, idx.GetNumberOfKeys())

		s.NoError(idx.UpdateMultipleDocs(ctx, batchUpdate...))

		s.Equal(3, idx.GetNumberOfKeys())
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 1)
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Equal(doc1b, worldMatches[0])
		changedMatches, err := idx.GetMatching("changed")
		s.NoError(err)
		s.Len(changedMatches, 1)
		changedMatches, err = idx.GetMatching("changed")
		s.NoError(err)
		s.Equal(doc2b, changedMatches[0])
		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal(doc3b, blopMatches[0])

		s.NoError(idx.RevertMultipleUpdates(ctx, batchUpdate...))

		s.Equal(3, idx.GetNumberOfKeys())
		helloMatches, err := idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 1)
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Equal(doc1, helloMatches[0])
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 1)
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Equal(doc2, worldMatches[0])
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal(doc3, blopMatches[0])

		// Now a simple update
		s.NoError(idx.Update(ctx, doc2, doc2b))

		s.Equal(3, idx.GetNumberOfKeys())
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 1)
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Equal(doc1, helloMatches[0])
		changedMatches, err = idx.GetMatching("changed")
		s.NoError(err)
		s.Len(changedMatches, 1)
		changedMatches, err = idx.GetMatching("changed")
		s.NoError(err)
		s.Equal(doc2b, changedMatches[0])
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal(doc3, blopMatches[0])

		s.NoError(idx.RevertUpdate(ctx, doc2, doc2b))

		s.Equal(3, idx.GetNumberOfKeys())
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 1)
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Equal(doc1, helloMatches[0])
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 1)
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Equal(doc2, worldMatches[0])
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal(doc3, blopMatches[0])
	})

	s.Run("InvalidRemove", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)

		ctx := new(contextMock)

		open := make(<-chan struct{})
		closed := make(chan struct{})
		close(closed)

		ctx.On("Done").Return(open).Once()
		ctx.On("Done").Return((<-chan struct{})(closed)).Once()
		ctx.On("Err").Return(fmt.Errorf("error")).Once()

		i.Update(ctx, nil, nil)
	})

} // ==== End of 'Update' ==== //

func (s *IndexesTestSuite) TestGetMatchingDocuments() {

	// Get matching documents
	s.Run("AllOrEmptyArray", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		doc4 := data.M{"a": 23, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.NoError(idx.Insert(ctx, doc4))

		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal([]domain.Document{doc3}, blopMatches)
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Equal([]domain.Document{doc2, doc4}, worldMatches)
		nopeMatches, err := idx.GetMatching("nope")
		s.NoError(err)
		s.Equal([]domain.Document{}, nopeMatches)
	})

	// Can get all documents for a given key in a unique index
	s.Run("AllForGivenKeyUnique", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Unique())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal([]domain.Document{doc3}, blopMatches)
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Equal([]domain.Document{doc2}, worldMatches)
		nopeMatches, err := idx.GetMatching("nope")
		s.NoError(err)
		s.Equal([]domain.Document{}, nopeMatches)
	})

	// Can get all documents for which a field is nil
	s.Run("GetAllForNilField", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 2, "nottf": "bloup"}
		doc3 := data.M{"a": 8, "tf": "world"}
		doc4 := data.M{"a": 7, "nottf": "yes"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal([]domain.Document{}, blopMatches)
		helloMatches, err := idx.GetMatching("hello")
		s.NoError(err)
		s.Equal([]domain.Document{doc1}, helloMatches)
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Equal([]domain.Document{doc3}, worldMatches)
		yesMatches, err := idx.GetMatching("yes")
		s.NoError(err)
		s.Equal([]domain.Document{}, yesMatches)
		nilMatches, err := idx.GetMatching(nil)
		s.NoError(err)
		s.Equal([]domain.Document{doc2}, nilMatches)

		s.NoError(idx.Insert(ctx, doc4))

		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal([]domain.Document{}, blopMatches)
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Equal([]domain.Document{doc1}, helloMatches)
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Equal([]domain.Document{doc3}, worldMatches)
		yesMatches, err = idx.GetMatching("yes")
		s.NoError(err)
		s.Equal([]domain.Document{}, yesMatches)
		nilMatches, err = idx.GetMatching(nil)
		s.NoError(err)
		s.Equal([]domain.Document{doc2, doc4}, nilMatches)
	})

	// NOTE: Go doesn't have "undefined", so undefined and null are treated
	// the same. This case is already covered by the previous test.
	//	it('Can get all documents for which a field is null', function () {
	//	  const idx = new Index({ fieldName: 'tf' })
	//	  const doc1 = { a: 5, tf: 'hello' }
	//	  const doc2 = { a: 2, tf: null }
	//	  const doc3 = { a: 8, tf: 'world' }
	//	  const doc4 = { a: 7, tf: null }
	//	  idx.insert(doc1)
	//	  idx.insert(doc2)
	//	  idx.insert(doc3)
	//	  assert.deepStrictEqual(idx.getMatching('bloup'), [])
	//	  assert.deepStrictEqual(idx.getMatching('hello'), [doc1])
	//	  assert.deepStrictEqual(idx.getMatching('world'), [doc3])
	//	  assert.deepStrictEqual(idx.getMatching('yes'), [])
	//	  assert.deepStrictEqual(idx.getMatching(null), [doc2])
	//	  idx.insert(doc4)
	//	  assert.deepStrictEqual(idx.getMatching('bloup'), [])
	//	  assert.deepStrictEqual(idx.getMatching('hello'), [doc1])
	//	  assert.deepStrictEqual(idx.getMatching('world'), [doc3])
	//	  assert.deepStrictEqual(idx.getMatching('yes'), [])
	//	  assert.deepStrictEqual(idx.getMatching(null), [doc2, doc4])
	//	})

	// Can get all documents for a given key in a sparse index, but not unindexed docs (= field undefined)
	s.Run("AllDocsForKeySparseButNotUnindexedDocs", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexSparse(true),
		)
		s.Equal("tf", i.FieldName())
		s.True(i.Sparse())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 2, "nottf": "bloup"}
		doc3 := data.M{"a": 8, "tf": "world"}
		doc4 := data.M{"a": 7, "nottf": "yes"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.NoError(idx.Insert(ctx, doc4))

		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal([]domain.Document{}, blopMatches)
		helloMatches, err := idx.GetMatching("hello")
		s.NoError(err)
		s.Equal([]domain.Document{doc1}, helloMatches)
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Equal([]domain.Document{doc3}, worldMatches)
		yesMatches, err := idx.GetMatching("yes")
		s.NoError(err)
		s.Equal([]domain.Document{}, yesMatches)
		nilMatches, err := idx.GetMatching(nil)
		s.NoError(err)
		s.Equal([]domain.Document{}, nilMatches)
	})

	// Can get all documents whose key is in an array of keys
	s.Run("AllWithKeyInArrayOfKey", func() {
		// For this test only we have to use objects with _ids as the
		// array version of getMatching relies on the _id property being
		// set, otherwise we have to use a quadratic algorithm or a
		// fingerprinting algorithm, both solutions too complicated and
		// slow given that live gedb indexes documents with _id always
		// set
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello", "_id": "1"}
		doc2 := data.M{"a": 2, "tf": "bloup", "_id": "2"}
		doc3 := data.M{"a": 8, "tf": "world", "_id": "3"}
		doc4 := data.M{"a": 7, "tf": "yes", "_id": "4"}
		doc5 := data.M{"a": 7, "tf": "yes", "_id": "5"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.NoError(idx.Insert(ctx, doc4))
		s.NoError(idx.Insert(ctx, doc5))

		emptyMatches, err := idx.GetMatching()
		s.NoError(err)
		s.Equal([]domain.Document{}, emptyMatches)
		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Equal([]domain.Document{doc2}, blopMatches)
		res, err := idx.GetMatching("bloup", "yes")
		s.NoError(err)
		s.Equal([]domain.Document{doc2, doc4, doc5}, res)
		noMatchesNoNo, err := idx.GetMatching("nope", "no")
		s.NoError(err)
		s.Equal([]domain.Document{}, noMatchesNoNo)
	})

	// Can get all documents whose key is between certain bounds
	s.Run("AllDocsWithKeyInCertainBounds", func() {
		i, err := NewIndex(domain.WithIndexFieldName("a"))
		s.Equal("a", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": int64(5), "tf": "hello"}
		doc2 := data.M{"a": int64(2), "tf": "bloup"}
		doc3 := data.M{"a": int64(8), "tf": "world"}
		doc4 := data.M{"a": int64(7), "tf": "yes"}
		doc5 := data.M{"a": int64(10), "tf": "yes"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))
		s.NoError(idx.Insert(ctx, doc4))
		s.NoError(idx.Insert(ctx, doc5))

		d, err := idx.GetBetweenBounds(ctx, data.M{"$lt": int64(10), "$gte": int64(5)})
		s.NoError(err)
		s.Equal([]domain.Document{doc1, doc4, doc3}, d)
		d, err = idx.GetBetweenBounds(ctx, data.M{"$lte": int64(8)})
		s.NoError(err)
		s.Equal([]domain.Document{doc2, doc1, doc4, doc3}, d)
		d, err = idx.GetBetweenBounds(ctx, data.M{"$gt": int64(7)})
		s.NoError(err)
		s.Equal([]domain.Document{doc3, doc5}, d)
	})
} // ==== End of 'Get matching documents' ==== //

func (s *IndexesTestSuite) TestGetMatchingInvalidParameter() {
	idx, err := NewIndex(domain.WithIndexFieldName("_id"))
	s.NoError(err)

	ctx := context.Background()

	s.NoError(idx.Insert(ctx, data.M{"_id": []string{}}))
	s.NoError(idx.Insert(ctx, data.M{"_id": []string{}}))

	data, err := idx.GetMatching([]string{}, []string{})
	s.Error(err)
	s.Nil(data)
}

func (s *IndexesTestSuite) TestGetMatchingInvalidSort() {
	idx, err := NewIndex(domain.WithIndexFieldName("_id"))
	s.NoError(err)
	ctx := context.Background()

	s.NoError(idx.Insert(ctx, data.M{"_id": 1}))

	s.NoError(idx.Insert(ctx, data.M{"_id": 2}))

	s.NoError(idx.Insert(ctx, data.M{"_id": 3}))

	c := new(comparerMock)
	idx.(*Index).comparer = c
	c.On("Compare", mock.Anything, mock.Anything).
		Return(0, fmt.Errorf("error")).
		Once()

	data, err := idx.GetMatching(1, 2, 3)
	s.Error(err)
	s.Nil(data)
	c.AssertExpectations(s.T())
}

func (s *IndexesTestSuite) TestGetMatchingInvalidMapGetKey() {
	idx, err := NewIndex(domain.WithIndexFieldName("_id"))
	s.NoError(err)
	ctx := context.Background()

	s.NoError(idx.Insert(ctx, data.M{"_id": 1}))

	c := new(comparerMock)
	idx.(*Index).comparer = c
	c.On("Compare", mock.Anything, mock.Anything).
		Return(0, fmt.Errorf("error")).
		Once()

	data, err := idx.GetMatching(1)
	s.Error(err)
	s.Nil(data)
}

func (s *IndexesTestSuite) TestResetting() {
	// Can reset an index without any new data, the index will be empty afterwards
	s.Run("ResetIndexWithoutData", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		s.Equal(3, idx.GetNumberOfKeys())
		helloMatches, err := idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 1)
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 1)
		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)

		s.NoError(idx.Reset(ctx))
		s.Equal(0, idx.GetNumberOfKeys())
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 0)
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 0)
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 0)
	})

	// Can reset an index and initialize it with one document
	s.Run("ResetAndInitialize", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		newDoc := data.M{"a": 555, "tf": "new"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		s.Equal(3, idx.GetNumberOfKeys())
		helloMatches, err := idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 1)
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 1)
		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)

		s.NoError(idx.Reset(ctx, newDoc))
		s.Equal(1, idx.GetNumberOfKeys())
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 0)
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 0)
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 0)
		newMatches, err := idx.GetMatching("new")
		s.NoError(err)
		s.Equal(555, newMatches[0].Get("a"))
	})

	// Can reset an index and initialize it with an array of documents
	s.Run("ResetWithMultipleDocs", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.Equal("tf", i.FieldName())
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}
		newDocs := []domain.Document{data.M{"a": 555, "tf": "new"}, data.M{"a": 666, "tf": "again"}}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		s.Equal(3, idx.GetNumberOfKeys())
		helloMatches, err := idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 1)
		worldMatches, err := idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 1)
		blopMatches, err := idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 1)

		s.NoError(idx.Reset(ctx, newDocs...))
		s.Equal(2, idx.GetNumberOfKeys())
		helloMatches, err = idx.GetMatching("hello")
		s.NoError(err)
		s.Len(helloMatches, 0)
		worldMatches, err = idx.GetMatching("world")
		s.NoError(err)
		s.Len(worldMatches, 0)
		blopMatches, err = idx.GetMatching("bloup")
		s.NoError(err)
		s.Len(blopMatches, 0)
		newMatches, err := idx.GetMatching("new")
		s.NoError(err)
		s.Equal(555, newMatches[0].Get("a"))
		againMatches, err := idx.GetMatching("again")
		s.NoError(err)
		s.Equal(666, againMatches[0].Get("a"))

	})
} // ==== End of 'Resetting' ==== //

// Get all elements in the index.
func (s *IndexesTestSuite) TestGetAll() {
	i, err := NewIndex(domain.WithIndexFieldName("tf"))
	s.Equal("tf", i.FieldName())
	s.NoError(err)
	idx := i.(*Index)
	doc1 := data.M{"a": 5, "tf": "hello"}
	doc2 := data.M{"a": 8, "tf": "world"}
	doc3 := data.M{"a": 2, "tf": "bloup"}

	ctx := context.Background()

	s.NoError(idx.Insert(ctx, doc1))
	s.NoError(idx.Insert(ctx, doc2))
	s.NoError(idx.Insert(ctx, doc3))

	s.Equal([]domain.Document{data.M{"a": 2, "tf": "bloup"}, data.M{"a": 5, "tf": "hello"}, data.M{"a": 8, "tf": "world"}}, idx.GetAll())
}

func (s *IndexesTestSuite) TestCancelContex() {

	idx, err := NewIndex()
	s.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	<-ctx.Done()

	err = idx.Reset(ctx)
	s.ErrorIs(err, context.Canceled)

	err = idx.Insert(ctx)
	s.ErrorIs(err, context.Canceled)

	err = idx.Remove(ctx)
	s.ErrorIs(err, context.Canceled)

	err = idx.Update(ctx, nil, nil)
	s.ErrorIs(err, context.Canceled)

	err = idx.UpdateMultipleDocs(ctx)
	s.ErrorIs(err, context.Canceled)

	err = idx.RevertMultipleUpdates(ctx)
	s.ErrorIs(err, context.Canceled)

	_, err = idx.GetBetweenBounds(ctx, nil)
	s.ErrorIs(err, context.Canceled)

	ctxMock := new(contextMock)

	ch := make(chan struct{})
	ctxMock.On("Done").Return((<-chan struct{})(ch)).Once()
	ctxMock.On("Done").
		Run(func(mock.Arguments) {
			close(ch)
		}).
		Return((<-chan struct{})(ch)).
		Once()
	ctxMock.On("Err").Return(context.Canceled).Once()

	err = idx.UpdateMultipleDocs(ctxMock, domain.Update{})
	s.ErrorIs(err, context.Canceled)
}

func (s *IndexesTestSuite) TestGetKeysMultiFieldFailedNavigation() {
	fn := new(fieldNavigatorMock)
	fn.On("SplitFields", "tf").Return([]string{"tf"}, nil).Once()
	idx, err := NewIndex(
		domain.WithIndexFieldName("tf"),
		domain.WithIndexFieldNavigator(fn),
	)
	s.NoError(err)

	s.Run("GetAddress", func() {
		expectedErr := fmt.Errorf("error")
		fn.On("GetAddress", "tf").Return([]string{}, expectedErr).Once()

		keys, err := idx.(*Index).getKeysMultiField(nil)
		s.ErrorIs(err, expectedErr)
		s.Nil(keys)

		fn.AssertExpectations(s.T())
	})

	s.Run("GetField", func() {
		expectedErr := fmt.Errorf("error")
		fn.On("GetAddress", "tf").Return([]string{"tf"}, nil).Once()
		fn.On("GetField", data.M{"obj": true}, []string{"tf"}).
			Return([]domain.GetSetter{}, false, expectedErr).
			Once()

		keys, err := idx.(*Index).getKeysMultiField(data.M{"obj": true})
		s.ErrorIs(err, expectedErr)
		s.Nil(keys)

		fn.AssertExpectations(s.T())
	})
}

func (s *IndexesTestSuite) TestGetKeysFailedNavigation() {
	fn := new(fieldNavigatorMock)
	fn.On("SplitFields", "tf").Return([]string{"tf"}, nil).Once()
	idx, err := NewIndex(
		domain.WithIndexFieldName("tf"),
		domain.WithIndexFieldNavigator(fn),
	)
	s.NoError(err)

	s.Run("GetAddress", func() {
		expectedErr := fmt.Errorf("error")
		fn.On("GetAddress", "tf").Return([]string{}, expectedErr).Once()

		keys, err := idx.(*Index).getKeys(nil)
		s.ErrorIs(err, expectedErr)
		s.Nil(keys)

		fn.AssertExpectations(s.T())
	})

	s.Run("GetField", func() {
		expectedErr := fmt.Errorf("error")
		fn.On("GetAddress", "tf").Return([]string{"tf"}, nil).Once()
		fn.On("GetField", data.M{"obj": true}, []string{"tf"}).
			Return([]domain.GetSetter{}, false, expectedErr).
			Once()

		keys, err := idx.(*Index).getKeys(data.M{"obj": true})
		s.ErrorIs(err, expectedErr)
		s.Nil(keys)

		fn.AssertExpectations(s.T())
	})
}

func (s *IndexesTestSuite) TestNoField() {
	idx, err := NewIndex(
		domain.WithIndexFieldName("tf.a"),
	)
	s.Equal("tf.a", idx.FieldName())
	s.NoError(err)
	s.NotNil(idx)

	ctx := context.Background()
	s.NoError(idx.Insert(ctx, data.M{"tf": []any{}}))
	docs, err := idx.GetMatching(nil)
	s.NoError(err)
	s.Equal(data.M{"tf": []any{}}, docs[0])

}

func (s *IndexesTestSuite) compareThings(a any, b any) int {
	comp, _ := s.comparer.Compare(a, b)
	return comp
}

func TestIndexesTestSuite(t *testing.T) {
	suite.Run(t, new(IndexesTestSuite))
}
