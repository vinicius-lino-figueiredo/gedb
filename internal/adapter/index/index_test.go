package index

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

type IndexesTestSuite struct {
	suite.Suite
	comparer domain.Comparer
}

func (s *IndexesTestSuite) SetupSuite() {
	s.comparer = comparer.NewComparer()
}

func (s *IndexesTestSuite) TestInsertion() {
	// Can insert pointers to documents in the index correctly when they have the field
	s.Run("Pointers", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.Equal(1, idx.Tree.GetNumberOfKeys())
		s.Error(idx.Insert(ctx, doc1))
	})

	// Inserting twice for a fieldName the docs don't have with a unique
	// index results in an error thrown
	s.Run("InsertTwiceNonUnique", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("nope"),
			domain.WithIndexUnique(true),
		)
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 5, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.Equal(1, idx.Tree.GetNumberOfKeys())
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
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 5, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.Equal(0, idx.Tree.GetNumberOfKeys()) // Docs are not indexed
	})

	// Inserting twice for the same compound fieldName in a unique index will result in an error thrown
	s.Run("InsertTwiceSameCompound", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf,tg"),
			domain.WithIndexUnique(true),
		)
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello", "tg": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.Equal(1, idx.Tree.GetNumberOfKeys())
		s.Error(idx.Insert(ctx, doc1))
	})

	// Inserting twice for a compound fieldName the docs dont have with a unique and sparse index will not throw, since the docs will be non indexed
	s.Run("InsertTwiceCompoundSparse", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("nope,nopeNope"),
			domain.WithIndexUnique(true),
			domain.WithIndexSparse(true),
		)
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 5, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.Equal(0, idx.Tree.GetNumberOfKeys()) // Docs are not indexed
	})

	// Works with dot notation
	s.Run("DotNotation", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf.nested"))
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1, doc2, doc3))
		s.Equal(3, idx.Tree.GetNumberOfKeys())
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

		s.Equal(0, idx.Tree.GetNumberOfKeys())
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
			s.Equal(3, idx.Tree.GetNumberOfKeys())
			s.Equal([]any{data.M{"a": int64(5), "tf": "hello", "tf2": int64(7)}}, idx.Tree.Search(data.M{"tf": "hello", "tf2": int64(7)}))
			s.Equal([]any{data.M{"a": int64(8), "tf": "hello", "tf2": int64(6)}}, idx.Tree.Search(data.M{"tf": "hello", "tf2": int64(6)}))
			s.Equal([]any{data.M{"a": int64(2), "tf": "bloup", "tf2": int64(3)}}, idx.Tree.Search(data.M{"tf": "bloup", "tf2": int64(3)}))

			// The nodes contain pointers to the actual documents
			s.Equal(doc2, idx.Tree.Search(data.M{"tf": "hello", "tf2": int64(6)})[0])
			idx.Tree.Search(data.M{"tf": "bloup", "tf2": int64(3)})[0].(domain.Document).Set("a", 42)
			s.Equal(42, doc3.Get("a"))
		})
	})
} // ==== End of 'Insertion' ==== //

func (s *IndexesTestSuite) TestRemoval() {
	// Can remove pointers from the index, even when multiple documents have the same key
	s.Run("PointersMultipleDocsSameKey", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())

		s.NoError(idx.Remove(ctx, doc1))
		s.Equal(2, idx.Tree.GetNumberOfKeys())
		s.Len(idx.Tree.Search("hello"), 0)

		s.NoError(idx.Remove(ctx, doc2))
		s.Equal(2, idx.Tree.GetNumberOfKeys())
		s.Len(idx.Tree.Search("world"), 1)
		s.Equal(doc4, idx.Tree.Search("world")[0])
	})

	// If we have a sparse index, removing a non indexed doc has no effect
	s.Run("SparseIndexNonIndexedDoc", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("nope"),
			domain.WithIndexSparse(true),
		)
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 5, "tf": "world"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.Equal(0, idx.Tree.GetNumberOfKeys())

		s.NoError(idx.Remove(ctx, doc1))
		s.Equal(0, idx.Tree.GetNumberOfKeys())
	})

	// Works with dot notation
	s.Run("DotNotation", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf.nested"))
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())

		s.NoError(idx.Remove(ctx, doc1))
		s.Equal(2, idx.Tree.GetNumberOfKeys())
		s.Len(idx.Tree.Search("hello"), 0)

		s.NoError(idx.Remove(ctx, doc2))
		s.Equal(2, idx.Tree.GetNumberOfKeys())
		s.Len(idx.Tree.Search("world"), 1)
		s.Equal(doc4, idx.Tree.Search("world")[0])
	})

	// Can remove an array of documents
	s.Run("ArrayOfDocuments", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1, doc2, doc3))
		s.Equal(3, idx.Tree.GetNumberOfKeys())
		s.NoError(idx.Remove(ctx, doc1, doc3))
		s.Equal(1, idx.Tree.GetNumberOfKeys())
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
} // ==== End of 'Removal' ==== //

func (s *IndexesTestSuite) TestUpdate() {
	s.Run("UpdateChangedOrUnchangedKey", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())
		s.Equal([]any{doc2}, idx.Tree.Search("world"))

		s.NoError(idx.Update(ctx, doc2, doc4))
		s.Equal(3, idx.Tree.GetNumberOfKeys())
		s.Equal([]any{doc4}, idx.Tree.Search("world"))

		s.NoError(idx.Update(ctx, doc1, doc5))
		s.Equal(3, idx.Tree.GetNumberOfKeys())
		s.Equal([]any{}, idx.Tree.Search("hello"))
		s.Equal([]any{doc5}, idx.Tree.Search("changed"))
	})

	// If a simple update violates a unique constraint, changes are rolled back and an error thrown
	s.Run("RollbackAndError", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
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

		s.Equal(3, idx.Tree.GetNumberOfKeys())
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))

		e := &bst.ErrViolated{}
		s.ErrorAs(idx.Update(ctx, doc3, bad), &e)

		// No change
		s.Equal(3, idx.Tree.GetNumberOfKeys())
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
		s.Equal([]any{doc1}, idx.Tree.Search("hello"))
	})

	// Can update an array of documents
	s.Run("ArrayOfDocuments", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())

		s.NoError(idx.UpdateMultipleDocs(ctx, []domain.Update{{OldDoc: doc1, NewDoc: doc1b}, {OldDoc: doc2, NewDoc: doc2b}, {OldDoc: doc3, NewDoc: doc3b}}...))

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())

		e := &bst.ErrViolated{}
		s.ErrorAs(idx.UpdateMultipleDocs(ctx, []domain.Update{{OldDoc: doc1, NewDoc: doc1b}, {OldDoc: doc2, NewDoc: doc2b}, {OldDoc: doc3, NewDoc: doc3b}}...), &e)

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())
		s.Equal([]any{doc2}, idx.Tree.Search("world"))

		s.NoError(idx.Update(ctx, doc2, noChange)) // No error returned
		s.Equal(3, idx.Tree.GetNumberOfKeys())
		s.Equal([]any{noChange}, idx.Tree.Search("world"))
	})

	// Can revert simple and batch updates
	s.Run("ReverSimpleAndBatch", func() {
		i, err := NewIndex(
			domain.WithIndexFieldName("tf"),
			domain.WithIndexUnique(true),
		)
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
		s.Equal(3, idx.Tree.GetNumberOfKeys())

		s.NoError(idx.UpdateMultipleDocs(ctx, batchUpdate...))

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
} // ==== End of 'Update' ==== //

func (s *IndexesTestSuite) TestGetMatchingDocuments() {

	// Get matching documents
	s.Run("AllOrEmptyArray", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
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

func (s *IndexesTestSuite) TestResetting() {
	// Can reset an index without any new data, the index will be empty afterwards
	s.Run("ResetIndexWithoutData", func() {
		i, err := NewIndex(domain.WithIndexFieldName("tf"))
		s.NoError(err)
		idx := i.(*Index)
		doc1 := data.M{"a": 5, "tf": "hello"}
		doc2 := data.M{"a": 8, "tf": "world"}
		doc3 := data.M{"a": 2, "tf": "bloup"}

		ctx := context.Background()

		s.NoError(idx.Insert(ctx, doc1))
		s.NoError(idx.Insert(ctx, doc2))
		s.NoError(idx.Insert(ctx, doc3))

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
		s.Equal(0, idx.Tree.GetNumberOfKeys())
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

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
		s.Equal(1, idx.Tree.GetNumberOfKeys())
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

		s.Equal(3, idx.Tree.GetNumberOfKeys())
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
		s.Equal(2, idx.Tree.GetNumberOfKeys())
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

func (s *IndexesTestSuite) compareThings(a any, b any) int {
	comp, _ := s.comparer.Compare(a, b)
	return comp
}

func TestIndexesTestSuite(t *testing.T) {
	suite.Run(t, new(IndexesTestSuite))
}
