package querier

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/matcher"
)

type M = data.M
type A = []any
type S = domain.Sort

type comparerMock struct{ mock.Mock }

// Comparable implements domain.Comparer.
func (c *comparerMock) Comparable(a any, b any) bool {
	return c.Called(a, b).Bool(0)
}

// Compare implements domain.Comparer.
func (c *comparerMock) Compare(a any, b any) (int, error) {
	return c.Called(a, b).Get(0).(func(any, any) (int, error))(a, b)
}

type fieldNavigatorMock struct{ mock.Mock }

// EnsureField implements [domain.FieldNavigator].
func (f *fieldNavigatorMock) EnsureField(doc any, addr ...string) ([]domain.GetSetter, error) {
	call := f.Called(doc, addr)
	return call.Get(0).([]domain.GetSetter), call.Error(1)
}

// GetAddress implements [domain.FieldNavigator].
func (f *fieldNavigatorMock) GetAddress(field string) ([]string, error) {
	call := f.Called(field)
	return call.Get(0).([]string), call.Error(1)
}

// GetField implements [domain.FieldNavigator].
func (f *fieldNavigatorMock) GetField(doc any, addr ...string) ([]domain.GetSetter, bool, error) {
	call := f.Called(doc, addr)
	return call.Get(0).([]domain.GetSetter), call.Bool(1), call.Error(2)
}

// SplitFields implements [domain.FieldNavigator].
func (f *fieldNavigatorMock) SplitFields(field string) ([]string, error) {
	call := f.Called(field)
	return call.Get(0).([]string), call.Error(1)
}

type QuerierTestSuite struct {
	suite.Suite
	q     *Querier
	docs1 []domain.Document
	docs2 []domain.Document
}

func (s *QuerierTestSuite) SetupSuite() {
	s.docs1 = []domain.Document{
		M{"age": 5},
		M{"age": 57},
		M{"age": 52},
		M{"age": 23},
		M{"age": 89},
	}
	s.docs2 = []domain.Document{
		M{
			"_id":    "doc0._id",
			"age":    5,
			"name":   "Jo",
			"planet": "B",
			"toys": M{
				"bebe":   true,
				"ballon": "much",
			},
		},
		M{
			"_id":    "doc1._id",
			"age":    57,
			"name":   "Louis",
			"planet": "R",
			"toys": M{
				"ballon": "yeah",
				"bebe":   false,
			},
		},
		M{
			"_id":    "doc2._id",
			"age":    52,
			"name":   "Graffiti",
			"planet": "C",
			"toys":   M{"bebe": "kind of"},
		},
		M{"_id": "doc3._id", "age": 23, "name": "LM", "planet": "S"},
		M{"_id": "doc4._id", "age": 89, "planet": "Earth"},
	}
}

func (s *QuerierTestSuite) SetupTest() {
	s.q = NewQuerier().(*Querier)
}

func (s *QuerierTestSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *QuerierTestSuite) TestNoTreatment() {
	docs, err := s.q.Query(s.docs1)
	s.NoError(err)
	s.Equal(s.docs1, docs)
}

func (s *QuerierTestSuite) TestEmptyQuery() {
	docs, err := s.q.Query(
		s.docs1,
		domain.WithQuery(M{}),
	)
	s.NoError(err)
	s.Equal(s.docs1, docs)
}

func (s *QuerierTestSuite) TestSimpleQuery() {
	docs, err := s.q.Query(
		s.docs1,
		domain.WithQuery(M{"age": M{"$gt": 23}}),
	)
	s.NoError(err)

	expected := []domain.Document{M{"age": 57}, M{"age": 52}, M{"age": 89}}
	s.Equal(expected, docs)
}

func (s *QuerierTestSuite) TestSortFailedFieldNavigation() {
	data := []domain.Document{M{"a": 1}, M{"a": 2}}
	s.Run("GetAddress", func() {
		fnm := new(fieldNavigatorMock)
		s.q.fn = fnm
		s.q = NewQuerier(
			domain.WithQuerierFieldNavigator(fnm),
			domain.WithQuerierMatcher(matcher.NewMatcher()),
		).(*Querier)
		fnm.On("GetAddress", "a").
			Return(([]string)(nil), fmt.Errorf("error")).
			Once()
		docs, err := s.q.Query(
			data,
			domain.WithQuerySort(S{{Key: "a", Order: 1}}),
		)
		s.Error(err)
		s.Nil(docs)
		fnm.AssertExpectations(s.T())
	})

	s.Run("GetFieldA", func() {
		fnm := new(fieldNavigatorMock)
		fnm.On("GetAddress", "a").
			Return([]string{"a"}, nil).
			Once()
		fnm.On("GetField", data[1], []string{"a"}).
			Return(([]domain.GetSetter)(nil), false, fmt.Errorf("error")).
			Once()
		s.q.fn = fnm
		docs, err := s.q.Query(
			data,
			domain.WithQuerySort(S{{Key: "a", Order: 1}}),
		)
		s.Error(err)
		s.Nil(docs)
		fnm.AssertExpectations(s.T())
	})

	s.Run("GetFieldB", func() {
		fnm := new(fieldNavigatorMock)
		fnm.On("GetAddress", "a").
			Return([]string{"a"}, nil).
			Once()
		fnm.On("GetField", data[0], []string{"a"}).
			Return(([]domain.GetSetter)(nil), false, fmt.Errorf("error")).
			Once()
		fnm.On("GetField", data[1], []string{"a"}).
			Return(([]domain.GetSetter)(nil), false, nil).
			Once()
		s.q.fn = fnm
		docs, err := s.q.Query(
			data,
			domain.WithQuerySort(S{{Key: "a", Order: 1}}),
		)
		s.Error(err)
		s.Nil(docs)
		fnm.AssertExpectations(s.T())
	})
}

func (s *QuerierTestSuite) TestEmptyCollection() {
	docs, err := s.q.Query([]domain.Document{})
	s.NoError(err)
	s.Len(docs, 0)
}

func (s *QuerierTestSuite) TestNilCollection() {
	docs, err := s.q.Query([]domain.Document{})
	s.NoError(err)
	s.Len(docs, 0)
}

func (s *QuerierTestSuite) TestLimit() {
	docs, err := s.q.Query(s.docs1, domain.WithQueryLimit(3))
	s.NoError(err)
	s.Len(docs, 3)
}

func (s *QuerierTestSuite) TestSkip() {
	docs, err := s.q.Query(s.docs1, domain.WithQuerySkip(2))
	s.NoError(err)
	s.Len(docs, 3)
}

func (s *QuerierTestSuite) TestLimitAndSkip() {
	docs, err := s.q.Query(
		s.docs1,
		domain.WithQueryLimit(4),
		domain.WithQuerySkip(3),
	)
	s.NoError(err)
	s.Len(docs, 2)
}

func (s *QuerierTestSuite) TestSort() {
	docs, err := s.q.Query(
		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
	)
	s.NoError(err)

	var last any = 0
	for _, d := range docs {
		s.LessOrEqual(last, d.Get("age"))
		last = d.Get("age")
	}

	docs, err = s.q.Query(
		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: -1}}),
	)
	s.NoError(err)

	for _, d := range docs {
		s.GreaterOrEqual(last, d.Get("age"))
		last = d.Get("age")
	}
}

func (s *QuerierTestSuite) TestSortCustomComparison() {
	data := []domain.Document{
		M{"name": "alpha"},
		M{"name": "charlie"},
		M{"name": "zulu"},
	}

	cm := new(comparerMock)

	cm.On("Compare", mock.Anything, mock.Anything).
		Return(func(a, b any) (int, error) {
			a, _ = a.([]any)[0].(domain.Getter).Get()
			b, _ = b.([]any)[0].(domain.Getter).Get()
			return len(a.(string)) - len(b.(string)), nil
		}).Times(5)
	s.q.cmpr = cm

	docs, err := s.q.Query(
		data,
		domain.WithQuerySort(S{{Key: "name", Order: 1}}),
	)
	s.NoError(err)
	l := ""
	for _, d := range docs {
		s.LessOrEqual(len(l), len(d.Get("name").(string)))
		l = d.Get("name").(string)
	}

	docs, err = s.q.Query(
		data,
		domain.WithQuerySort(S{{Key: "name", Order: -1}}),
	)
	s.NoError(err)

	for _, d := range docs {
		s.GreaterOrEqual(len(l), len(d.Get("name").(string)))
		l = d.Get("name").(string)
	}

	cm.AssertExpectations(s.T())
}

func (s *QuerierTestSuite) TestFailMatching() {
	data := []domain.Document{
		M{"error": []string{}},
	}
	docs, err := s.q.Query(data, domain.WithQuery(M{"error": []int{}}))
	s.Error(err)
	s.Nil(docs)
}

func (s *QuerierTestSuite) TestSortEmpty() {
	docs, err := s.q.Query(

		nil,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
	)
	s.NoError(err)
	s.Len(docs, 0)
}

func (s *QuerierTestSuite) TestLimitAndSort() {
	docs, err := s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQueryLimit(3),
	)
	s.NoError(err)
	s.Len(docs, 3)
	s.Equal(5, docs[0].Get("age"))
	s.Equal(23, docs[1].Get("age"))
	s.Equal(52, docs[2].Get("age"))

	docs, err = s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: -1}}),
		domain.WithQueryLimit(2),
	)
	s.NoError(err)
	s.Len(docs, 2)
	s.Equal(89, docs[0].Get("age"))
	s.Equal(57, docs[1].Get("age"))
}

func (s *QuerierTestSuite) TestLimitGreaterThanTotal() {
	docs, err := s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQueryLimit(7),
	)
	s.NoError(err)
	s.Len(docs, 5)
	s.Equal(5, docs[0].Get("age"))
	s.Equal(23, docs[1].Get("age"))
	s.Equal(52, docs[2].Get("age"))
	s.Equal(57, docs[3].Get("age"))
	s.Equal(89, docs[4].Get("age"))
}

func (s *QuerierTestSuite) TestLimitSkipAndSort() {
	docs, err := s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQueryLimit(1),
		domain.WithQuerySkip(2),
	)
	s.NoError(err)
	s.Len(docs, 1)
	s.Equal(52, docs[0].Get("age"))

	docs, err = s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQueryLimit(3),
		domain.WithQuerySkip(1),
	)
	s.NoError(err)
	s.Len(docs, 3)
	s.Equal(23, docs[0].Get("age"))
	s.Equal(52, docs[1].Get("age"))
	s.Equal(57, docs[2].Get("age"))

	docs, err = s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: -1}}),
		domain.WithQueryLimit(2),
		domain.WithQuerySkip(2),
	)
	s.NoError(err)
	s.Len(docs, 2)
	s.Equal(52, docs[0].Get("age"))
	s.Equal(23, docs[1].Get("age"))
}

func (s *QuerierTestSuite) TestTooBigLimitWithSkipAndSort() {
	docs, err := s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQueryLimit(8),
		domain.WithQuerySkip(2),
	)
	s.NoError(err)
	s.Len(docs, 3)
	s.Equal(52, docs[0].Get("age"))
	s.Equal(57, docs[1].Get("age"))
	s.Equal(89, docs[2].Get("age"))
}

func (s *QuerierTestSuite) TestNoReturnTooBigSkip() {
	docs, err := s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQuerySkip(5),
	)
	s.NoError(err)
	s.Len(docs, 0)

	docs, err = s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQuerySkip(5),
	)
	s.NoError(err)
	s.Len(docs, 0)

	docs, err = s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQuerySkip(7),
		domain.WithQueryLimit(3),
	)
	s.NoError(err)
	s.Len(docs, 0)

	docs, err = s.q.Query(

		s.docs1,
		domain.WithQuerySort(S{{Key: "age", Order: 1}}),
		domain.WithQuerySkip(7),
		domain.WithQueryLimit(6),
	)
	s.NoError(err)
	s.Len(docs, 0)
}

func (s *QuerierTestSuite) TestSortStrings() {
	data := []domain.Document{
		M{"name": "jako"},
		M{"name": "jakeb"},
		M{"name": "sue"},
	}

	docs, err := s.q.Query(data, domain.WithQuerySort(S{{Key: "name", Order: 1}}))
	s.NoError(err)
	s.Len(docs, 3)
	s.Equal("jakeb", docs[0].Get("name"))
	s.Equal("jako", docs[1].Get("name"))
	s.Equal("sue", docs[2].Get("name"))

	docs, err = s.q.Query(data, domain.WithQuerySort(S{{Key: "name", Order: -1}}))
	s.NoError(err)
	s.Len(docs, 3)
	s.Equal("sue", docs[0].Get("name"))
	s.Equal("jako", docs[1].Get("name"))
	s.Equal("jakeb", docs[2].Get("name"))
}

func (s *QuerierTestSuite) TestSortDates() {
	data := []domain.Document{
		M{"event": M{"recorded": time.UnixMilli(400)}},
		M{"event": M{"recorded": time.UnixMilli(60000)}},
		M{"event": M{"recorded": time.UnixMilli(32)}},
	}

	docs, err := s.q.Query(

		data,
		domain.WithQuerySort(S{{Key: "event.recorded", Order: 1}}),
	)
	s.NoError(err)
	s.Equal(time.UnixMilli(32), docs[0].D("event").Get("recorded"))
	s.Equal(time.UnixMilli(400), docs[1].D("event").Get("recorded"))
	s.Equal(time.UnixMilli(60000), docs[2].D("event").Get("recorded"))

	docs, err = s.q.Query(

		data,
		domain.WithQuerySort(S{{Key: "event.recorded", Order: -1}}),
	)
	s.NoError(err)
	s.Equal(time.UnixMilli(60000), docs[0].D("event").Get("recorded"))
	s.Equal(time.UnixMilli(400), docs[1].D("event").Get("recorded"))
	s.Equal(time.UnixMilli(32), docs[2].D("event").Get("recorded"))
}

func (s *QuerierTestSuite) TestSortSomeUndefined() {
	data := []domain.Document{
		M{"name": "jako", "other": 2},
		M{"name": "jakeb", "other": 3},
		M{"name": "sue"},
		M{"name": "henry", "other": 4},
	}

	docs, err := s.q.Query(data, domain.WithQuerySort(S{{Key: "other", Order: 1}}))
	s.NoError(err)
	s.Len(docs, 4)
	s.False(docs[0].Has("other"))
	s.Equal("sue", docs[0].Get("name"))
	s.Equal("jako", docs[1].Get("name"))
	s.Equal(2, docs[1].Get("other"))
	s.Equal("jakeb", docs[2].Get("name"))
	s.Equal(3, docs[2].Get("other"))
	s.Equal("henry", docs[3].Get("name"))
	s.Equal(4, docs[3].Get("other"))

	docs, err = s.q.Query(

		data,
		domain.WithQuerySort(S{{Key: "other", Order: -1}}),
		domain.WithQuery(M{
			"name": M{"$in": A{"suzy", "jakeb", "jako"}},
		}),
	)
	s.NoError(err)
	s.Len(docs, 2)
	s.Equal("jakeb", docs[0].Get("name"))
	s.Equal(3, docs[0].Get("other"))
	s.Equal("jako", docs[1].Get("name"))
	s.Equal(2, docs[1].Get("other"))
}

func (s *QuerierTestSuite) TestSortAllUndefined() {
	data := []domain.Document{
		M{"name": "jako"},
		M{"name": "jakeb"},
		M{"name": "sue"},
	}

	docs, err := s.q.Query(data, domain.WithQuerySort(S{{Key: "other", Order: 1}}))
	s.NoError(err)
	s.Len(docs, 3)

	data = []domain.Document{M{"name": "jakeb"}, M{"name": "sue"}}

	docs, err = s.q.Query(data, domain.WithQuerySort(S{{Key: "other", Order: 1}}))
	s.NoError(err)
	s.Len(docs, 2)
}

func (s *QuerierTestSuite) TestSortMultipleTimes() {
	data := []domain.Document{
		M{"name": "jako", "age": 43, "nid": 1},
		M{"name": "jakeb", "age": 43, "nid": 2},
		M{"name": "sue", "age": 12, "nid": 3},
		M{"name": "zoe", "age": 23, "nid": 4},
		M{"name": "jako", "age": 35, "nid": 5},
	}

	docs, err := s.q.Query(

		data,
		domain.WithQuerySort(S{
			{Key: "name", Order: 1},
			{Key: "age", Order: -1},
		}),
	)
	s.NoError(err)
	s.Len(docs, 5)
	s.Equal(2, docs[0].Get("nid"))
	s.Equal(1, docs[1].Get("nid"))
	s.Equal(5, docs[2].Get("nid"))
	s.Equal(3, docs[3].Get("nid"))
	s.Equal(4, docs[4].Get("nid"))

	docs, err = s.q.Query(

		data,
		domain.WithQuerySort(S{
			{Key: "name", Order: 1},
			{Key: "age", Order: 1},
		}),
	)
	s.NoError(err)
	s.Len(docs, 5)
	s.Equal(2, docs[0].Get("nid"))
	s.Equal(5, docs[1].Get("nid"))
	s.Equal(1, docs[2].Get("nid"))
	s.Equal(3, docs[3].Get("nid"))
	s.Equal(4, docs[4].Get("nid"))

	docs, err = s.q.Query(

		data,
		domain.WithQuerySort(S{
			{Key: "age", Order: 1},
			{Key: "name", Order: 1},
		}),
	)
	s.NoError(err)
	s.Len(docs, 5)
	s.Equal(3, docs[0].Get("nid"))
	s.Equal(4, docs[1].Get("nid"))
	s.Equal(5, docs[2].Get("nid"))
	s.Equal(2, docs[3].Get("nid"))
	s.Equal(1, docs[4].Get("nid"))

	docs, err = s.q.Query(

		data,
		domain.WithQuerySort(S{
			{Key: "age", Order: 1},
			{Key: "name", Order: -1},
		}),
	)
	s.NoError(err)
	s.Len(docs, 5)
	s.Equal(3, docs[0].Get("nid"))
	s.Equal(4, docs[1].Get("nid"))
	s.Equal(5, docs[2].Get("nid"))
	s.Equal(1, docs[3].Get("nid"))
	s.Equal(2, docs[4].Get("nid"))
}

func (s *QuerierTestSuite) TestSortSimilarDataMultipleTimes() {
	companies := []string{"acme", "milkman", "zoinks"}
	var data []domain.Document
	id := 0
	for _, company := range companies {
		for j := 5; j < 100; j += 5 {
			data = append(data, M{
				"company": company,
				"cost":    j,
				"nid":     id,
			})
			id++
		}
	}
	docs, err := s.q.Query(

		data,
		domain.WithQuerySort(S{
			{Key: "company", Order: 1},
			{Key: "cost", Order: 1},
		}),
	)
	s.NoError(err)
	for i, doc := range docs {
		s.Equal(i, doc.Get("nid"))
	}
}

func (s *QuerierTestSuite) TestFailSorting() {
	data := []domain.Document{
		M{"error": []string{}},
		M{"error": []string{}},
		M{"error": nil},
	}
	docs, err := s.q.Query(data, domain.WithQuerySort(S{{Key: "error", Order: 1}}))
	s.Error(err)
	s.Nil(docs)
}

func TestQuerierTestSuite(t *testing.T) {
	suite.Run(t, new(QuerierTestSuite))
}
