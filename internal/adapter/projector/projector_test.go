package projector

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldnavigator"
)

type M = data.M
type A = []any
type S = domain.Sort
type P = map[string]uint8

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

type ProjectorTestSuite struct {
	suite.Suite
	p     *Projector
	docs1 []domain.Document
	docs2 []domain.Document
	docs3 []domain.Document
}

func (s *ProjectorTestSuite) SetupSuite() {
	s.docs1 = []domain.Document{
		M{
			"$$indexCreated": M{
				"fieldName": "a",
				"unique":    false,
				"sparse":    false,
			},
		},
		M{"$$indexCreated": M{"fieldName": "a", "unique": true}},
		M{
			"a":         M{"a": 1, "b": 1, "c": 1},
			"_id":       "HfbXqYaCGpirLvyt",
			"createdAt": M{"$$date": 1754518566626},
			"updatedAt": M{"$$date": 1754518566626},
		},
		M{
			"a":         M{"d": 1, "e": 1, "f": 1},
			"_id":       "NNwSp1WG7NLnKp48",
			"createdAt": M{"$$date": 1754518566630},
			"updatedAt": M{"$$date": 1754518566630},
		},
	}

	s.docs2 = []domain.Document{
		M{"age": 5},
		M{"age": 57},
		M{"age": 52},
		M{"age": 23},
		M{"age": 89},
	}

	s.docs3 = []domain.Document{
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

func (s *ProjectorTestSuite) SetupTest() {
	s.p = NewProjector().(*Projector)
}

func (s *ProjectorTestSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *ProjectorTestSuite) TestNoProjection() {
	docs, err := s.p.Project(s.docs3, nil)
	s.NoError(err)
	s.Equal(s.docs3, docs)

	docs, err = s.p.Project(s.docs3, P{})
	s.NoError(err)
	s.Equal(s.docs3, docs)
}

func (s *ProjectorTestSuite) TestProjectNonExistentFields() {
	data := []domain.Document{
		M{"_id": "id-01", "age": 5, "name": "Jo"},
		M{"_id": "id-02", "age": 23, "name": "LM"},
		M{"_id": "id-03", "age": 52, "name": "Graffiti"},
		M{"_id": "id-04", "age": 57, "name": "Louis"},
		M{"_id": "id-05", "age": 89},
	}
	docs, err := s.p.Project(data, P{"age": 1, "name": 1})
	s.NoError(err)
	s.Equal(data, docs)

}

func (s *ProjectorTestSuite) TestOmitOnlyExpected() {
	docs, err := s.p.Project(s.docs3, P{"age": 0, "name": 0})
	s.NoError(err)
	s.Len(docs, 5)

	s.Equal(M{"planet": "B", "_id": "doc0._id", "toys": M{"bebe": true, "ballon": "much"}}, docs[0])
	s.Equal(M{"planet": "R", "_id": "doc1._id", "toys": M{"bebe": false, "ballon": "yeah"}}, docs[1])
	s.Equal(M{"planet": "C", "_id": "doc2._id", "toys": M{"bebe": "kind of"}}, docs[2])
	s.Equal(M{"planet": "S", "_id": "doc3._id"}, docs[3])
	s.Equal(M{"planet": "Earth", "_id": "doc4._id"}, docs[4])

	docs, err = s.p.Project(s.docs3, P{"age": 0, "name": 0, "_id": 0})
	s.NoError(err)
	s.Len(docs, 5)

	s.Equal(M{"planet": "B", "toys": M{"bebe": true, "ballon": "much"}}, docs[0])
	s.Equal(M{"planet": "R", "toys": M{"bebe": false, "ballon": "yeah"}}, docs[1])
	s.Equal(M{"planet": "C", "toys": M{"bebe": "kind of"}}, docs[2])
	s.Equal(M{"planet": "S"}, docs[3])
	s.Equal(M{"planet": "Earth"}, docs[4])
}

func (s *ProjectorTestSuite) TestProjectIncludeAndExclude() {
	docs, err := s.p.Project(s.docs3, P{"age": 1, "name": 0})
	s.Error(err)
	s.Nil(docs)

	docs, err = s.p.Project(s.docs3, P{"age": 1, "_id": 0})
	s.NoError(err)
	s.NotNil(docs)
	s.Equal(M{"age": 5}, docs[0])
	s.Equal(M{"age": 57}, docs[1])
	s.Equal(M{"age": 52}, docs[2])
	s.Equal(M{"age": 23}, docs[3])
	s.Equal(M{"age": 89}, docs[4])

	docs, err = s.p.Project(s.docs3, P{"age": 0, "toys": 0, "planet": 0, "_id": 1})
	s.NoError(err)
	s.NotNil(docs)
	s.Equal(M{"name": "Jo", "_id": "doc0._id"}, docs[0])
	s.Equal(M{"name": "Louis", "_id": "doc1._id"}, docs[1])
	s.Equal(M{"name": "Graffiti", "_id": "doc2._id"}, docs[2])
	s.Equal(M{"name": "LM", "_id": "doc3._id"}, docs[3])
	s.Equal(M{"_id": "doc4._id"}, docs[4])
}

func (s *ProjectorTestSuite) TestProjectNested() {
	docs, err := s.p.Project(s.docs3, P{"name": 0, "planet": 0, "toys.bebe": 0, "_id": 0})
	s.NoError(err)
	s.NotNil(docs)
	s.Equal(M{"age": 5, "toys": M{"ballon": "much"}}, docs[0])
	s.Equal(M{"age": 57, "toys": M{"ballon": "yeah"}}, docs[1])
	s.Equal(M{"age": 52, "toys": M{}}, docs[2])
	s.Equal(M{"age": 23}, docs[3])
	s.Equal(M{"age": 89}, docs[4])

	docs, err = s.p.Project(s.docs3, P{"name": 1, "toys.ballon": 1, "_id": 0})
	s.NoError(err)
	s.Equal(M{"name": "Jo", "toys": M{"ballon": "much"}}, docs[0])
	s.Equal(M{"name": "Louis", "toys": M{"ballon": "yeah"}}, docs[1])
	s.Equal(M{"name": "Graffiti"}, docs[2])
	s.Equal(M{"name": "LM"}, docs[3])
	s.Equal(M{}, docs[4])
}

func (s *ProjectorTestSuite) TestProjectExpanded() {
	data := []domain.Document{
		M{"values": A{
			M{"name": "Earth", "color": "blue"},
			M{"name": "Mars", "color": "red"},
		}},
		M{"values": A{
			M{"name": "nights", "value": 5},
			M{"name": "days", "value": 50},
		}},
	}
	docs, err := s.p.Project(
		data,
		P{"values.name": 1, "_id": 0},
	)
	s.NoError(err)
	s.Len(docs, 2)
	s.Equal(M{"values": M{"name": []any{"Earth", "Mars"}}}, docs[0])
	s.Equal(M{"values": M{"name": []any{"nights", "days"}}}, docs[1])
}

func (s *ProjectorTestSuite) TestProjectionFailedFieldNavigation() {
	s.Run("GetAddress", func() {
		data := []domain.Document{M{"a": 1}}
		fnm := new(fieldNavigatorMock)
		s.p = NewProjector(WithFieldNavigator(fnm)).(*Projector)
		fnm.On("GetAddress", "a").
			Return(([]string)(nil), fmt.Errorf("error"))
		docs, err := s.p.Project(
			data,
			P{"a": 1},
		)
		s.Error(err)
		s.Nil(docs)
	})
	s.Run("GetField", func() {
		data := []domain.Document{M{"a": 1}}
		fnm := new(fieldNavigatorMock)
		s.p = NewProjector(WithFieldNavigator(fnm)).(*Projector)
		fnm.On("GetAddress", "a").
			Return([]string{"a"}, nil).
			Once()
		fnm.On("GetField", M{"a": 1}, []string{"a"}).
			Return([]domain.GetSetter{}, false, fmt.Errorf("error")).
			Once()
		docs, err := s.p.Project(
			data,
			P{"a": 1},
		)
		s.Error(err)
		s.Nil(docs)
	})
	s.Run("NegativeGetField", func() {
		data := []domain.Document{M{"a": 1}}
		fnm := new(fieldNavigatorMock)
		s.p = NewProjector(WithFieldNavigator(fnm)).(*Projector)
		fnm.On("GetAddress", "a").
			Return([]string{"a"}, nil).
			Once()
		fnm.On("GetField", M{"a": 1}, []string{"a"}).
			Return([]domain.GetSetter{}, false, fmt.Errorf("error")).
			Once()
		docs, err := s.p.Project(data, P{"a": 0})
		s.Error(err)
		s.Nil(docs)
	})
	s.Run("EnsureField", func() {
		data := []domain.Document{M{"a": 1}}
		fnm := new(fieldNavigatorMock)
		s.p = NewProjector(WithFieldNavigator(fnm)).(*Projector)
		fnm.On("GetAddress", "a").
			Return([]string{"a"}, nil).
			Once()
		fnm.On("GetField", M{"a": 1}, []string{"a"}).
			Return(
				[]domain.GetSetter{
					fieldnavigator.NewGetSetterEmpty(),
				},
				true,
				nil,
			).
			Once()
		fnm.On("EnsureField", M{}, []string{"a"}).
			Return([]domain.GetSetter{}, fmt.Errorf("error")).
			Once()
		docs, err := s.p.Project(
			data,
			P{"a": 1},
		)
		s.Error(err)
		s.Nil(docs)
	})
}

func (s *ProjectorTestSuite) TestFailedDocumentFactory() {
	data := []domain.Document{
		M{"a": "b", "c": "d"},
	}

	errDocFac := fmt.Errorf("error")
	docFac := func(any) (domain.Document, error) {
		return nil, errDocFac
	}

	s.Run("positive", func() {
		s.p = NewProjector(WithDocumentFactory(docFac)).(*Projector)

		res, err := s.p.Project(data, P{"a": 1})
		s.Error(err)
		s.Nil(res)
	})
	s.Run("negative", func() {
		s.p = NewProjector(WithDocumentFactory(docFac)).(*Projector)

		res, err := s.p.Project(data, P{"a": 0})
		s.Error(err)
		s.Nil(res)
	})

}

func TestProjectorTestSuite(t *testing.T) {
	suite.Run(t, new(ProjectorTestSuite))
}
