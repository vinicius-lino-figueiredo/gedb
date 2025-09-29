package fieldgetter

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

type FieldGetterTestSuite struct {
	suite.Suite
	fg *FieldGetter
}

func (s *FieldGetterTestSuite) SetupTest() {
	s.fg = NewFieldGetter().(*FieldGetter)
}

func (s *FieldGetterTestSuite) TestFirstLevel() {
	doc := data.M{
		"hello": "world",
		"type": data.M{
			"planet": true,
			"blue":   true,
		},
	}

	dv, expanded, err := s.fg.GetField(doc, "hello")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	value, isSet := dv[0].Get()
	s.True(isSet)
	s.Equal("world", value)

	dv, expanded, err = s.fg.GetField(doc, "type", "planet")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	value, isSet = dv[0].Get()
	s.True(isSet)
	s.Equal(true, value)
}

func (s *FieldGetterTestSuite) TestNotOk() {
	doc := data.M{
		"hello": "world",
		"type": data.M{
			"planet": true,
			"blue":   true,
		},
	}

	dv, expanded, err := s.fg.GetField(doc, "helloo")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	_, isSet := dv[0].Get()
	s.False(isSet)

	dv, expanded, err = s.fg.GetField(doc, "type", "plane")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	_, isSet = dv[0].Get()
	s.False(isSet)
}

func (s *FieldGetterTestSuite) TestArray() {
	doc := data.M{
		"data": data.M{
			"planets": []any{
				data.M{"name": "Earth", "number": 3},
				data.M{"name": "Mars", "number": 4},
				data.M{"name": "Pluton", "number": 9},
			},
		},
		"planets": []any{
			data.M{"name": "Earth", "number": 3},
			data.M{"name": "Mars", "number": 4},
			data.M{"name": "Pluton", "number": 9},
		},
		"planetsMultiNumber": []any{
			data.M{"name": "Earth", "number": []any{1, 3}},
			data.M{"name": "Mars", "number": []any{7}},
			data.M{"name": "Pluton", "number": []any{9, 5, 1}},
		},
	}

	// simple
	dv, expanded, err := s.fg.GetField(doc, "planets", "name")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	s.Equal([]any{"Earth", "Mars", "Pluton"}, s.ListGetSetter(dv))

	// nested
	dv, expanded, err = s.fg.GetField(doc, "data", "planets", "number")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	s.Equal([]any{3, 4, 9}, s.ListGetSetter(dv))

	// nested arrays (should not concat)
	dv, expanded, err = s.fg.GetField(doc, "planetsMultiNumber", "number")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	s.Equal([]any{[]any{1, 3}, []any{7}, []any{9, 5, 1}}, s.ListGetSetter(dv))
}

func (s *FieldGetterTestSuite) TestIndex() {
	doc := data.M{
		"planets": []any{
			data.M{"name": "Earth", "number": 3},
			data.M{"name": "Mars", "number": 4},
			data.M{"name": "Pluton", "number": 9},
		},
		"data": data.M{
			"planets": []any{
				data.M{"name": "Earth", "number": 3},
				data.M{"name": "Mars", "number": 4},
				data.M{"name": "Pluton", "number": 9},
			},
		},
	}

	// simple
	dv, expanded, err := s.fg.GetField(doc, "planets", "1")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	s.Equal(data.M{"name": "Mars", "number": 4}, s.ListGetSetter(dv)[0])

	// out of bounds
	dv, expanded, err = s.fg.GetField(doc, "planets", "3")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	_, isSet := dv[0].Get()
	s.False(isSet)

	// nested list
	dv, expanded, err = s.fg.GetField(doc, "data", "planets", "2")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	s.Equal(data.M{"name": "Pluton", "number": 9}, s.ListGetSetter(dv)[0])

	// index in middle
	dv, expanded, err = s.fg.GetField(doc, "data", "planets", "0", "name")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	s.Equal("Earth", s.ListGetSetter(dv)[0])

}

func (s *FieldGetterTestSuite) TestEmptyObject() {

	dv, expanded, err := s.fg.GetField(nil, "planets", "0")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)

}

func (s *FieldGetterTestSuite) TestUnsetFieldInList() {
	doc := data.M{
		"planets": []any{
			nil,
			nil,
			nil,
		},
	}

	dv, expanded, err := s.fg.GetField(doc, "planets", "name")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	for _, v := range dv {
		value, isSet := v.Get()
		s.Nil(value)
		s.False(isSet)
	}
}

func (s *FieldGetterTestSuite) TestNestedInPrimitive() {
	doc := data.M{
		"data": data.M{
			"planets": "Not an object",
		},
	}

	dv, expnded, err := s.fg.GetField(doc, "data", "planets", "name")
	s.NoError(err)
	s.False(expnded)
	s.Len(dv, 1)
	value, isSet := dv[0].Get()
	s.False(isSet)
	s.Nil(value)
}

// should always return defined when expanding list values
func (s *FieldGetterTestSuite) TestReturnDefinedOnLists() {
	doc := data.M{
		"planets": []any{
			"Not an object 1",
			"Not an object 2",
			"Not an object 3",
		},
	}

	dv, expanded, err := s.fg.GetField(doc, "planets", "name")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	for _, v := range dv {
		value, isSet := v.Get()
		s.Nil(value)
		s.False(isSet)
	}
}

// Out of bounds wont be undefined if search has been expanded.
func (s *FieldGetterTestSuite) TestExpandedOutOfBounds() {
	doc := data.M{
		"planets": []any{
			data.M{
				"value": []any{
					"Not an object 1",
					"Not an object 2",
					"Not an object 3",
				},
			},
		},
	}

	dv, expanded, err := s.fg.GetField(doc, "planets", "value", "5")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	value, isSet := dv[0].Get()
	s.Nil(value)
	s.False(isSet)
}

func (s *FieldGetterTestSuite) TestStopExpansion() {
	doc := data.M{
		"ducks": []any{
			[]any{
				data.M{
					"name": "Huguinho",
				},
				data.M{
					"name": "Zezinho",
				},
				data.M{
					"name": "Luisinho",
				},
			},
			data.M{
				"name": "Donald",
			},
		},
	}
	dv, expanded, err := s.fg.GetField(doc, "ducks", "name")
	s.NoError(err)
	s.True(expanded)
	s.Equal([]any{nil, "Donald"}, s.ListGetSetter(dv))

	dv, expanded, err = s.fg.GetField(doc, "ducks", "nope")
	s.NoError(err)
	s.True(expanded)
	s.Equal([]any{nil, nil}, s.ListGetSetter(dv))

}

func (s *FieldGetterTestSuite) ListGetSetter(gsl []domain.GetSetter) []any {
	res := make([]any, len(gsl))
	for n, gs := range gsl {
		res[n], _ = gs.Get()
	}
	return res
}

func TestFieldGetterTestSuite(t *testing.T) {
	suite.Run(t, new(FieldGetterTestSuite))
}
