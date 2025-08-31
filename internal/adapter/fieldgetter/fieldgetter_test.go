package fieldgetter

import (
	"testing"

	"github.com/stretchr/testify/suite"
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

	dv, defined, err := s.fg.GetField(doc, "hello")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 1)
	s.Equal("world", dv[0])

	dv, defined, err = s.fg.GetField(doc, "type.planet")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 1)
	s.Equal(true, dv[0])
}

func (s *FieldGetterTestSuite) TestNotOk() {
	doc := data.M{
		"hello": "world",
		"type": data.M{
			"planet": true,
			"blue":   true,
		},
	}

	dv, defined, err := s.fg.GetField(doc, "helloo")
	s.NoError(err)
	s.False(defined)
	s.Len(dv, 0)

	dv, defined, err = s.fg.GetField(doc, "type.plane")
	s.NoError(err)
	s.False(defined)
	s.Len(dv, 0)
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
	dv, defined, err := s.fg.GetField(doc, "planets.name")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 3)
	s.Equal([]any{"Earth", "Mars", "Pluton"}, dv)

	// nested
	dv, defined, err = s.fg.GetField(doc, "data.planets.number")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 3)
	s.Equal([]any{3, 4, 9}, dv)

	// nested arrays (should not concat)
	dv, defined, err = s.fg.GetField(doc, "planetsMultiNumber.number")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 3)
	s.Equal([]any{[]any{1, 3}, []any{7}, []any{9, 5, 1}}, dv)
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
	dv, defined, err := s.fg.GetField(doc, "planets.1")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 1)
	s.Equal(data.M{"name": "Mars", "number": 4}, dv[0])

	// out of bounds
	dv, defined, err = s.fg.GetField(doc, "planets.3")
	s.NoError(err)
	s.False(defined)
	s.Len(dv, 0)

	// nested list
	dv, defined, err = s.fg.GetField(doc, "data.planets.2")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 1)
	s.Equal(data.M{"name": "Pluton", "number": 9}, dv[0])

	// index in middle
	dv, defined, err = s.fg.GetField(doc, "data.planets.0.name")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 1)
	s.Equal("Earth", dv[0])

}

func (s *FieldGetterTestSuite) TestEmptyObject() {

	dv, defined, err := s.fg.GetField(nil, "planets.0")
	s.NoError(err)
	s.False(defined)
	s.Len(dv, 0)

}

func (s *FieldGetterTestSuite) TestUnsetFieldInList() {
	doc := data.M{
		"planets": []any{
			nil,
			nil,
			nil,
		},
	}

	dv, defined, err := s.fg.GetField(doc, "planets.name")
	s.NoError(err)
	s.True(defined)
	s.Equal([]any{nil, nil, nil}, dv)
}

func (s *FieldGetterTestSuite) TestNestedInPrimitive() {
	doc := data.M{
		"data": data.M{
			"planets": "Not an object",
		},
	}

	dv, defined, err := s.fg.GetField(doc, "data.planets.name")
	s.NoError(err)
	s.False(defined)
	s.Nil(dv)
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

	dv, defined, err := s.fg.GetField(doc, "planets.name")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 3)
	s.Equal([]any{nil, nil, nil}, dv)
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

	dv, defined, err := s.fg.GetField(doc, "planets.value.5")
	s.NoError(err)
	s.True(defined)
	s.Len(dv, 1)
	s.Equal([]any{nil}, dv)
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
	dv, defined, err := s.fg.GetField(doc, "ducks.name")
	s.NoError(err)
	s.True(defined)
	s.Equal([]any{nil, "Donald"}, dv)

	dv, defined, err = s.fg.GetField(doc, "ducks.nope")
	s.NoError(err)
	s.True(defined)
	s.Equal([]any{nil, nil}, dv)

}

func TestFieldGetterTestSuite(t *testing.T) {
	suite.Run(t, new(FieldGetterTestSuite))
}
