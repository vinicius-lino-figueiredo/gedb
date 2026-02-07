package fieldnavigator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

type FieldNavigatorTestSuite struct {
	suite.Suite
	fn *FieldNavigator
}

func (s *FieldNavigatorTestSuite) SetupTest() {
	s.fn = NewFieldNavigator(data.NewDocument).(*FieldNavigator)
}

func (s *FieldNavigatorTestSuite) TestFirstLevel() {
	doc := data.M{
		"hello": "world",
		"type": data.M{
			"planet": true,
			"blue":   true,
		},
	}

	dv, expanded, err := s.fn.GetField(doc, "hello")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	value, isSet := dv[0].Get()
	s.True(isSet)
	s.Equal("world", value)

	dv, expanded, err = s.fn.GetField(doc, "type", "planet")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	value, isSet = dv[0].Get()
	s.True(isSet)
	s.Equal(true, value)
}

func (s *FieldNavigatorTestSuite) TestNotOk() {
	doc := data.M{
		"hello": "world",
		"type": data.M{
			"planet": true,
			"blue":   true,
		},
	}

	dv, expanded, err := s.fn.GetField(doc, "helloo")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	_, isSet := dv[0].Get()
	s.False(isSet)

	dv, expanded, err = s.fn.GetField(doc, "type", "plane")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	_, isSet = dv[0].Get()
	s.False(isSet)
}

func (s *FieldNavigatorTestSuite) TestArray() {
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
	dv, expanded, err := s.fn.GetField(doc, "planets", "name")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	s.Equal([]any{"Earth", "Mars", "Pluton"}, s.ListGetSetter(dv))

	// nested
	dv, expanded, err = s.fn.GetField(doc, "data", "planets", "number")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	s.Equal([]any{3, 4, 9}, s.ListGetSetter(dv))

	// nested arrays (should not concat)
	dv, expanded, err = s.fn.GetField(doc, "planetsMultiNumber", "number")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	s.Equal([]any{[]any{1, 3}, []any{7}, []any{9, 5, 1}}, s.ListGetSetter(dv))
}

func (s *FieldNavigatorTestSuite) TestIndex() {
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
	dv, expanded, err := s.fn.GetField(doc, "planets", "1")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	s.Equal(data.M{"name": "Mars", "number": 4}, s.ListGetSetter(dv)[0])

	// out of bounds
	dv, expanded, err = s.fn.GetField(doc, "planets", "3")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	_, isSet := dv[0].Get()
	s.False(isSet)

	// nested list
	dv, expanded, err = s.fn.GetField(doc, "data", "planets", "2")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	s.Equal(data.M{"name": "Pluton", "number": 9}, s.ListGetSetter(dv)[0])

	// index in middle
	dv, expanded, err = s.fn.GetField(doc, "data", "planets", "0", "name")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)
	s.Equal("Earth", s.ListGetSetter(dv)[0])

}

func (s *FieldNavigatorTestSuite) TestEmptyObject() {

	dv, expanded, err := s.fn.GetField(nil, "planets", "0")
	s.NoError(err)
	s.False(expanded)
	s.Len(dv, 1)

}

func (s *FieldNavigatorTestSuite) TestUnsetFieldInList() {
	doc := data.M{
		"planets": []any{
			nil,
			nil,
			nil,
		},
	}

	dv, expanded, err := s.fn.GetField(doc, "planets", "name")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	for _, v := range dv {
		value, isSet := v.Get()
		s.Nil(value)
		s.False(isSet)
	}
}

func (s *FieldNavigatorTestSuite) TestNestedInPrimitive() {
	doc := data.M{
		"data": data.M{
			"planets": "Not an object",
		},
	}

	dv, expnded, err := s.fn.GetField(doc, "data", "planets", "name")
	s.NoError(err)
	s.False(expnded)
	s.Len(dv, 1)
	value, isSet := dv[0].Get()
	s.False(isSet)
	s.Nil(value)
}

// should always return defined when expanding list values.
func (s *FieldNavigatorTestSuite) TestReturnDefinedOnLists() {
	doc := data.M{
		"planets": []any{
			"Not an object 1",
			"Not an object 2",
			"Not an object 3",
		},
	}

	dv, expanded, err := s.fn.GetField(doc, "planets", "name")
	s.NoError(err)
	s.True(expanded)
	s.Len(dv, 3)
	for _, v := range dv {
		value, isSet := v.Get()
		s.Nil(value)
		s.False(isSet)
	}
}

func (s *FieldNavigatorTestSuite) TestStopExpansion() {
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
	dv, expanded, err := s.fn.GetField(doc, "ducks", "name")
	s.NoError(err)
	s.True(expanded)
	s.Equal([]any{nil, "Donald"}, s.ListGetSetter(dv))

	dv, expanded, err = s.fn.GetField(doc, "ducks", "nope")
	s.NoError(err)
	s.True(expanded)
	s.Equal([]any{nil, nil}, s.ListGetSetter(dv))

}

// Will return unset fields when looking for out-of-bounds index in an expanded
// address.
func (s *FieldNavigatorTestSuite) TestGetExpandedOutOfBounds() {
	obj := data.M{"existent": []any{data.M{"a": []any{}}}}

	fields, expanded, err := s.fn.GetField(obj, "existent", "a", "3")

	s.True(expanded)
	s.NoError(err)
	s.Len(fields, 1)
	value, defined := fields[0].Get()
	s.False(defined)
	s.Nil(value)

	s.Equal(data.M{"existent": []any{data.M{"a": []any{}}}}, obj)
}

// If address points to obj argument, result is a read-only field.
func (s *FieldNavigatorTestSuite) TestNoAddress() {
	obj := data.M{"yes": "indeed"}

	fields, expanded, err := s.fn.GetField(obj)
	s.NoError(err)
	s.False(expanded)
	s.Len(fields, 1)

	field := fields[0]

	// can get
	value, defined := field.Get()
	s.True(defined)
	s.Equal(obj, value)

	// cannot unset
	field.Unset()
	s.Equal(data.M{"yes": "indeed"}, obj)

	// cannot set
	field.Set(data.M{"nope": "not really"})
	s.Equal(data.M{"yes": "indeed"}, obj)
}

// Can get a dot notation address.
func (s *FieldNavigatorTestSuite) TestGetAddress() {
	r, err := s.fn.GetAddress("")
	s.NoError(err)
	s.Equal([]string{""}, r)

	r, err = s.fn.GetAddress(".")
	s.NoError(err)
	s.Equal([]string{"", ""}, r)

	r, err = s.fn.GetAddress("a.b")
	s.NoError(err)
	s.Equal([]string{"a", "b"}, r)

	r, err = s.fn.GetAddress("cd.e.fg")
	s.NoError(err)
	s.Equal([]string{"cd", "e", "fg"}, r)

	r, err = s.fn.GetAddress("h.i.j.k...l.m")
	s.NoError(err)
	s.Equal([]string{"h", "i", "j", "k", "", "", "l", "m"}, r)
}

// Can split a slice of fields without breaking dot notation.
func (s *FieldNavigatorTestSuite) TestSplitFields() {
	r, err := s.fn.SplitFields("")
	s.NoError(err)
	s.Equal([]string{""}, r)

	r, err = s.fn.SplitFields(",")
	s.NoError(err)
	s.Equal([]string{"", ""}, r)

	r, err = s.fn.SplitFields("a.b")
	s.NoError(err)
	s.Equal([]string{"a.b"}, r)

	r, err = s.fn.SplitFields("c,d.e.f,g")
	s.NoError(err)
	s.Equal([]string{"c", "d.e.f", "g"}, r)

	r, err = s.fn.SplitFields("h,i,j,k,,,l,m")
	s.NoError(err)
	s.Equal([]string{"h", "i", "j", "k", "", "", "l", "m"}, r)
}

// Will ensure an inexistent field, setting it to nil.
func (s *FieldNavigatorTestSuite) TestEnsureField() {
	obj := data.M{"existent": "yep"}

	fields, err := s.fn.EnsureField(obj, "nope")

	s.NoError(err)
	s.Len(fields, 1)
	value, defined := fields[0].Get()
	s.True(defined)
	s.Nil(value)

	s.Equal(data.M{"existent": "yep", "nope": nil}, obj)
}

// Will create nested objects if ensuring a field.
func (s *FieldNavigatorTestSuite) TestEnsureNestedField() {
	obj := data.M{"existent": "yep"}

	fields, err := s.fn.EnsureField(obj, "yes", "indeed")

	s.NoError(err)
	s.Len(fields, 1)
	value, defined := fields[0].Get()
	s.True(defined)
	s.Nil(value)

	s.Equal(data.M{"existent": "yep", "yes": data.M{"indeed": nil}}, obj)
}

// EnsureField appends to array if necessary.
func (s *FieldNavigatorTestSuite) TestEnsureArrayItem() {
	obj := data.M{"existent": []any{}}

	fields, err := s.fn.EnsureField(obj, "existent", "4")

	s.NoError(err)
	s.Len(fields, 1)
	value, defined := fields[0].Get()
	s.True(defined)
	s.Nil(value)

	s.Equal(data.M{"existent": []any{nil, nil, nil, nil, nil}}, obj)
}

// Document factory error does not affect GetField.
func (s *FieldNavigatorTestSuite) TestGetFieldDocumentFactoryError() {
	s.fn.docFac = func(any) (domain.Document, error) {
		return nil, fmt.Errorf("error")
	}
	obj := data.M{"yes": "indeed"}
	fields, expanded, err := s.fn.GetField(obj, "yes")
	s.NoError(err)
	s.False(expanded)
	s.Len(fields, 1)
	value, defined := fields[0].Get()
	s.True(defined)
	s.Equal("indeed", value)
}

// Document factory error does not affect non-nested EnsureField.
func (s *FieldNavigatorTestSuite) TestEnsureSimpleFieldDocumentFactoryError() {
	s.fn.docFac = func(any) (domain.Document, error) {
		return nil, fmt.Errorf("error")
	}
	obj := data.M{}
	fields, err := s.fn.EnsureField(obj, "yes")
	s.NoError(err)
	value, defined := fields[0].Get()
	s.True(defined)
	s.Nil(value)
}

// Document factory error makes nested EnsureField fail.
func (s *FieldNavigatorTestSuite) TestEnsureNestedFieldDocumentFactoryError() {
	errDocFac := fmt.Errorf("document factory error")
	s.fn.docFac = func(any) (domain.Document, error) {
		return nil, errDocFac
	}
	obj := data.M{}
	fields, err := s.fn.EnsureField(obj, "yes", "indeed")
	s.ErrorIs(err, errDocFac)
	s.Nil(fields)
}

func (s *FieldNavigatorTestSuite) ListGetSetter(gsl []domain.GetSetter) []any {
	res := make([]any, len(gsl))
	for n, gs := range gsl {
		res[n], _ = gs.Get()
	}
	return res
}

func TestFieldNavigatorTestSuite(t *testing.T) {
	suite.Run(t, new(FieldNavigatorTestSuite))
}
