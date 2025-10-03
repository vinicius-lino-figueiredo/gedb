package modifier

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/matcher"
)

type M = data.M
type A = []any

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

// matcherMock implements [domain.Matcher].
type matcherMock struct {
	mock.Mock
}

// Match implements [domain.Matcher].
func (g *matcherMock) Match(obj any, qry any) (bool, error) {
	call := g.Called(obj, qry)
	return call.Bool(0), call.Error(1)
}

type ModifierTestSuite struct {
	suite.Suite
	modifier *Modifier
}

func (s *ModifierTestSuite) SetupTest() {
	s.modifier = NewModifier(
		data.NewDocument,
		comparer.NewComparer(),
		fieldnavigator.NewFieldNavigator(data.NewDocument),
		matcher.NewMatcher(),
	).(*Modifier)
}

// Queries not containing any modifier just replace the document by the
// contents of the query but keep its _id
func (s *ModifierTestSuite) TestModifyDoc() {
	obj := M{"some": "thing", "_id": "keepit"}
	updateQuery := M{"replace": "done", "bloup": A{1, 8}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"replace": "done", "bloup": A{1, 8}, "_id": "keepit"}, t)
}

// Return an error if trying to change the _id field in a copy-type modification
func (s *ModifierTestSuite) TestModifyID() {
	obj := M{"some": "thing", "_id": "keepit"}
	updateQuery := M{
		"replace": "done",
		"bloup":   A{1, 8},
		"_id":     "donttry",
	}

	_, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
}

// Return an error if obj and query have invalid _id's
func (s *ModifierTestSuite) TestModifyInvalidID() {
	obj := M{"some": "thing", "_id": []string{}}
	updateQuery := M{"_id": []string{}}

	_, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
}

// Should not return error when setting unchanged _id
func (s *ModifierTestSuite) TestModifyUnchangedID() {
	obj := M{"_id": 1}
	updateQuery := M{"_id": 1}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"_id": 1}, t)
}

// Will return error if document factory fails on copying modifications
func (s *ModifierTestSuite) TestCopyWithFailedNewDoc() {
	obj := M{}
	updateQuery := M{}

	s.modifier.docFac = func(any) (domain.Document, error) {
		return nil, fmt.Errorf("")
	}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will return error if document factory fails during $unset
func (s *ModifierTestSuite) TestModifyWithFailedNewDoc() {
	obj := M{}
	updateQuery := M{"$unset": M{"a": true}}

	s.modifier.docFac = func(any) (domain.Document, error) {
		return nil, fmt.Errorf("")
	}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Return an error if trying to use modify in a mixed copy+modify way
func (s *ModifierTestSuite) TestMixCopyModify() {
	obj := M{"some": "thing"}
	updateQuery := M{"replace": "me", "$modify": "metoo"}

	_, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
}

// Return an error if trying to use an inexistent modifier
func (s *ModifierTestSuite) TestInexistentModifier() {
	obj := M{"some": "thing"}
	updateQuery := M{"$set": M{"it": "exists"}, "$modify": "not this one"}

	_, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
}

// Return an error if a modifier is used with a non-object argument
func (s *ModifierTestSuite) TestSetObjectArgument() {
	obj := M{"some": "thing"}
	updateQuery := M{"$set": "this stat"}

	_, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
}

// Can change already set fields without modifying the underlying object
func (s *ModifierTestSuite) TestSetExistentFields() {
	obj := M{"some": "thing", "yup": "yes", "nay": "noes"}
	updateQuery := M{"$set": M{"some": "changed", "nay": "yes indeed"}}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "changed", "yup": "yes", "nay": "yes indeed"}, t)

	// unchanged
	s.Equal(M{"some": "thing", "yup": "yes", "nay": "noes"}, obj)
}

// Creates fields to set if they dont exist yet
func (s *ModifierTestSuite) TestSetCreatesFields() {
	obj := M{"yup": "yes"}
	updateQuery := M{"$set": M{"some": "changed", "nay": "yes indeed"}}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"yup": "yes", "some": "changed", "nay": "yes indeed"}, t)
}

// Appends nil values to arrays if they dont exist yet
func (s *ModifierTestSuite) TestSetIncreasesArrayLength() {
	obj := M{"yup": A{0, 1}}
	updateQuery := M{"$set": M{"yup.5": 5}}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"yup": A{0, 1, nil, nil, nil, 5}}, t)
}

// Can set sub-fields and create them if necessary
func (s *ModifierTestSuite) TestSetCreatesSubFields() {
	obj := M{"yup": M{"subfield": "bloup"}}
	updateQuery := M{
		"$set": M{
			"yup.subfield":         "changed",
			"yup.yop":              "yes indeed",
			"totally.doesnt.exist": "now it does",
		},
	}
	expected := M{
		"yup": M{
			"subfield": "changed",
			"yop":      "yes indeed",
		},
		"totally": M{
			"doesnt": M{
				"exist": "now it does",
			},
		},
	}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(expected, t)

}

// Doesn't replace a falsy field by an object when recursively following dot
// notation
//
// That test is not really necessary in go, but I'm keeping it anyway
func (s *ModifierTestSuite) TestSetDoesNotReplaceFalsyValue() {
	obj := M{"nested": false}
	updateQuery := M{"$set": M{"nested.now": "it is"}}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"nested": false}, t)
}

// Will return error when EnsureField fails
func (s *ModifierTestSuite) TestSetFailedEnsure() {
	obj := M{"nested": false}
	updateQuery := M{"$set": M{"nested.now": "it is"}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("EnsureField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Can delete a field, not returning an error if the field doesnt exist
func (s *ModifierTestSuite) TestUnsetIgnoresUnsetFields() {

	obj := M{"yup": "yes", "other": "also"}

	updateQuery := M{"$unset": M{"yup": true}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"other": "also"}, t)

	updateQuery = M{"$unset": M{"nope": true}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(obj, t)

	updateQuery = M{"$unset": M{"nope": true, "other": true}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"yup": "yes"}, t)
}

// Can unset sub-fields and entire nested documents
func (s *ModifierTestSuite) TestUnsetSubfields() {
	obj := M{"yup": "yes", "nested": M{"a": "also", "b": "yeah"}}

	updateQuery := M{"$unset": M{"nested": true}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"yup": "yes"}, t)

	updateQuery = M{"$unset": M{"nested.a": true}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"yup": "yes", "nested": M{"b": "yeah"}}, t)

	updateQuery = M{"$unset": M{"nested.a": true, "nested.b": true}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"yup": "yes", "nested": M{}}, t)
}

// When unsetting nested fields, should not create an empty parent to nested
// field
func (s *ModifierTestSuite) TestUnsetDoesNotCreateEmptyParent() {

	updateQuery := M{"$unset": M{"bad.worse": true}}

	obj := M{"argh": true}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"argh": true}, t)

	obj = M{"argh": true, "bad": M{"worse": "oh"}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"argh": true, "bad": M{}}, t)

	obj = M{"argh": true, "bad": M{}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"argh": true, "bad": M{}}, t)
}

// Will not allow _id modifications in dollar field operations
func (s *ModifierTestSuite) TestUnsetID() {
	obj := M{"_id": 123}
	updateQuery := M{"$unset": M{"_id": true}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will return error if GetAddress fails during $unset
func (s *ModifierTestSuite) TestUnsetGetAddressError() {
	obj := M{"a": "b"}
	updateQuery := M{"$unset": M{"a": true}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
	fn.AssertExpectations(s.T())

}

// Will return error if GetField fails during $unset
func (s *ModifierTestSuite) TestUnsetGetFieldError() {
	obj := M{"a": "b"}
	updateQuery := M{"$unset": M{"a": true}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("GetField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), false, fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
	fn.AssertExpectations(s.T())

}

// Will return error when GetField fails
func (s *ModifierTestSuite) TestUnsetFailedGetField() {
	obj := M{"nested": false}
	updateQuery := M{"$unset": M{"nested.now": true}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("GetField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), false, fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Return an error if you try to use it with a non-number or on a non number
// field
func (s *ModifierTestSuite) TestIncNonNumberField() {

	obj := M{"some": "thing", "yup": "yes", "nay": 2}
	updateQuery := M{"$inc": M{"nay": "notanumber"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)

	obj = M{"some": "thing", "yup": "yes", "nay": "nope"}
	updateQuery = M{"$inc": M{"nay": 1}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Can increment number fields or create and initialize them if needed
func (s *ModifierTestSuite) TestIncCanCreateField() {
	obj := M{"some": "thing", "nay": 40}

	updateQuery := M{"$inc": M{"nay": 2}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	// as mentioned in docs, math operations result in floats
	s.Equal(M{"some": "thing", "nay": 42.0}, t)

	updateQuery = M{"$inc": M{"inexistent": -6}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "nay": 40, "inexistent": -6.0}, t)
}

// Works recursively
func (s *ModifierTestSuite) TestIncWorksRecursively() {
	obj := M{"some": "thing", "nay": M{"nope": 40}}
	updateQuery := M{"$inc": M{"nay.nope": -2, "blip.blop": 123}}

	expected := M{
		"some": "thing",
		"nay":  M{"nope": 38.0},
		"blip": M{"blop": 123.0},
	}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(expected, t)
}

// Can increment any numeric type
func (s *ModifierTestSuite) TestIncAnyNumber() {
	obj := M{"value": 1}

	numbers := []any{
		int(2), int8(2), int16(2), int32(2), int64(2), uint(2),
		uint8(2), uint16(2), uint32(2), uint64(2), float32(2),
		float64(2),
	}
	for _, number := range numbers {
		s.Run(fmt.Sprintf("%T", number), func() {
			updateQuery := M{"$inc": M{"value": number}}
			t, err := s.modifier.Modify(obj, updateQuery)
			s.NoError(err)
			s.Equal(M{"value": 3.0}, t)
		})
	}
}

// Will return error when EnsureField fails
func (s *ModifierTestSuite) TestIncFailedEnsure() {
	obj := M{"nested": false}
	updateQuery := M{"$inc": M{"nested.now": 1}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("EnsureField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will ignore unset fields (that cannot be ensured by fieldNavigator) when
// using $inc modifier
func (s *ModifierTestSuite) TestIncUnset() {
	obj := M{"planets": A{"earth", "mars"}}
	updateQuery := M{"$inc": M{"planets.age": 1}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"planets": A{"earth", "mars"}}, t)
}

// Can push an element to the end of an array
func (s *ModifierTestSuite) TestPushAddsToEndOfSlice() {
	obj := M{"arr": A{"hello"}}
	updateQuery := M{"$push": M{"arr": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello", "world"}}, t)
}

// Can push an element to a non-existent field and will create the array
func (s *ModifierTestSuite) TestPushCreatesUnexistentFields() {
	obj := M{}
	updateQuery := M{"$push": M{"arr": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"world"}}, t)
}

// Can push on nested fields
func (s *ModifierTestSuite) TestPushNestedFields() {
	obj := M{"arr": M{"nested": A{"hello"}}}
	updateQuery := M{"$push": M{"arr.nested": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": M{"nested": A{"hello", "world"}}}, t)

	obj = M{"arr": M{"a": 2}}
	updateQuery = M{"$push": M{"arr.nested": "world"}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": M{"a": 2, "nested": A{"world"}}}, t)
}

// Return an error if we try to push to a non-array
func (s *ModifierTestSuite) TestPushNonSlice() {
	obj := M{"arr": "hello"}
	updateQuery := M{"$push": M{"arr": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)

	obj = M{"arr": M{"nested": 45}}
	updateQuery = M{"$push": M{"arr.nested": "world"}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Can use the $each modifier to add multiple values to an array at once
func (s *ModifierTestSuite) TestPushEach() {
	obj := M{"arr": A{"hello"}}
	updateQuery := M{
		"$push": M{
			"arr": M{"$each": A{"world", "earth", "everything"}},
		},
	}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello", "world", "earth", "everything"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$each": 45}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)

	updateQuery = M{"$push": M{
		"arr": M{
			"$each": A{"world"}, "unauthorized": true},
	},
	}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Can use the $slice modifier to limit the number of array elements
func (s *ModifierTestSuite) TestPushAndSlice() {
	obj := M{"arr": A{"hello"}}

	updateQuery := M{"$push": M{"arr": M{"$each": A{"world", "earth", "everything"}, "$slice": 1}}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$each": A{"world", "earth", "everything"}, "$slice": -1}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"everything"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$each": A{"world", "earth", "everything"}, "$slice": 0}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{}}, t)

	updateQuery = M{"$push": M{"arr": M{"$each": A{"world", "earth", "everything"}, "$slice": 2}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello", "world"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$each": A{"world", "earth", "everything"}, "$slice": -2}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"earth", "everything"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$each": A{"world", "earth", "everything"}, "$slice": -20}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello", "world", "earth", "everything"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$each": A{"world", "earth", "everything"}, "$slice": 20}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello", "world", "earth", "everything"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$each": A{}, "$slice": 1}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$slice": 1}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello"}}, t)

	updateQuery = M{"$push": M{"arr": M{"$slice": 1, "unauthorized": true}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)

	updateQuery = M{"$push": M{"arr": M{"$each": A{}, "unauthorized": true}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will ignore unset fields (that cannot be ensured by fieldNavigator) when
// using $push modifier
func (s *ModifierTestSuite) TestPushUnset() {
	obj := M{"planets": A{"earth"}}
	updateQuery := M{"$push": M{"planets.satellites": "moon"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"planets": A{"earth"}}, t)
}

// Will return error when EnsureField fails
func (s *ModifierTestSuite) TestPushFailedEnsure() {
	obj := M{"nested": false}
	updateQuery := M{"$push": M{"nested.now": true}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("EnsureField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Can add an element to a set
func (s *ModifierTestSuite) TestAddToSet() {
	obj := M{"arr": A{"hello"}}

	updateQuery := M{"$addToSet": M{"arr": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello", "world"}}, t)

	updateQuery = M{"$addToSet": M{"arr": "hello"}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello"}}, t)
}

// Can add an element to a non-existent set and will create the array
func (s *ModifierTestSuite) TestAddToSetCreatesArray() {
	obj := M{"arr": A{}}
	updateQuery := M{"$addToSet": M{"arr": "world"}}

	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"world"}}, t)
}

// Return an error if we try to addToSet to a non-array
func (s *ModifierTestSuite) TestAddToSetNonArray() {
	obj := M{"arr": "hello"}
	updateQuery := M{"$addToSet": M{"arr": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Use deep-equality to check whether we can add a value to a set
func (s *ModifierTestSuite) TestAddToSetIgnoreDeepEqual() {
	obj := M{"arr": A{M{"b": 2}}}

	updateQuery := M{"$addToSet": M{"arr": M{"b": 3}}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{M{"b": 2}, M{"b": 3}}}, t)

	updateQuery = M{"$addToSet": M{"arr": M{"b": 2}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{M{"b": 2}}}, t)
}

// Can use the $each modifier to add multiple values to a set at once
func (s *ModifierTestSuite) TestAddToSetMultiple() {
	obj := M{"arr": A{"hello"}}

	updateQuery := M{"$addToSet": M{"arr": M{"$each": A{"world", "earth", "hello", "earth"}}}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello", "world", "earth"}}, t)

	updateQuery = M{"$addToSet": M{"arr": M{"$each": 45}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)

	updateQuery = M{"$addToSet": M{"arr": M{"$each": A{"world"}, "unauthorized": true}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will return error when EnsureField fails
func (s *ModifierTestSuite) TestAddToSetFailedEnsureField() {
	obj := M{"nested": false}
	updateQuery := M{"$addToSet": M{"nested.now": true}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("EnsureField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will ignore unset fields (that cannot be ensured by fieldNavigator) when
// using $addToSet modifier
func (s *ModifierTestSuite) TestAddToSetUnset() {
	obj := M{"planets": A{"earth", "mars"}}
	updateQuery := M{"$addToSet": M{"planets.age": 1}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"planets": A{"earth", "mars"}}, t)
}

// Will set []any to nil fields before starting to apply $addToSet modification
func (s *ModifierTestSuite) TestAddToSetNil() {
	obj := M{"planets": nil}
	updateQuery := M{"$addToSet": M{"planets": "earth"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"planets": A{"earth"}}, t)
}

// Will return error when both $addToSet param and one of the set items are of
// not recognized types
func (s *ModifierTestSuite) TestAddToSetInvalidType() {
	obj := M{"planets": A{[]string{}}}
	updateQuery := M{"$addToSet": M{"planets": make(chan int)}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Return an error if called on a non array, a non defined field or a non
// integer
func (s *ModifierTestSuite) TestPopUnexpectedTypes() {
	obj := M{"arr": "hello"}
	updateQuery := M{"$pop": M{"arr": 1}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)

	obj = M{"bloup": "nope"}
	updateQuery = M{"$pop": M{"arr": 1}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)

	obj = M{"bloup": A{1, 4, 8}}
	updateQuery = M{"$pop": M{"arr": true}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Can remove the first and last element of an array
func (s *ModifierTestSuite) TestPopFirstAndLast() {
	obj := M{"arr": A{1, 4, 8}}
	updateQuery := M{"$pop": M{"arr": 1}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{1, 4}}, t)

	updateQuery = M{"$pop": M{"arr": -1}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{4, 8}}, t)

	obj = M{"arr": A{}}

	updateQuery = M{"$pop": M{"arr": 1}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{}}, t)

	updateQuery = M{"$pop": M{"arr": -1}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{}}, t)
}

// Pop passing 0 as argument will have no effect
func (s *ModifierTestSuite) TestPopZero() {
	obj := M{"arr": A{0, 1, 2}}
	updateQuery := M{"$pop": M{"arr": 0}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{0, 1, 2}}, t)
}

// Will return error when GetField fails
func (s *ModifierTestSuite) TestPopFailedGetField() {
	obj := M{"nested": false}
	updateQuery := M{"$pop": M{"nested.now": 1}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("GetField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), false, fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Can remove an element from an array
func (s *ModifierTestSuite) TestPull() {
	obj := M{"arr": A{"hello", "world"}}
	updateQuery := M{"$pull": M{"arr": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello"}}, t)

	obj = M{"arr": A{"hello"}}
	updateQuery = M{"$pull": M{"arr": "world"}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello"}}, t)
}

// Can remove multiple matching elements
func (s *ModifierTestSuite) TestPullMultiple() {
	obj := M{"arr": A{"hello", "world", "hello", "world"}}
	updateQuery := M{"$pull": M{"arr": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{"hello", "hello"}}, t)
}

// Return an error if we try to pull from a non-array
func (s *ModifierTestSuite) TestPullNonArray() {
	obj := M{"arr": "hello"}
	updateQuery := M{"$pull": M{"arr": "world"}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Use deep-equality to check whether we can remove a value from an array
func (s *ModifierTestSuite) TestPullDeepEqual() {
	obj := M{"arr": A{M{"b": 2}, M{"b": 3}}}
	updateQuery := M{"$pull": M{"arr": M{"b": 3}}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{M{"b": 2}}}, t)

	obj = M{"arr": A{M{"b": 2}}}
	updateQuery = M{"$pull": M{"arr": M{"b": 3}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{M{"b": 2}}}, t)
}

// Can use any kind of nedb query with $pull
func (s *ModifierTestSuite) TestPullQuery() {
	obj := M{"arr": A{4, 7, 12, 2}, "other": "yup"}
	updateQuery := M{"$pull": M{"arr": M{"$gte": 5}}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{4, 2}, "other": "yup"}, t)

	obj = M{"arr": A{M{"b": 4}, M{"b": 7}, M{"b": 1}}, "other": "yup"}
	updateQuery = M{"$pull": M{"arr": M{"b": M{"$gte": 5}}}}
	t, err = s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"arr": A{M{"b": 4}, M{"b": 1}}, "other": "yup"}, t)
}

// Will return error when GetField fails
func (s *ModifierTestSuite) TestPullFailedGetField() {
	obj := M{"nested": false}
	updateQuery := M{"$pull": M{"nested.now": 1}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("GetField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), false, fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will return error when Match fails
func (s *ModifierTestSuite) TestPullFailedMatch() {
	obj := M{"nested": A{1}}
	updateQuery := M{"$pull": M{"nested": 1}}

	mtchr := new(matcherMock)
	s.modifier.matcher = mtchr

	mtchr.On("Match", mock.Anything, mock.Anything).
		Return(false, fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)

	mtchr.AssertExpectations(s.T())
}

// Will set the field to the updated value if value is greater than current one,
// without modifying the original object
func (s *ModifierTestSuite) TestMax() {
	obj := M{"some": "thing", "number": 10}
	updateQuery := M{"$max": M{"number": 12}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "number": 12}, t)
	s.Equal(M{"some": "thing", "number": 10}, obj)
}

// Will not update the field if new value is smaller than current one
func (s *ModifierTestSuite) TestMaxIgnoresSmaller() {
	obj := M{"some": "thing", "number": 10}
	updateQuery := M{"$max": M{"number": 9}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "number": 10}, t)
}

// Will create the field if it does not exist
func (s *ModifierTestSuite) TestMaxCreatesInexistentField() {
	obj := M{"some": "thing"}
	updateQuery := M{"$max": M{"number": 10}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "number": 10}, t)
}

// Works on embedded documents
func (s *ModifierTestSuite) TestMaxWorksOnSubDoc() {
	obj := M{"some": "thing", "somethingElse": M{"number": 10}}
	updateQuery := M{"$max": M{"somethingElse.number": 12}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "somethingElse": M{"number": 12}}, t)
}

// Will return error when EnsureField fails
func (s *ModifierTestSuite) TestMaxFailedEnsureField() {
	obj := M{"nested": false}
	updateQuery := M{"$max": M{"nested.now": 1}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("EnsureField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will fail to find $max if both values are of invalid types
func (s *ModifierTestSuite) TestMaxCompareInvalid() {
	obj := M{"some": make(chan struct{})}
	updateQuery := M{"$max": M{"some": struct{}{}}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will set the field to the updated value if value is smaller than current one,
// without modifying the original object
func (s *ModifierTestSuite) TestMin() {
	obj := M{"some": "thing", "number": 10}
	updateQuery := M{"$min": M{"number": 8}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "number": 8}, t)
	s.Equal(M{"some": "thing", "number": 10}, obj)
}

// Will not update the field if new value is greater than current one
func (s *ModifierTestSuite) TestMinIgnoresGreater() {
	obj := M{"some": "thing", "number": 10}
	updateQuery := M{"$min": M{"number": 12}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "number": 10}, t)
}

// Will create the field if it does not exist
func (s *ModifierTestSuite) TestMinCreatesInexistentField() {
	obj := M{"some": "thing"}
	updateQuery := M{"$min": M{"number": 10}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "number": 10}, t)
}

// Works on embedded documents
func (s *ModifierTestSuite) TestMinWorksOnSubDoc() {
	obj := M{"some": "thing", "somethingElse": M{"number": 10}}
	updateQuery := M{"$min": M{"somethingElse.number": 8}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.NoError(err)
	s.Equal(M{"some": "thing", "somethingElse": M{"number": 8}}, t)
}

// Will return error when EnsureField fails
func (s *ModifierTestSuite) TestMinFailedEnsureField() {
	obj := M{"nested": false}
	updateQuery := M{"$min": M{"nested.now": 1}}

	fn := new(fieldNavigatorMock)
	s.modifier.fieldNavigator = fn

	// no error
	fn.On("GetAddress", mock.Anything).
		Return(([]string)(nil), nil).
		Once()

	fn.On("EnsureField", mock.Anything, mock.Anything).
		Return(([]domain.GetSetter)(nil), fmt.Errorf("error")).
		Once()

	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will fail to find $min if both values are of invalid types
func (s *ModifierTestSuite) TestMinCompareInvalid() {
	obj := M{"some": make(chan struct{})}
	updateQuery := M{"$min": M{"some": struct{}{}}}
	t, err := s.modifier.Modify(obj, updateQuery)
	s.Error(err)
	s.Nil(t)
}

// Will not copy dollar fields
func (s *ModifierTestSuite) TestCopyDollarField() {
	obj := M{"$dollarField": "exists", "noItDoesNot": true}
	docCopy, err := s.modifier.copyDoc(obj)
	s.NoError(err)
	s.Equal(M{"noItDoesNot": true}, docCopy)
}

// Failing to create a new Document will stop doc from being copied
func (s *ModifierTestSuite) TestCopyFailDocFactory() {
	obj := M{"checks": A{M{"exists": true}}}

	counter := 0
	s.modifier.docFac = func(any) (domain.Document, error) {
		if counter == 0 {
			// by not failing the first attempt, we follow another
			// if/else path so we do not miss some test coverage
			counter++
			return M{}, nil
		}
		return nil, fmt.Errorf("first error")
	}

	docCopy, err := s.modifier.copyDoc(obj)
	s.Error(err)
	s.Nil(docCopy)
}

func TestModifierTestSuite(t *testing.T) {
	suite.Run(t, new(ModifierTestSuite))
}
