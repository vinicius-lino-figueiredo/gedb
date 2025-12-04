package matcher

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

var undefined Undefined

type M = data.M

type A = []any

type Undefined struct{}

func (u Undefined) Get() (any, bool) { return nil, false }

type fieldNavigatorMock struct{ mock.Mock }

// EnsureField implements domain.FieldNavigator.
func (f *fieldNavigatorMock) EnsureField(obj any, addr ...string) ([]domain.GetSetter, error) {
	call := f.Called(obj, addr)
	return call.Get(0).([]domain.GetSetter), call.Error(1)
}

// GetAddress implements domain.FieldNavigator.
func (f *fieldNavigatorMock) GetAddress(field string) ([]string, error) {
	call := f.Called(field)
	return call.Get(0).([]string), call.Error(1)
}

// GetField implements domain.FieldNavigator.
func (f *fieldNavigatorMock) GetField(obj any, addr ...string) ([]domain.GetSetter, bool, error) {
	call := f.Called(obj, addr)
	return call.Get(0).([]domain.GetSetter), call.Bool(1), call.Error(2)
}

// SplitFields implements domain.FieldNavigator.
func (f *fieldNavigatorMock) SplitFields(in string) ([]string, error) {
	call := f.Called(in)
	return call.Get(0).([]string), call.Error(1)
}

type comparerMock struct{ mock.Mock }

// Comparable implements domain.Comparer.
func (c *comparerMock) Comparable(a any, b any) bool {
	return c.Called(a, b).Bool(0)
}

// Compare implements domain.Comparer.
func (c *comparerMock) Compare(a any, b any) (int, error) {
	call := c.Called(a, b)
	return call.Int(0), call.Error(1)
}

type MatcherTestSuite struct {
	suite.Suite
	mtchr *Matcher
}

// Can find documents with simple fields.
func (s *MatcherTestSuite) TestSimpleFieldEquality() {
	s.NotMatches(s.mtchr.Match(M{"test": "yeah"}, M{"test": "yea"}))
	s.NotMatches(s.mtchr.Match(M{"test": "yeah"}, M{"test": "yeahh"}))
	s.Matches(s.mtchr.Match(M{"test": "yeah"}, M{"test": "yeah"}))
}

// Can find documents with the dot-notation.
func (s *MatcherTestSuite) TestCanFindDocumentsWithTheDotNotation() {
	s.NotMatches(s.mtchr.Match(M{"test": M{"ooo": "yeah"}}, M{"test.ooo": "yea"}))
	s.NotMatches(s.mtchr.Match(M{"test": M{"ooo": "yeah"}}, M{"test.oo": "yeah"}))
	s.NotMatches(s.mtchr.Match(M{"test": M{"ooo": "yeah"}}, M{"tst.ooo": "yeah"}))
	s.Matches(s.mtchr.Match(M{"test": M{"ooo": "yeah"}}, M{"test.ooo": "yeah"}))
}

// Cannot find undefined.
func (s *MatcherTestSuite) TestCannotFindUndefined() {
	s.NotMatches(s.mtchr.Match(M{"test": undefined}, M{"test": undefined}))
	s.NotMatches(s.mtchr.Match(M{"test": M{"pp": undefined}}, M{"test.pp": undefined}))
}

// Nested objects are deep-equality matched and not treated as sub-queries.
func (s *MatcherTestSuite) TestNestedObjectsAreDeepEqualNotSubQuery() {
	s.Matches(s.mtchr.Match(M{"a": M{"b": 5}}, M{"a": M{"b": 5}}))
	s.NotMatches(s.mtchr.Match(M{"a": M{"b": 5, "c": 3}}, M{"a": M{"b": 5}}))

	s.NotMatches(s.mtchr.Match(M{"a": M{"b": 5}}, M{"a": M{"b": M{"$lt": 10}}}))
	s.ErrorMatch(s.mtchr.Match(M{"a": M{"b": 5}}, M{"a": M{"$or": A{M{"b": 10}, M{"b": 5}}}}))
}

// Can match for field equality inside an array with the dot notation.
func (s *MatcherTestSuite) TestInsideArrayDotNotation() {
	s.NotMatches(s.mtchr.Match(
		M{"a": true, "b": A{"node", "embedded", "database"}},
		M{"b.1": "node"},
	))
	s.Matches(s.mtchr.Match(
		M{"a": true, "b": A{"node", "embedded", "database"}},
		M{"b.1": "embedded"},
	))
	s.NotMatches(s.mtchr.Match(
		M{"a": true, "b": A{"node", "embedded", "database"}},
		M{"b.1": "database"},
	))
}

// Will return error if GetAddress fails.
func (s *MatcherTestSuite) TestFailedGetAddress() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)
	fn.On("GetAddress", "a").
		Return([]string{}, fmt.Errorf("error")).
		Once()
	s.ErrorMatch(s.mtchr.Match(M{"a": 1}, M{"a": 1}))
	fn.AssertExpectations(s.T())
}

// Will return error if matching two invalid types.
func (s *MatcherTestSuite) TestEqualInvalidTypes() {
	s.ErrorMatch(s.mtchr.Match(M{"a": []string{}}, M{"a": make(chan int)}))
}

// Will return error if matching two invalid types nested in arrays.
func (s *MatcherTestSuite) TestEqualInvalidTypesInArray() {
	s.ErrorMatch(s.mtchr.Match(
		M{"a": A{[]string{}}},
		M{"a": A{make(chan int)}},
	))
}

// Will return error if GetField fails.
func (s *MatcherTestSuite) TestFailedGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)
	fn.On("GetAddress", "a").
		Return([]string{"a"}, nil).
		Once()
	fn.On("GetField", M{"a": 1}, []string{"a"}).
		Return([]domain.GetSetter{}, false, fmt.Errorf("error")).
		Once()
	s.ErrorMatch(s.mtchr.Match(M{"a": 1}, M{"a": 1}))
	fn.AssertExpectations(s.T())
}

// Matching a non-string to a regular expression always yields false.
func (s *MatcherTestSuite) TestRegexNonString() {
	d := time.Now()
	r := regexp.MustCompile(regexp.QuoteMeta(d.String()))

	s.NotMatches(s.mtchr.Match(M{"test": true}, M{"test": regexp.MustCompile(`true`)}))
	s.NotMatches(s.mtchr.Match(M{"test": nil}, M{"test": regexp.MustCompile(`nil`)}))
	s.NotMatches(s.mtchr.Match(M{"test": 42}, M{"test": regexp.MustCompile(`42`)}))
	s.NotMatches(s.mtchr.Match(M{"test": d}, M{"test": r}))
}

// Will not match if using regex in an unset field, but will not return error.
func (s *MatcherTestSuite) TestRegexUndefined() {
	s.NotMatches(s.mtchr.Match(M{}, M{"test": regexp.MustCompile(`^a$ `)}))
}

// Regular expression matching.
func (s *MatcherTestSuite) TestMatchBasicQueryStringRegex() {
	s.Matches(s.mtchr.Match(M{"test": "true"}, M{"test": regexp.MustCompile(`true`)}))
	s.Matches(s.mtchr.Match(M{"test": "babaaaar"}, M{"test": regexp.MustCompile(`aba+r`)}))
	s.NotMatches(s.mtchr.Match(M{"test": "babaaaar"}, M{"test": regexp.MustCompile(`^aba+r`)}))
	s.NotMatches(s.mtchr.Match(M{"test": "true"}, M{"test": regexp.MustCompile(`t[ru]e`)}))
}

// Can match strings using the $regex operator.
func (s *MatcherTestSuite) TestMatchStringRegexOperator() {
	s.Matches(s.mtchr.Match(M{"test": "true"}, M{"test": M{"$regex": regexp.MustCompile(`true`)}}))
	s.Matches(s.mtchr.Match(M{"test": "babaaaar"}, M{"test": M{"$regex": regexp.MustCompile(`aba+r`)}}))
	s.NotMatches(s.mtchr.Match(M{"test": "babaaaar"}, M{"test": M{"$regex": regexp.MustCompile(`^aba+r`)}}))
	s.NotMatches(s.mtchr.Match(M{"test": "true"}, M{"test": M{"$regex": regexp.MustCompile(`t[ru]e`)}}))
}

// Will throw if $regex operator is used with a non regex value.
func (s *MatcherTestSuite) TestNonRegexInOperator() {
	s.ErrorMatch(s.mtchr.Match(
		M{"test": "true"},
		M{"test": M{
			"$regex": 42,
		}}),
	)
	s.ErrorMatch(s.mtchr.Match(
		M{"test": "true"},
		M{"test": M{
			"$regex": "true",
		}}),
	)
}

// Can use the $regex operator in conjunction with other operators.
func (s *MatcherTestSuite) TestRegexWithOtherOps() {
	s.Matches(s.mtchr.Match(
		M{"test": "helLo"},
		M{"test": M{
			"$regex": regexp.MustCompile(`(?i)ll`),
			"$nin":   A{"helL", "helLop"},
		}}),
	)
	s.NotMatches(s.mtchr.Match(
		M{"test": "helLo"},
		M{"test": M{
			"$regex": regexp.MustCompile(`(?i)ll`),
			"$nin":   A{"helLo", "helLop"},
		}}),
	)
}

// Cannot compare a field to an object, an array, null or a boolean, it will
// return false.
func (s *MatcherTestSuite) TestFieldLowerThanNonPrimitive() {
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lt": M{"a": 6}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lt": A{6, 7}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lt": nil}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lt": true}}))
}

// Can compare numbers, with or without dot notation.
func (s *MatcherTestSuite) TestLowerThanNumbers() {
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lt": 6}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lt": 5}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lt": 4}}))

	s.Matches(s.mtchr.Match(M{"a": M{"b": 5}}, M{"a.b": M{"$lt": 6}}))
	s.NotMatches(s.mtchr.Match(M{"a": M{"b": 5}}, M{"a.b": M{"$lt": 3}}))
}

// Can compare strings, with or without dot notation.
func (s *MatcherTestSuite) TestLowerThanStrings() {
	s.Matches(s.mtchr.Match(
		M{"a": "gedb"},
		M{"a": M{"$lt": "gedc"}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"a": "gedb"},
		M{"a": M{"$lt": "geda"}},
	))

	s.Matches(s.mtchr.Match(
		M{"a": M{"b": "gedb"}},
		M{"a.b": M{"$lt": "gedc"}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"a": M{"b": "gedb"}},
		M{"a.b": M{"$lt": "geda"}},
	))
}

// If field is an array field, a match means a match on at least one element.
func (s *MatcherTestSuite) TestLowerThanLooksUpArrayItems() {
	s.NotMatches(s.mtchr.Match(M{"a": A{5, 10}}, M{"a": M{"$lt": 4}}))
	s.Matches(s.mtchr.Match(M{"a": A{5, 10}}, M{"a": M{"$lt": 6}}))
	s.Matches(s.mtchr.Match(M{"a": A{5, 10}}, M{"a": M{"$lt": 11}}))
}

// Works with dates too.
func (s *MatcherTestSuite) TestLowerThanDates() {
	date1000 := time.UnixMilli(1000)
	date1001 := time.UnixMilli(1001)

	s.NotMatches(s.mtchr.Match(M{"a": date1000}, M{"a": M{"$gte": date1001}}))
	s.Matches(s.mtchr.Match(M{"a": date1000}, M{"a": M{"$lt": date1001}}))

}

// $lte.
func (s *MatcherTestSuite) TestLowerThanOrEqual() {
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lte": 6}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lte": 5}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$lte": 4}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}, M{"a": M{"$lte": []int{}}}))
}

// $gt.
func (s *MatcherTestSuite) TestGreaterThan() {
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$gt": 6}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$gt": 5}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$gt": 4}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}, M{"a": M{"$gt": []int{}}}))
}

// $gte.
func (s *MatcherTestSuite) TestGreaterThanOrEqual() {
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$gte": 6}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$gte": 5}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$gte": 4}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}, M{"a": M{"$gte": []int{}}}))
}

// $ne.
func (s *MatcherTestSuite) TestNotEqual() {
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$ne": 6}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$ne": 5}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$ne": 4}}))
	s.NotMatches(s.mtchr.Match(M{"a": false}, M{"a": M{"$ne": false}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}, M{"a": M{"$ne": []int{}}}))
}

// $in.
func (s *MatcherTestSuite) TestIn() {
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$in": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 6}, M{"a": M{"$in": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 7}, M{"a": M{"$in": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 8}, M{"a": M{"$in": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 9}, M{"a": M{"$in": A{6, 8, 9}}}))

	s.ErrorMatch(s.mtchr.Match(M{"a": 5}, M{"a": M{"$in": 5}}))

	s.NotMatches(s.mtchr.Match(M{"a": []int{}}, M{"a": M{"$in": A{2}}}))
}

// $nin.
func (s *MatcherTestSuite) TestNin() {
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$nin": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 6}, M{"a": M{"$nin": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 7}, M{"a": M{"$nin": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 8}, M{"a": M{"$nin": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 9}, M{"a": M{"$nin": A{6, 8, 9}}}))

	s.Matches(s.mtchr.Match(M{"a": 9}, M{"b": M{"$nin": A{6, 8, 9}}}))

	s.ErrorMatch(s.mtchr.Match(M{"a": 5}, M{"a": M{"$nin": 5}}))

	// fails if not using valid types
	s.ErrorMatch(s.mtchr.Match(
		M{"a": make(chan int)},
		M{"a": M{"$nin": A{[]int{}}}},
	))
}

// $exists.
func (s *MatcherTestSuite) TestExists() {
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": 1}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": true}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": time.Now()}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": ""}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": A{}}}))
	s.Matches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": M{}}}))

	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": 0}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": false}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": nil}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"a": M{"$exists": undefined}}))

	s.NotMatches(s.mtchr.Match(M{"a": 5}, M{"b": M{"$exists": true}}))

	s.Matches(s.mtchr.Match(M{"a": 5}, M{"b": M{"$exists": false}}))
}

// Will return error if FieldNavigator fails getting field value.
func (s *MatcherTestSuite) TestExistsFailGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	fn.On("GetAddress", "needAKey").
		Return([]string{"needAKey"}, nil).
		Once()

	fn.On("GetField", M{"needAKey": A{}}, []string{"needAKey"}).
		Return([]domain.GetSetter{}, false, fmt.Errorf("error")).
		Once()

	s.ErrorMatch(s.mtchr.Match(A{}, M{"$exists": M{}}))
}

func (s *MatcherTestSuite) TestCompareError() {

	c := new(comparerMock)
	s.mtchr = NewMatcher(WithComparer(c)).(*Matcher)

	c.On("Comparable", mock.Anything, []int{}).
		Return(true).
		Times(5)
	c.On("Compare", mock.Anything, mock.Anything).
		Return(0, fmt.Errorf("error")).
		Times(7)

	s.ErrorMatch(s.mtchr.Match([]int{}, M{"$lt": []int{}}))
	s.ErrorMatch(s.mtchr.Match([]int{}, M{"$gte": []int{}}))
	s.ErrorMatch(s.mtchr.Match([]int{}, M{"$lte": []int{}}))
	s.ErrorMatch(s.mtchr.Match([]int{}, M{"$gt": []int{}}))
	s.ErrorMatch(s.mtchr.Match([]int{}, M{"$ne": []int{}}))
	s.ErrorMatch(s.mtchr.Match([]int{}, M{"$in": A{[]int{}}}))
	s.ErrorMatch(s.mtchr.Match([]int{}, M{"$exists": A{[]int{}}}))
}

// will return error if matchList cannot get fields.
func (s *MatcherTestSuite) TestMatchListFailGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	fn.On("GetAddress", "a").
		Return([]string{"a"}, nil).
		Once()
	fn.On("GetField", M{"a": 1}, []string{"a"}).
		Return([]domain.GetSetter{}, false, fmt.Errorf("error")).
		Once()

	s.ErrorMatch(s.mtchr.Match(
		M{"a": 1},
		M{"a": M{"$ne": 2}},
	))
	fn.AssertExpectations(s.T())
}

// Can perform a direct array match.
func (s *MatcherTestSuite) TestCompareArrays() {
	s.NotMatches(s.mtchr.Match(
		M{"planets": A{"Earth", "Mars", "Pluto"}, "something": "else"},
		M{"planets": A{"Earth", "Mars"}},
	))

	s.Matches(s.mtchr.Match(
		M{"planets": A{"Earth", "Mars", "Pluto"}, "something": "else"},
		M{"planets": A{"Earth", "Mars", "Pluto"}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"planets": A{"Earth", "Mars", "Pluto"}, "something": "else"},
		M{"planets": A{"Earth", "Pluto", "Mars"}},
	))
}

// Can query on the size of an array field.
func (s *MatcherTestSuite) TestSize() {
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$size": 0}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$size": 1}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$size": 2}},
	))

	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$size": 3}},
	))
}

// Can query on the size of a nested array field.
func (s *MatcherTestSuite) TestSizeNested() {
	s.NotMatches(s.mtchr.Match(
		M{"hello": "world",
			"description": M{
				"satellites": A{"Moon", "Hubble"},
				"diameter":   6300,
			},
		},
		M{"description.satellites": M{"$size": 0}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"hello": "world",
			"description": M{
				"satellites": A{"Moon", "Hubble"},
				"diameter":   6300,
			},
		},
		M{"description.satellites": M{"$size": 1}},
	))

	s.Matches(s.mtchr.Match(
		M{"hello": "world",
			"description": M{
				"satellites": A{"Moon", "Hubble"},
				"diameter":   6300,
			},
		},
		M{"description.satellites": M{"$size": 2}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"hello": "world",
			"description": M{
				"satellites": A{"Moon", "Hubble"},
				"diameter":   6300,
			},
		},
		M{"description.satellites": M{"$size": 3}},
	))
}

// Can query on the size of a expanded array field.
func (s *MatcherTestSuite) TestSizeExpanded() {
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.name": M{"$size": 0}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.name": M{"$size": 1}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.name": M{"$size": 2}},
	))

	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.name": M{"$size": 3}},
	))
}

// $size operator works with empty arrays.
func (s *MatcherTestSuite) TestSizeEmpty() {
	s.Matches(s.mtchr.Match(
		M{"children": A{}},
		M{"children": M{"$size": 0}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{}},
		M{"children": M{"$size": 1}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{}},
		M{"children": M{"$size": 2}},
	))
}

// Should return an error if a query operator is used without comparing to an
// integer.
func (s *MatcherTestSuite) TestSizeNonIntegerParam() {
	s.ErrorMatch(s.mtchr.Match(
		M{"children": A{1, 5}},
		M{"children": M{"$size": 1.4}},
	))

	s.ErrorMatch(s.mtchr.Match(
		M{"children": A{1, 5}},
		M{"children": M{"$size": "fdf"}},
	))

	s.ErrorMatch(s.mtchr.Match(
		M{"children": A{1, 5}},
		M{"children": M{"$size": M{"$lt": 5}}},
	))

}

// Any numeric can be used in $size.
func (s *MatcherTestSuite) TestSizeAcceptsAnyNumber() {
	two := A{
		int(2), int8(2), int16(2), int32(2), int64(2), uint(2),
		uint8(2), uint16(2), uint32(2), uint64(2), float32(2),
		float64(2),
	}
	three := A{
		int(3), int8(3), int16(3), int32(3), int64(3), uint(3),
		uint8(3), uint16(3), uint32(3), uint64(3), float32(3),
		float64(3),
	}
	doc := M{"list": A{M{"a": 0}, M{"a": 1}}}

	for n := range two {
		s.Matches(s.mtchr.Match(doc, M{"list": M{"$size": two[n]}}))
		s.NotMatches(s.mtchr.Match(doc, M{"list": M{"$size": three[n]}}))
	}
}

// Will return error if FieldNavigator fails getting field value.
func (s *MatcherTestSuite) TestSizeFailGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	fn.On("GetAddress", "needAKey").
		Return([]string{"needAKey"}, nil).
		Once()

	fn.On("GetField", M{"needAKey": A{}}, []string{"needAKey"}).
		Return([]domain.GetSetter{}, false, fmt.Errorf("error")).
		Once()

	s.ErrorMatch(s.mtchr.Match(A{}, M{"$size": M{}}))
}

// Will not count nil and undefined as len 0 array.
func (s *MatcherTestSuite) TestNilValueSize() {
	s.NotMatches(s.mtchr.Match(M{"field": nil}, M{"field": M{"$size": 0}}))
	s.NotMatches(s.mtchr.Match(M{"nope": nil}, M{"field": M{"$size": 0}}))
}

// Will return error if receives a non-integer number for $size.
func (s *MatcherTestSuite) TestNonIntegerNumber() {
	broken := A{0.1, 0.4, 3.14, -1.99, 1.000000000001, -10.5, 7.75, 3.99999}

	doc := M{"list": A{M{"a": 0}, M{"a": 1}}}
	for _, num := range broken {
		s.ErrorMatch(s.mtchr.Match(doc, M{"list": M{"$size": num}}))
	}

}

// Using $size operator on a non-array field should prevent match but not return
// an error.
func (s *MatcherTestSuite) TestSizeNonArray() {
	s.NotMatches(s.mtchr.Match(
		M{"a": 5},
		M{"a": M{"$size": 1}},
	))
}

// NOTE: CANNOT use $size several times. This test will not be added

// Can query array documents with multiple simultaneous conditions.
func (s *MatcherTestSuite) TestElemMatch() {
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$elemMatch": M{"name": "Dewey", "age": 7}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$elemMatch": M{"name": "Dewey", "age": 12}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}},
	))

	// nested

	s.Matches(s.mtchr.Match(
		M{"outer": M{
			"children": A{
				M{"name": "Huey", "age": 3},
				M{"name": "Dewey", "age": 7},
				M{"name": "Louie", "age": 12},
			}},
		},
		M{"outer.children": M{"$elemMatch": M{"name": "Dewey", "age": 7}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"outer": M{
			"children": A{
				M{"name": "Huey", "age": 3},
				M{"name": "Dewey", "age": 7},
				M{"name": "Louie", "age": 12},
			}},
		},
		M{"outer.children": M{"$elemMatch": M{"name": "Dewey", "age": 12}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"outer": M{
			"children": A{
				M{"name": "Huey", "age": 3},
				M{"name": "Dewey", "age": 7},
				M{"name": "Louie", "age": 12},
			}},
		},
		M{"outer.children": M{"$elemMatch": M{"name": "Louie", "age": 3}}},
	))

	// non array

	s.NotMatches(s.mtchr.Match(
		M{"children": nil},
		M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"children": "not an array"},
		M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"children": M{}},
		M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"children": 123},
		M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}},
	))
}

// $elemMatch operator works with empty arrays.
func (s *MatcherTestSuite) TestElemMatchEmptyArray() {
	s.NotMatches(s.mtchr.Match(
		M{"children": A{}},
		M{"children": M{"$elemMatch": M{"name": "Mitsos"}}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{}},
		M{"children": M{"$elemMatch": M{}}},
	))
}

// Will return error if FieldNavigator fails getting field value.
func (s *MatcherTestSuite) TestElemMatchFailGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	fn.On("GetAddress", "needAKey").
		Return([]string{"needAKey"}, nil).
		Once()

	fn.On("GetField", M{"needAKey": A{}}, []string{"needAKey"}).
		Return([]domain.GetSetter{}, false, fmt.Errorf("error")).
		Once()

	s.ErrorMatch(s.mtchr.Match(A{}, M{"$elemMatch": M{}}))
}

// Can use more complex comparisons inside nested query documents.
func (s *MatcherTestSuite) TestElemMatchComplex() {
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$elemMatch": M{"name": "Dewey", "age": M{"$gt": 6, "$lt": 8}}}},
	))

	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$elemMatch": M{"name": "Dewey", "age": M{"$in": A{6, 7, 8}}}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$elemMatch": M{"name": "Dewey", "age": M{"$gt": 6, "$lt": 7}}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children": M{"$elemMatch": M{"name": "Louie", "age": M{"$gt": 6, "$lte": 7}}}},
	))
}

// Any of the subqueries should match for an $or to match.
func (s *MatcherTestSuite) TestOr() {
	s.Matches(s.mtchr.Match(
		M{"hello": "world"},
		M{"$or": A{M{"hello": "pluton"}, M{"hello": "world"}}},
	))

	s.Matches(s.mtchr.Match(
		M{"hello": "pluton"},
		M{"$or": A{M{"hello": "pluton"}, M{"hello": "world"}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"hello": "nope"},
		M{"$or": A{M{"hello": "pluton"}, M{"hello": "world"}}},
	))

	s.Matches(s.mtchr.Match(
		M{"hello": "nope", "age": 15},
		M{"$or": A{M{"hello": "pluton"}, M{"age": M{"$lt": 20}}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"hello": "nope", "age": 15},
		M{"$or": A{M{"hello": "pluton"}, M{"age": M{"$lt": 10}}}},
	))
}

// All of the subqueries should match for an $and to match.
func (s *MatcherTestSuite) TestAnd() {
	s.Matches(s.mtchr.Match(
		M{"hello": "world", "age": 15},
		M{"$and": A{M{"age": 15}, M{"hello": "world"}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"hello": "world", "age": 15},
		M{"$and": A{M{"age": 16}, M{"hello": "world"}}},
	))

	s.Matches(s.mtchr.Match(
		M{"hello": "world", "age": 15},
		M{"$and": A{M{"hello": "world"}, M{"age": M{"$lt": 20}}}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"hello": "world", "age": 15},
		M{"$and": A{M{"hello": "pluton"}, M{"age": M{"$lt": 20}}}},
	))
}

// Subquery should not match for a $not to match.
func (s *MatcherTestSuite) TestNot() {
	s.Matches(s.mtchr.Match(
		M{"a": 5, "b": 10},
		M{"a": 5},
	))
	s.NotMatches(s.mtchr.Match(
		M{"a": 5, "b": 10},
		M{"$not": M{"a": 5}},
	))
	s.ErrorMatch(s.mtchr.Match(
		M{"a": 5, "b": 10},
		M{"$not": M{"a": M{"$in": 5}}},
	))
}

// Logical operators are all top-level, only other logical operators can be
// above.
func (s *MatcherTestSuite) TestLogicalOperatorsTopLevel() {
	s.ErrorMatch(s.mtchr.Match(
		M{"a": M{"b": 7}},
		M{"a": M{"$or": A{M{"b": 5}, M{"b": 7}}}},
	))
	s.Matches(s.mtchr.Match(
		M{"a": M{"b": 7}},
		M{"$or": A{M{"a.b": 5}, M{"a.b": 7}}},
	))
}

// Logical operators can be combined as long as they are on top of the decision
// tree.
func (s *MatcherTestSuite) TestMultipleLogicalOps() {
	s.Matches(s.mtchr.Match(
		M{"a": 5, "b": 7, "c": 12},
		M{"$or": A{
			M{"$and": A{
				M{"a": 5},
				M{"b": 8},
			}},
			M{"$and": A{
				M{"a": 5},
				M{"c": M{"$lt": 40}},
			}},
		}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"a": 5, "b": 7, "c": 12},
		M{"$or": A{
			M{"$and": A{
				M{"a": 5},
				M{"b": 8},
			}},
			M{"$and": A{
				M{"a": 5},
				M{"c": M{"$lt": 10}},
			}},
		}},
	))
}

// Should throw an error if a logical operator is used without an array or if an
// unknown logical operator is used.
func (s *MatcherTestSuite) TestLogicOpError() {
	s.ErrorMatch(s.mtchr.Match(M{"a": 5}, M{"$or": M{"a": 5}}))
	s.ErrorMatch(s.mtchr.Match(M{"a": 5}, M{"$and": M{"a": 5}}))
	s.ErrorMatch(s.mtchr.Match(M{"a": 5}, M{"$unknown": A{M{"a": 5}}}))
}

// Function should match and not match correctly.
func (s *MatcherTestSuite) TestWhere() {
	s.Matches(s.mtchr.Match(
		M{"a": 4},
		M{"$where": func(doc domain.Document) (bool, error) {
			return doc.Get("a") == 4, nil
		}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"a": 4},
		M{"$where": func(doc domain.Document) (bool, error) {
			return doc.Get("a") == 5, nil
		}},
	))
}

// Should throw an error if the $where function is not, in fact, a function.
func (s *MatcherTestSuite) TestWhereNotAFunction() {
	s.ErrorMatch(s.mtchr.Match(M{"a": 4}, M{"$where": "not a function"}))
}

// Should throw an error if the $where function returns a non-boolean.
func (s *MatcherTestSuite) TestWhereNonBoolean() {
	s.ErrorMatch(s.mtchr.Match(
		M{"a": 4},
		M{"$where": func(domain.Document) string {
			return "not a boolean"
		}},
	))
}

// Should be able to do the complex matching it must be used for.
func (s *MatcherTestSuite) TestWhereComplexMatching() {
	checkEmail := func(doc domain.Document) bool {
		if !doc.Has("firstName") || !doc.Has("lastName") {
			return false
		}
		fn, ok := doc.Get("firstName").(string)
		if !ok {
			return false
		}
		ln, ok := doc.Get("lastName").(string)
		if !ok {
			return false
		}
		email, ok := doc.Get("email").(string)
		if !ok {
			return false
		}
		return strings.ToLower(fn)+"."+strings.ToLower(ln)+"@mail.com" == email
	}

	s.Matches(s.mtchr.Match(
		M{
			"firstName": "John",
			"lastName":  "Doe",
			"email":     "john.doe@mail.com",
		},
		M{"$where": checkEmail},
	))

	s.Matches(s.mtchr.Match(
		M{
			"firstName": "john",
			"lastName":  "doe",
			"email":     "john.doe@mail.com",
		},
		M{"$where": checkEmail},
	))

	s.NotMatches(s.mtchr.Match(
		M{
			"firstName": "Jane",
			"lastName":  "Doe",
			"email":     "john.doe@mail.com",
		},
		M{"$where": checkEmail},
	))

	s.NotMatches(s.mtchr.Match(
		M{
			"firstName": "John",
			"lastName":  "Deere",
			"email":     "john.doe@mail.com",
		},
		M{"$where": checkEmail},
	))

	s.NotMatches(s.mtchr.Match(
		M{"lastName": "Deere", "email": "john.doe@mail.com"},
		M{"$where": checkEmail},
	))
}

// Array field equality.
func (s *MatcherTestSuite) TestArrayFieldEquality() {
	s.NotMatches(s.mtchr.Match(
		M{"tags": A{"go", "embedded", "db"}},
		M{"tags": "python"},
	))

	s.NotMatches(s.mtchr.Match(
		M{"tags": A{"go", "embedded", "db"}},
		M{"tagss": "go"},
	))

	s.Matches(s.mtchr.Match(
		M{"tags": A{"go", "embedded", "db"}},
		M{"tags": "go"},
	))

	// NOTE: duplicate key test here. not adding

	s.Matches(s.mtchr.Match(
		M{"tags": A{"go", "embedded", "db"}, "gedb": true},
		M{"tags": "go", "gedb": true},
	))

	s.Matches(s.mtchr.Match(
		M{"number": 5, "data": M{"tags": A{"go", "embedded", "db"}}},
		M{"data.tags": "go"},
	))

	s.NotMatches(s.mtchr.Match(
		M{"number": 5, "data": M{"tags": A{"go", "embedded", "db"}}},
		M{"data.tags": "g"},
	))
}

// Array fields with one comparison operator.
func (s *MatcherTestSuite) TestArrayOneComparisonOp() {
	s.NotMatches(s.mtchr.Match(
		M{"ages": A{3, 7, 12}},
		M{"ages": M{"$lt": 2}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"ages": A{3, 7, 12}},
		M{"ages": M{"$lt": 3}},
	))
	s.Matches(s.mtchr.Match(
		M{"ages": A{3, 7, 12}},
		M{"ages": M{"$lt": 4}},
	))
	s.Matches(s.mtchr.Match(
		M{"ages": A{3, 7, 12}},
		M{"ages": M{"$lt": 8}},
	))
	s.Matches(s.mtchr.Match(
		M{"ages": A{3, 7, 12}},
		M{"ages": M{"$lt": 13}},
	))
}

// Array fields work with arrays that are in subdocuments.
func (s *MatcherTestSuite) TestArraySubDoc() {
	s.NotMatches(s.mtchr.Match(
		M{"children": M{"ages": A{3, 7, 12}}},
		M{"children.ages": M{"$lt": 2}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"children": M{"ages": A{3, 7, 12}}},
		M{"children.ages": M{"$lt": 3}},
	))
	s.Matches(s.mtchr.Match(
		M{"children": M{"ages": A{3, 7, 12}}},
		M{"children.ages": M{"$lt": 4}},
	))
	s.Matches(s.mtchr.Match(
		M{"children": M{"ages": A{3, 7, 12}}},
		M{"children.ages": M{"$lt": 8}},
	))
	s.Matches(s.mtchr.Match(
		M{"children": M{"ages": A{3, 7, 12}}},
		M{"children.ages": M{"$lt": 13}},
	))
}

// Can query inside arrays thanks to dot notation.
func (s *MatcherTestSuite) TestQueryInsideArray() {
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.age": M{"$lt": 2}},
	))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.age": M{"$lt": 3}},
	))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.age": M{"$lt": 4}},
	))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.age": M{"$lt": 5}},
	))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.age": M{"$lt": 13}},
	))

	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.name": "Lois"},
	))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.name": "Louie"},
	))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.name": "Lewi"},
	))
}

// Can query for a specific element inside arrays thanks to dot notation.
func (s *MatcherTestSuite) TestMatchArrayOnIndex() {
	doc := M{"children": A{
		M{"name": "Huey", "age": 3},
		M{"name": "Dewey", "age": 7},
		M{"name": "Louie", "age": 12},
	}}
	s.NotMatches(s.mtchr.Match(doc, M{"children.0.name": "Louie"}))
	s.NotMatches(s.mtchr.Match(doc, M{"children.1.name": "Louie"}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.2.name": "Louie"},
	))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
		M{"children.3.name": "Louie"},
	))
}

// A single array-specific operator and the query is treated as array specific.
func (s *MatcherTestSuite) TestArraySpecificQuery() {
	s.ErrorMatch(s.mtchr.Match(
		M{"children": A{"Huguinho", "Zezinho", "Luisinho"}},
		M{"children": M{"Dewey": true, "$size": 3}},
	))
}

// Can mix queries on array fields and non array filds with array specific
// operators.
func (s *MatcherTestSuite) TestMixArrayAndNonArrayOps() {
	doc := M{
		"uncle":   "Donald",
		"nephews": A{"Huguinho", "Zezinho", "Luisinho"},
	}

	s.NotMatches(s.mtchr.Match(
		doc,
		M{"nephews": M{"$size": 2}, "uncle": "Donald"},
	))
	s.Matches(s.mtchr.Match(
		doc,
		M{"nephews": M{"$size": 3}, "uncle": "Donald"},
	))
	s.NotMatches(s.mtchr.Match(
		doc,
		M{"nephews": M{"$size": 4}, "uncle": "Donald"},
	))

	s.NotMatches(s.mtchr.Match(
		doc,
		M{"nephews": M{"$size": 3}, "uncle": "Patinhas"},
	))
	s.Matches(s.mtchr.Match(
		doc,
		M{"nephews": M{"$size": 3}, "uncle": "Donald"},
	))
	s.NotMatches(s.mtchr.Match(
		doc,
		M{"nephews": M{"$size": 3}, "uncle": "Margarida"},
	))
}

// can match non-doc queries.
func (s *MatcherTestSuite) TestNonDocMatch() {
	// $regex
	s.Matches(s.mtchr.Match("a", M{"$regex": regexp.MustCompile(`^a$`)}))
	s.NotMatches(s.mtchr.Match("a", M{"$regex": regexp.MustCompile(`^b$`)}))

	// $nin
	s.Matches(s.mtchr.Match(12, M{"$nin": A{11, 13, 15}}))
	s.NotMatches(s.mtchr.Match(12, M{"$nin": A{11, 12, 13}}))

	// $lt
	s.Matches(s.mtchr.Match(12, M{"$lt": 13}))
	s.NotMatches(s.mtchr.Match(12, M{"$lt": 12}))

	// $gte
	s.Matches(s.mtchr.Match(12, M{"$gte": 12}))
	s.NotMatches(s.mtchr.Match(12, M{"$gte": 13}))

	// $lte
	s.Matches(s.mtchr.Match(12, M{"$lte": 12}))
	s.NotMatches(s.mtchr.Match(12, M{"$lte": 11}))

	// $gt
	s.Matches(s.mtchr.Match(12, M{"$gt": 11}))
	s.NotMatches(s.mtchr.Match(12, M{"$gt": 12}))

	// $ne
	s.Matches(s.mtchr.Match(12, M{"$ne": 11}))
	s.NotMatches(s.mtchr.Match(12, M{"$ne": 12}))

	// $in
	s.Matches(s.mtchr.Match(12, M{"$in": A{11, 12, 13}}))
	s.NotMatches(s.mtchr.Match(12, M{"$in": A{11, 13, 15}}))

	// "$exists":    m.exists,
	s.Matches(s.mtchr.Match(12, M{"$exists": true}))
	s.NotMatches(s.mtchr.Match(12, M{"$exists": false}))

	// "$size":      m.size,
	s.Matches(s.mtchr.Match(A{1, 2}, M{"$size": 2}))
	s.NotMatches(s.mtchr.Match(A{1, 2, 3}, M{"$size": 2}))
	s.ErrorMatch(s.mtchr.Match(A{1, 2, 3}, M{"$size": false}))

	// "$elemMatch": m.elemMatch,
	s.Matches(s.mtchr.Match(A{1, 2}, M{"$elemMatch": 2}))
	s.NotMatches(s.mtchr.Match(A{1, 2, 3}, M{"$elemMatch": 4}))
}

// Will not match query if second argument is not a doc.
func (s *MatcherTestSuite) TestNonDocQuery() {
	s.NotMatches(s.mtchr.Match(M{"a": "value"}, "a"))
	s.NotMatches(s.mtchr.Match(M{"a": "value"}, "value"))
	s.NotMatches(s.mtchr.Match(M{"you": M{"expected": "a"}}, "test"))
	s.NotMatches(s.mtchr.Match(M{"but": A{"it", "was", "me"}}, "dio"))
}

func (s *MatcherTestSuite) TestNonDocFailNewDoc() {
	// should work by default
	s.Matches(s.mtchr.Match("a", "a"))

	df := func(any) (domain.Document, error) {
		return nil, fmt.Errorf("error")
	}
	s.mtchr = NewMatcher(WithDocumentFactory(df)).(*Matcher)

	// should error if document factory fails for the object
	s.ErrorMatch(s.mtchr.Match("a", "a"))

	shouldErr := false
	df = func(v any) (domain.Document, error) {
		if !shouldErr {
			shouldErr = true
			return data.NewDocument(v)
		}
		return nil, fmt.Errorf("error")
	}

	s.mtchr = NewMatcher(WithDocumentFactory(df)).(*Matcher)

	// should error if document factory fails for the query as well
	s.ErrorMatch(s.mtchr.Match("a", "a"))
}

// cannot mix normal fields and operators in queries.
func (s *MatcherTestSuite) TestMixOperators() {
	// without operator works
	s.Matches(s.mtchr.Match(
		M{"a": 1},
		M{"a": 1},
	))

	// without operator too
	s.Matches(s.mtchr.Match(
		M{"a": 1},
		M{"$and": A{M{"a": 1}, M{"a": M{"$gt": 0}}}},
	))

	// cannot combine them
	s.ErrorMatch(s.mtchr.Match(
		M{"a": 1},
		M{"a": 1, "$and": A{M{"a": 1}, M{"a": M{"$gt": 0}}}},
	))
}

func (s *MatcherTestSuite) TestNilQuery() {
	s.Matches(s.mtchr.Match(
		"anything",
		nil,
	))
	s.Matches(s.mtchr.Match(
		[]string{"is"},
		nil,
	))
	s.Matches(s.mtchr.Match(
		func() string { return "valid" },
		nil,
	))
}

func (s *MatcherTestSuite) Matches(matches bool, err error) {
	s.NoError(err)
	s.True(matches)
}

func (s *MatcherTestSuite) NotMatches(matches bool, err error) {
	s.NoError(err)
	s.False(matches)
}

func (s *MatcherTestSuite) ErrorMatch(_ bool, err error) {
	s.Error(err)
}

func (s *MatcherTestSuite) SetupTest() {
	s.mtchr = NewMatcher().(*Matcher)
}

func TestMatcherTestSuite(t *testing.T) {
	suite.Run(t, new(MatcherTestSuite))
}
