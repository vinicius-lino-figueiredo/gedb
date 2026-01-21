package matcher

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var undefined Undefined

type M = data.M

type A = []any

type Undefined struct{}

func (u Undefined) Get() (any, bool) { return nil, false }

type GetterImpl [1]any

func (gi GetterImpl) Get() (any, bool) { return gi[0], true }

type fieldNavigatorMock struct{ mock.Mock }

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
func (f *fieldNavigatorMock) SplitFields(in string) ([]string, error) {
	call := f.Called(in)
	return call.Get(0).([]string), call.Error(1)
}

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

type MatcherTestSuite struct {
	suite.Suite
	mtchr *Matcher
}

// Can find documents with simple fields.
func (s *MatcherTestSuite) TestSimpleFieldEquality() {
	s.NoError(s.mtchr.SetQuery(M{"test": "yeah"}))

	s.NotMatches(s.mtchr.Match(M{"test": "yea"}))
	s.NotMatches(s.mtchr.Match(M{"test": "yeahh"}))
	s.Matches(s.mtchr.Match(M{"test": "yeah"}))
}

// Can find documents with the dot-notation.
func (s *MatcherTestSuite) TestCanFindDocumentsWithTheDotNotation() {
	s.NoError(s.mtchr.SetQuery(M{"test.ooo": "yea"}))
	s.NotMatches(s.mtchr.Match(M{"test": M{"ooo": "yeah"}}))

	s.NoError(s.mtchr.SetQuery(M{"test.oo": "yeah"}))
	s.NotMatches(s.mtchr.Match(M{"test": M{"ooo": "yeah"}}))

	s.NoError(s.mtchr.SetQuery(M{"tst.ooo": "yeah"}))
	s.NotMatches(s.mtchr.Match(M{"test": M{"ooo": "yeah"}}))

	s.NoError(s.mtchr.SetQuery(M{"test.ooo": "yeah"}))
	s.Matches(s.mtchr.Match(M{"test": M{"ooo": "yeah"}}))
}

// Cannot find undefined.
func (s *MatcherTestSuite) TestCannotFindUndefined() {
	s.NoError(s.mtchr.SetQuery(M{"test": undefined}))
	s.NotMatches(s.mtchr.Match(M{"test": undefined}))

	s.NoError(s.mtchr.SetQuery(M{"test.pp": undefined}))
	s.NotMatches(s.mtchr.Match(M{"test": M{"pp": undefined}}))
}

// Nested objects are deep-equality matched and not treated as sub-queries.
func (s *MatcherTestSuite) TestNestedObjectsAreDeepEqualNotSubQuery() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"b": 5}}))
	s.Matches(s.mtchr.Match(M{"a": M{"b": 5}}))
	s.NotMatches(s.mtchr.Match(M{"a": M{"b": 5, "c": 3}}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"b": M{"$lt": 10}}}))
	s.NotMatches(s.mtchr.Match(M{"a": M{"b": 5}}))

	err := s.mtchr.SetQuery(M{"a": M{"$or": A{M{"b": 10}, M{"b": 5}}}})
	s.ErrorIs(err, ErrUnknownComparison{Comparison: "$or"})
}

// Can match for field equality inside an array with the dot notation.
func (s *MatcherTestSuite) TestInsideArrayDotNotation() {
	s.NoError(s.mtchr.SetQuery(M{"b.1": "golang"}))
	s.NotMatches(s.mtchr.Match(M{"a": true, "b": A{"node", "embedded", "database"}}))

	s.NoError(s.mtchr.SetQuery(M{"b.1": "embedded"}))
	s.Matches(s.mtchr.Match(M{"a": true, "b": A{"node", "embedded", "database"}}))

	s.NoError(s.mtchr.SetQuery(M{"b.1": "database"}))
	s.NotMatches(s.mtchr.Match(M{"a": true, "b": A{"node", "embedded", "database"}}))
}

// Will return error if GetAddress fails.
func (s *MatcherTestSuite) TestFailedGetAddress() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	errGetAddr := fmt.Errorf("get address error")
	fn.On("GetAddress", "a").
		Return([]string{}, errGetAddr).
		Once()

	err := s.mtchr.SetQuery(M{"a": 1})
	s.ErrorIs(err, errGetAddr)

	fn.AssertExpectations(s.T())
}

// Will return error if matching two invalid types.
func (s *MatcherTestSuite) TestEqualInvalidTypes() {
	s.NoError(s.mtchr.SetQuery(M{"a": make(chan int)}))
	m, err := s.mtchr.Match(M{"a": []string{}})
	s.ErrorAs(err, &domain.ErrCannotCompare{})
	s.False(m)

	s.NoError(s.mtchr.SetQuery(M{"a.b": make(chan int)}))
	m, err = s.mtchr.Match(M{"a": A{M{"b": A{[]string{}}}}})
	s.ErrorAs(err, &domain.ErrCannotCompare{})
	s.False(m)

}

// Will return error if matching two invalid types nested in arrays.
func (s *MatcherTestSuite) TestEqualInvalidTypesInArray() {
	s.NoError(s.mtchr.SetQuery(M{"a": A{make(chan int)}}))
	m, err := s.mtchr.Match(M{"a": A{[]string{}}})
	s.ErrorAs(err, &domain.ErrCannotCompare{})
	s.False(m)
}

// Will return error if GetField fails.
func (s *MatcherTestSuite) TestFailedGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)
	fn.On("GetAddress", "a").
		Return([]string{"a"}, nil).
		Once()
	errGetField := fmt.Errorf("get field error")
	fn.On("GetField", M{"a": 1}, []string{"a"}).
		Return([]domain.GetSetter{}, false, errGetField).
		Once()

	s.NoError(s.mtchr.SetQuery(M{"a": 1}))
	m, err := s.mtchr.Match(M{"a": 1})
	s.ErrorIs(err, errGetField)
	s.False(m)
	fn.AssertExpectations(s.T())
}

// Matching a non-string to a regular expression always yields false.
func (s *MatcherTestSuite) TestRegexNonString() {
	d := time.Now()
	r := regexp.MustCompile(regexp.QuoteMeta(d.String()))

	s.NoError(s.mtchr.SetQuery(M{"test": regexp.MustCompile(`true`)}))
	s.NotMatches(s.mtchr.Match(M{"test": true}))

	s.NoError(s.mtchr.SetQuery(M{"test": regexp.MustCompile(`nil`)}))
	s.NotMatches(s.mtchr.Match(M{"test": nil}))

	s.NoError(s.mtchr.SetQuery(M{"test": regexp.MustCompile(`42`)}))
	s.NotMatches(s.mtchr.Match(M{"test": 42}))

	s.NoError(s.mtchr.SetQuery(M{"test": r}))
	s.NotMatches(s.mtchr.Match(M{"test": d}))
}

// Will not match if using regex in an unset field, but will not return error.
func (s *MatcherTestSuite) TestRegexUndefined() {
	s.NoError(s.mtchr.SetQuery(M{"test": regexp.MustCompile(`^a$ `)}))
	s.NotMatches(s.mtchr.Match(M{}))
}

// Regular expression matching.
func (s *MatcherTestSuite) TestMatchBasicQueryStringRegex() {
	s.NoError(s.mtchr.SetQuery(M{"test": regexp.MustCompile(`true`)}))
	s.Matches(s.mtchr.Match(M{"test": "true"}))

	s.NoError(s.mtchr.SetQuery(M{"test": regexp.MustCompile(`aba+r`)}))
	s.Matches(s.mtchr.Match(M{"test": "babaaaar"}))

	s.NoError(s.mtchr.SetQuery(M{"test": regexp.MustCompile(`^aba+r`)}))
	s.NotMatches(s.mtchr.Match(M{"test": "babaaaar"}))

	s.NoError(s.mtchr.SetQuery(M{"test": regexp.MustCompile(`t[ru]e`)}))
	s.NotMatches(s.mtchr.Match(M{"test": "true"}))
}

// Can match strings using the $regex operator.
func (s *MatcherTestSuite) TestMatchStringRegexOperator() {
	s.NoError(s.mtchr.SetQuery(M{"test": M{"$regex": regexp.MustCompile(`true`)}}))
	s.Matches(s.mtchr.Match(M{"test": "true"}))

	s.NoError(s.mtchr.SetQuery(M{"test": M{"$regex": regexp.MustCompile(`aba+r`)}}))
	s.Matches(s.mtchr.Match(M{"test": "babaaaar"}))

	s.NoError(s.mtchr.SetQuery(M{"test": M{"$regex": regexp.MustCompile(`^aba+r`)}}))
	s.NotMatches(s.mtchr.Match(M{"test": "babaaaar"}))

	s.NoError(s.mtchr.SetQuery(M{"test": M{"$regex": regexp.MustCompile(`t[ru]e`)}}))
	s.NotMatches(s.mtchr.Match(M{"test": "true"}))
}

// Will throw if $regex operator is used with a non regex value.
func (s *MatcherTestSuite) TestNonRegexInOperator() {
	s.ErrorAs(s.mtchr.SetQuery(M{"test": M{"$regex": 42}}), &ErrCompArgType{})
	s.Error(s.mtchr.SetQuery(M{"test": M{"$regex": "true"}}))
}

// Can use the $regex operator in conjunction with other operators.
func (s *MatcherTestSuite) TestRegexWithOtherOps() {

	s.NoError(s.mtchr.SetQuery(M{"test": M{
		"$regex": regexp.MustCompile(`(?i)ll`),
		"$nin":   A{"helL", "helLop"},
	}}))
	s.Matches(s.mtchr.Match(M{"test": "helLo"}))

	s.NoError(s.mtchr.SetQuery(M{"test": M{
		"$regex": regexp.MustCompile(`(?i)ll`),
		"$nin":   A{"helLo", "helLop"},
	}}))
	s.NotMatches(s.mtchr.Match(M{"test": "helLo"}))
}

// Cannot compare a field to an object, an array, null or a boolean, it will
// return false.
func (s *MatcherTestSuite) TestFieldLowerThanNonPrimitive() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": M{"a": 6}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": A{6, 7}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": nil}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": true}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))
}

// Can compare numbers, with or without dot notation.
func (s *MatcherTestSuite) TestLowerThanNumbers() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": 6}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": 5}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": 4}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a.b": M{"$lt": 6}}))
	s.Matches(s.mtchr.Match(M{"a": M{"b": 5}}))

	s.NoError(s.mtchr.SetQuery(M{"a.b": M{"$lt": 3}}))
	s.NotMatches(s.mtchr.Match(M{"a": M{"b": 5}}))
}

// Can compare strings, with or without dot notation.
func (s *MatcherTestSuite) TestLowerThanStrings() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": "gedc"}}))
	s.Matches(s.mtchr.Match(M{"a": "gedb"}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": "geda"}}))
	s.NotMatches(s.mtchr.Match(M{"a": "gedb"}))

	s.NoError(s.mtchr.SetQuery(M{"a.b": M{"$lt": "gedc"}}))
	s.Matches(s.mtchr.Match(M{"a": M{"b": "gedb"}}))

	s.NoError(s.mtchr.SetQuery(M{"a.b": M{"$lt": "geda"}}))
	s.NotMatches(s.mtchr.Match(M{"a": M{"b": "gedb"}}))
}

// If field is an array field, a match means a match on at least one element.
func (s *MatcherTestSuite) TestLowerThanLooksUpArrayItems() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": 4}}))
	s.NotMatches(s.mtchr.Match(M{"a": A{5, 10}}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": 6}}))
	s.Matches(s.mtchr.Match(M{"a": A{5, 10}}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": 11}}))
	s.Matches(s.mtchr.Match(M{"a": A{5, 10}}))
}

// Works with dates too.
func (s *MatcherTestSuite) TestLowerThanDates() {
	date1000 := time.UnixMilli(1000)
	date1001 := time.UnixMilli(1001)

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gte": date1001}}))
	s.NotMatches(s.mtchr.Match(M{"a": date1000}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": date1001}}))
	s.Matches(s.mtchr.Match(M{"a": date1000}))
}

func (s *MatcherTestSuite) TestUndefinedItem() {
	s.NoError(s.mtchr.SetQuery(M{"a.c": M{"$lt": 2}}))
	s.Matches(s.mtchr.Match(M{"a": A{M{"b": 1}, M{"c": 1}}}))

	s.NoError(s.mtchr.SetQuery(M{"a.c": M{"$lte": 1}}))
	s.Matches(s.mtchr.Match(M{"a": A{M{"b": 1}, M{"c": 1}}}))

	s.NoError(s.mtchr.SetQuery(M{"a.c": M{"$gt": 0}}))
	s.Matches(s.mtchr.Match(M{"a": A{M{"b": 1}, M{"c": 1}}}))

	s.NoError(s.mtchr.SetQuery(M{"a.c": M{"$gte": 1}}))
	s.Matches(s.mtchr.Match(M{"a": A{M{"b": 1}, M{"c": 1}}}))

}

func (s *MatcherTestSuite) TestNonComparable() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lt": 2}}))
	s.NotMatches(s.mtchr.Match(M{"a": "oops"}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lte": 1}}))
	s.NotMatches(s.mtchr.Match(M{"a": "oops"}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gt": 0}}))
	s.NotMatches(s.mtchr.Match(M{"a": "oops"}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gte": 1}}))
	s.NotMatches(s.mtchr.Match(M{"a": "oops"}))
}

// $lte.
func (s *MatcherTestSuite) TestLowerThanOrEqual() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lte": 6}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lte": 5}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lte": 4}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$lte": []int{}}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}))
}

// $gt.
func (s *MatcherTestSuite) TestGreaterThan() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gt": 6}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gt": 5}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gt": 4}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gt": []int{}}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}))
}

// $gte.
func (s *MatcherTestSuite) TestGreaterThanOrEqual() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gte": 6}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gte": 5}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gte": 4}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$gte": []int{}}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}))
}

// $ne.
func (s *MatcherTestSuite) TestNotEqual() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$ne": 6}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$ne": 5}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$ne": 4}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$ne": false}}))
	s.NotMatches(s.mtchr.Match(M{"a": false}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$ne": []int{}}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}))
}

// $in.
func (s *MatcherTestSuite) TestIn() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$in": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$in": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 6}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$in": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 7}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$in": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 8}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$in": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 9}))

	s.ErrorAs(s.mtchr.SetQuery(M{"a": M{"$in": 5}}), &ErrCompArgType{})

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$in": A{2}}}))
	s.NotMatches(s.mtchr.Match(M{"a": []int{}}))
}

// $nin.
func (s *MatcherTestSuite) TestNin() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$nin": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$nin": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 6}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$nin": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 7}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$nin": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 8}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$nin": A{6, 8, 9}}}))
	s.NotMatches(s.mtchr.Match(M{"a": 9}))

	s.NoError(s.mtchr.SetQuery(M{"b": M{"$nin": A{6, 8, 9}}}))
	s.Matches(s.mtchr.Match(M{"a": 9}))

	s.ErrorAs(s.mtchr.SetQuery(M{"a": M{"$nin": 5}}), &ErrCompArgType{})

	// fails if not using valid types
	s.ErrorAs(s.mtchr.SetQuery(M{"a": M{"$nin": make(chan int)}}), &ErrCompArgType{})
}

// $exists.
func (s *MatcherTestSuite) TestExists() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": 1}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": true}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": time.Now()}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": ""}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": A{}}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": M{}}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": 0}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": false}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": nil}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$exists": undefined}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"b": M{"$exists": true}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))

	s.NoError(s.mtchr.SetQuery(M{"b": M{"$exists": false}}))
	s.Matches(s.mtchr.Match(M{"a": 5}))
}

// Will return error if FieldNavigator fails getting field value.
func (s *MatcherTestSuite) TestExistsFailGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	fn.On("GetAddress", "needAKey").
		Return([]string{"needAKey"}, nil).
		Once()

	errGetField := fmt.Errorf("get field error")
	fn.On("GetField", M{"needAKey": A{}}, []string{"needAKey"}).
		Return([]domain.GetSetter{}, false, errGetField).
		Once()

	s.NoError(s.mtchr.SetQuery(M{"$exists": M{}}))
	m, err := s.mtchr.Match(A{})
	s.ErrorIs(err, errGetField)
	s.False(m)
}

func (s *MatcherTestSuite) TestCompareError() {
	c := new(comparerMock)
	s.mtchr = NewMatcher(WithComparer(c)).(*Matcher)

	c.On("Comparable", mock.Anything, []int{}).
		Return(true).
		Times(5)
	errCompare := fmt.Errorf("compare error")
	c.On("Compare", mock.Anything, mock.Anything).
		Return(0, errCompare).
		Times(7)

	s.NoError(s.mtchr.SetQuery(M{"$lt": []int{}}))
	m, err := s.mtchr.Match(make(chan int))
	s.ErrorIs(err, errCompare)
	s.False(m)

	s.NoError(s.mtchr.SetQuery(M{"$gte": []int{}}))
	m, err = s.mtchr.Match(make(chan int))
	s.ErrorIs(err, errCompare)
	s.False(m)

	s.NoError(s.mtchr.SetQuery(M{"$lte": []int{}}))
	m, err = s.mtchr.Match(make(chan int))
	s.ErrorIs(err, errCompare)
	s.False(m)

	s.NoError(s.mtchr.SetQuery(M{"$gt": []int{}}))
	m, err = s.mtchr.Match(make(chan int))
	s.ErrorIs(err, errCompare)
	s.False(m)

	s.NoError(s.mtchr.SetQuery(M{"$ne": []int{}}))
	m, err = s.mtchr.Match(make(chan int))
	s.ErrorIs(err, errCompare)
	s.False(m)

	s.NoError(s.mtchr.SetQuery(M{"$in": A{[]int{}}}))
	m, err = s.mtchr.Match(make(chan int))
	s.ErrorIs(err, errCompare)
	s.False(m)

	// While creating a query, value of "$exists" is compared to 0.
	s.Error(s.mtchr.SetQuery(M{"$exists": A{[]int{}}}))
}

// will return error if matchList cannot get fields.
func (s *MatcherTestSuite) TestMatchListFailGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	fn.On("GetAddress", "a").
		Return([]string{"a"}, nil).
		Once()

	errGetField := fmt.Errorf("get field error")
	fn.On("GetField", M{"a": 1}, []string{"a"}).
		Return([]domain.GetSetter{}, false, errGetField).
		Once()

	s.NoError(s.mtchr.SetQuery(M{"a": M{"$ne": 2}}))
	m, err := s.mtchr.Match(M{"a": 1})
	s.ErrorIs(err, errGetField)
	s.False(m)
	fn.AssertExpectations(s.T())
}

// Can perform a direct array match.
func (s *MatcherTestSuite) TestCompareArrays() {
	s.NoError(s.mtchr.SetQuery(M{"planets": A{"Earth", "Mars"}}))
	s.NotMatches(s.mtchr.Match(M{"planets": A{"Earth", "Mars", "Pluto"}, "something": "else"}))

	s.NoError(s.mtchr.SetQuery(M{"planets": A{"Earth", "Mars", "Pluto"}}))
	s.Matches(s.mtchr.Match(M{"planets": A{"Earth", "Mars", "Pluto"}, "something": "else"}))

	s.NoError(s.mtchr.SetQuery(M{"planets": A{"Earth", "Pluto", "Mars"}}))
	s.NotMatches(s.mtchr.Match(M{"planets": A{"Earth", "Mars", "Pluto"}, "something": "else"}))

}

// Can query on the size of an array field.
func (s *MatcherTestSuite) TestSize() {
	s.NoError(s.mtchr.SetQuery(M{"children": M{"$size": 0}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$size": 1}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$size": 2}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$size": 3}}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

}

// Can query on the size of a nested array field.
func (s *MatcherTestSuite) TestSizeNested() {
	s.NoError(s.mtchr.SetQuery(M{"description.satellites": M{"$size": 0}}))
	s.NotMatches(s.mtchr.Match(
		M{"hello": "world",
			"description": M{
				"satellites": A{"Moon", "Hubble"},
				"diameter":   6300,
			},
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"description.satellites": M{"$size": 1}}))
	s.NotMatches(s.mtchr.Match(
		M{"hello": "world",
			"description": M{
				"satellites": A{"Moon", "Hubble"},
				"diameter":   6300,
			},
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"description.satellites": M{"$size": 2}}))
	s.Matches(s.mtchr.Match(
		M{"hello": "world",
			"description": M{
				"satellites": A{"Moon", "Hubble"},
				"diameter":   6300,
			},
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"description.satellites": M{"$size": 3}}))
	s.NotMatches(s.mtchr.Match(
		M{"hello": "world",
			"description": M{
				"satellites": A{"Moon", "Hubble"},
				"diameter":   6300,
			},
		},
	))
}

// Can query on the size of a expanded array field.
func (s *MatcherTestSuite) TestSizeExpanded() {

	s.NoError(s.mtchr.SetQuery(M{"children.name": M{"$size": 0}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.name": M{"$size": 1}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.name": M{"$size": 2}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.name": M{"$size": 3}}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))
}

// $size operator works with empty arrays.
func (s *MatcherTestSuite) TestSizeEmpty() {
	s.NoError(s.mtchr.SetQuery(M{"children": M{"$size": 0}}))
	s.Matches(s.mtchr.Match(M{"children": A{}}))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$size": 1}}))
	s.NotMatches(s.mtchr.Match(M{"children": A{}}))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$size": 2}}))
	s.NotMatches(s.mtchr.Match(M{"children": A{}}))
}

// Should return an error if a query operator is used without comparing to an
// integer.
func (s *MatcherTestSuite) TestSizeNonIntegerParam() {

	s.ErrorAs(s.mtchr.SetQuery(M{"children": M{"$size": 1.4}}), &ErrCompArgType{})

	s.ErrorAs(s.mtchr.SetQuery(M{"children": M{"$size": "fdf"}}), &ErrCompArgType{})

	s.ErrorAs(s.mtchr.SetQuery(M{"children": M{"$size": M{"$lt": 5}}}), &ErrCompArgType{})
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
		s.NoError(s.mtchr.SetQuery(M{"list": M{"$size": two[n]}}))
		s.Matches(s.mtchr.Match(doc))

		s.NoError(s.mtchr.SetQuery(M{"list": M{"$size": three[n]}}))
		s.NotMatches(s.mtchr.Match(doc))
	}
}

// Will return error if FieldNavigator fails getting field value.
func (s *MatcherTestSuite) TestSizeFailGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	fn.On("GetAddress", "needAKey").
		Return([]string{"needAKey"}, nil).
		Once()

	errGetField := fmt.Errorf("get field error")
	fn.On("GetField", M{"needAKey": A{}}, []string{"needAKey"}).
		Return([]domain.GetSetter{}, false, errGetField).
		Once()

	s.NoError(s.mtchr.SetQuery(M{"$size": 0}))
	m, err := s.mtchr.Match(A{})
	s.ErrorIs(err, errGetField)
	s.False(m)
}

// Will not count nil and undefined as len 0 array.
func (s *MatcherTestSuite) TestNilValueSize() {
	s.NoError(s.mtchr.SetQuery(M{"field": M{"$size": 0}}))
	s.NotMatches(s.mtchr.Match(M{"field": nil}))

	s.NoError(s.mtchr.SetQuery(M{"field": M{"$size": 0}}))
	s.NotMatches(s.mtchr.Match(M{"nope": nil}))
}

// Will return error if receives a non-integer number for $size.
func (s *MatcherTestSuite) TestNonIntegerNumber() {
	broken := A{0.1, 0.4, 3.14, -1.99, 1.000000000001, -10.5, 7.75, 3.99999}

	for _, num := range broken {
		s.ErrorAs(s.mtchr.SetQuery(M{"list": M{"$size": num}}), &ErrCompArgType{})
	}
}

// Using $size operator on a non-array field should prevent match but not return
// an error.
func (s *MatcherTestSuite) TestSizeNonArray() {
	s.NoError(s.mtchr.SetQuery(M{"a": M{"$size": 1}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5}))
}

// NOTE: CANNOT use $size several times. This test will not be added

// Can query array documents with multiple simultaneous conditions.
func (s *MatcherTestSuite) TestElemMatch() {

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Dewey", "age": 7}}}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Dewey", "age": 12}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	// nested

	s.NoError(s.mtchr.SetQuery(M{"outer.children": M{"$elemMatch": M{"name": "Dewey", "age": 7}}}))
	s.Matches(s.mtchr.Match(
		M{"outer": M{
			"children": A{
				M{"name": "Huey", "age": 3},
				M{"name": "Dewey", "age": 7},
				M{"name": "Louie", "age": 12},
			}},
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"outer.children": M{"$elemMatch": M{"name": "Dewey", "age": 12}}}))
	s.NotMatches(s.mtchr.Match(
		M{"outer": M{
			"children": A{
				M{"name": "Huey", "age": 3},
				M{"name": "Dewey", "age": 7},
				M{"name": "Louie", "age": 12},
			}},
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"outer.children": M{"$elemMatch": M{"name": "Louie", "age": 3}}}))
	s.NotMatches(s.mtchr.Match(
		M{"outer": M{
			"children": A{
				M{"name": "Huey", "age": 3},
				M{"name": "Dewey", "age": 7},
				M{"name": "Louie", "age": 12},
			}},
		},
	))

	// non array

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": nil},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": "not an array"},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": M{}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Louie", "age": 3}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": 123},
	))

	s.ErrorIs(
		s.mtchr.SetQuery(M{"a": M{"$elemMatch": M{"$and": A{}, "b": "nop"}}}),
		ErrMixedOperators,
	)

}

// $elemMatch operator works with empty arrays.
func (s *MatcherTestSuite) TestElemMatchEmptyArray() {
	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Mitsos"}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{}}))
	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{}},
	))
}

func (s *MatcherTestSuite) TestElemMatchUndefinedItem() {
	s.NoError(s.mtchr.SetQuery(M{"a.b": M{"$elemMatch": 1}}))
	s.Matches(s.mtchr.Match(M{"a": A{M{"c": 1}, M{"b": 1}}}))
}

func (s *MatcherTestSuite) TestInUndefinedItem() {
	s.NoError(s.mtchr.SetQuery(M{"a.b": M{"$in": A{1}}}))
	s.Matches(s.mtchr.Match(M{"a": A{M{"c": 1}, M{"b": 1}}}))
}

// Will return error if FieldNavigator fails getting field value.
func (s *MatcherTestSuite) TestElemMatchFailGetField() {
	fn := new(fieldNavigatorMock)
	s.mtchr = NewMatcher(WithFieldNavigator(fn)).(*Matcher)

	fn.On("GetAddress", "needAKey").
		Return([]string{"needAKey"}, nil).
		Once()

	errGetField := fmt.Errorf("get field error")
	fn.On("GetField", M{"needAKey": A{}}, []string{"needAKey"}).
		Return([]domain.GetSetter{}, false, errGetField).
		Once()

	s.NoError(s.mtchr.SetQuery(M{"$elemMatch": M{}}))
	m, err := s.mtchr.Match(A{})
	s.ErrorIs(err, errGetField)
	s.False(m)
}

// Can use more complex comparisons inside nested query documents.
func (s *MatcherTestSuite) TestElemMatchComplex() {

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Dewey", "age": M{"$gt": 6, "$lt": 8}}}}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Dewey", "age": M{"$in": A{6, 7, 8}}}}}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Dewey", "age": M{"$gt": 6, "$lt": 7}}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children": M{"$elemMatch": M{"name": "Louie", "age": M{"$gt": 6, "$lte": 7}}}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))
}

// Any of the subqueries should match for an $or to match.
func (s *MatcherTestSuite) TestOr() {

	s.NoError(s.mtchr.SetQuery(M{"$or": A{M{"hello": "pluton"}, M{"hello": "world"}}}))
	s.Matches(s.mtchr.Match(
		M{"hello": "world"},
	))

	s.NoError(s.mtchr.SetQuery(M{"$or": A{M{"hello": "pluton"}, M{"hello": "world"}}}))
	s.Matches(s.mtchr.Match(
		M{"hello": "pluton"},
	))

	s.NoError(s.mtchr.SetQuery(M{"$or": A{M{"hello": "pluton"}, M{"hello": "world"}}}))
	s.NotMatches(s.mtchr.Match(
		M{"hello": "nope"},
	))

	s.NoError(s.mtchr.SetQuery(M{"$or": A{M{"hello": "pluton"}, M{"age": M{"$lt": 20}}}}))
	s.Matches(s.mtchr.Match(
		M{"hello": "nope", "age": 15},
	))

	s.NoError(s.mtchr.SetQuery(M{"$or": A{M{"hello": "pluton"}, M{"age": M{"$lt": 10}}}}))
	s.NotMatches(s.mtchr.Match(
		M{"hello": "nope", "age": 15},
	))
}

// All of the subqueries should match for an $and to match.
func (s *MatcherTestSuite) TestAnd() {

	s.NoError(s.mtchr.SetQuery(M{"$and": A{M{"age": 15}, M{"hello": "world"}}}))
	s.Matches(s.mtchr.Match(M{"hello": "world", "age": 15}))

	s.NoError(s.mtchr.SetQuery(M{"$and": A{M{"age": 16}, M{"hello": "world"}}}))
	s.NotMatches(s.mtchr.Match(M{"hello": "world", "age": 15}))

	s.NoError(s.mtchr.SetQuery(M{"$and": A{M{"hello": "world"}, M{"age": M{"$lt": 20}}}}))
	s.Matches(s.mtchr.Match(M{"hello": "world", "age": 15}))

	s.NoError(s.mtchr.SetQuery(M{"$and": A{M{"hello": "pluton"}, M{"age": M{"$lt": 20}}}}))
	s.NotMatches(s.mtchr.Match(M{"hello": "world", "age": 15}))

	s.NoError(s.mtchr.SetQuery(M{"$and": A{}}))
	s.Matches(s.mtchr.Match(M{"hello": "world", "age": 15}))

	s.Error(s.mtchr.SetQuery(M{"$and": A{A{}}}))

	s.Error(s.mtchr.SetQuery(M{"$and": A{M{"$and": A{}, "nope": "nah"}}}))

}

// Subquery should not match for a $not to match.
func (s *MatcherTestSuite) TestNot() {

	s.NoError(s.mtchr.SetQuery(M{"a": 5}))
	s.Matches(s.mtchr.Match(M{"a": 5, "b": 10}))

	s.NoError(s.mtchr.SetQuery(M{"$not": M{"a": 5}}))
	s.NotMatches(s.mtchr.Match(M{"a": 5, "b": 10}))

	s.ErrorAs(s.mtchr.SetQuery(M{"$not": M{"a": M{"$in": 5}}}), &ErrCompArgType{})

	s.Error(s.mtchr.SetQuery(M{"$not": 1}))
}

func (s *MatcherTestSuite) TestNotSubError() {
	s.NoError(s.mtchr.SetQuery(M{"$not": M{"a": 1}}))
	cm := new(comparerMock)
	s.mtchr.comparer = cm

	errComp := fmt.Errorf("compare error")
	cm.On("Compare", 1, 1).Return(0, errComp).Once()

	matches, err := s.mtchr.Match(M{"a": 1})
	s.ErrorIs(err, errComp)
	s.False(matches)

	cm.AssertExpectations(s.T())

}

func (s *MatcherTestSuite) TestUnknownComp() {
	s.Error(s.mtchr.SetQuery(M{"$or": A{M{"$unknown": 1}}}))
}

func (s *MatcherTestSuite) TestUnreachable() {
	// these are not reachable lines, but they mess up test coverage
	lo, invalid, err := s.mtchr.dollarCond(nil, false, nil)
	s.NoError(err)
	s.False(invalid)
	s.Nil(lo)

	matches, err := s.mtchr.matchLogicOp(nil, LogicOp{Type: 255})
	s.NoError(err)
	s.False(matches)

	matches, err = s.mtchr.matchCond(nil, false, &Cond{Op: 255})
	s.NoError(err)
	s.False(matches)
}

// Logical operators are all top-level, only other logical operators can be
// above.
func (s *MatcherTestSuite) TestLogicalOperatorsTopLevel() {

	s.ErrorAs(s.mtchr.SetQuery(M{"a": M{"$or": A{M{"b": 5}, M{"b": 7}}}}), &ErrUnknownComparison{})

	s.NoError(s.mtchr.SetQuery(M{"$or": A{M{"a.b": 5}, M{"a.b": 7}}}))
	s.Matches(s.mtchr.Match(M{"a": M{"b": 7}}))
}

// Logical operators can be combined as long as they are on top of the decision
// tree.
func (s *MatcherTestSuite) TestMultipleLogicalOps() {

	s.NoError(s.mtchr.SetQuery(
		M{"$or": A{M{"$and": A{
			M{"a": 5},
			M{"b": 8},
		}},
			M{"$and": A{
				M{"a": 5},
				M{"c": M{"$lt": 40}},
			}},
		}},
	))
	s.Matches(s.mtchr.Match(M{"a": 5, "b": 7, "c": 12}))

	s.NoError(s.mtchr.SetQuery(
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
	s.NotMatches(s.mtchr.Match(M{"a": 5, "b": 7, "c": 12}))
}

// Should throw an error if a logical operator is used without an array or if an
// unknown logical operator is used.
func (s *MatcherTestSuite) TestLogicOpError() {
	s.ErrorAs(s.mtchr.SetQuery(M{"$or": M{"a": 5}}), &ErrCompArgType{})
	s.ErrorAs(s.mtchr.SetQuery(M{"$and": M{"a": 5}}), &ErrCompArgType{})
	s.ErrorAs(s.mtchr.SetQuery(M{"$unknown": A{M{"a": 5}}}), &ErrUnknownOperator{})
}

// Function should match and not match correctly.
func (s *MatcherTestSuite) TestWhere() {
	s.NoError(s.mtchr.SetQuery(M{"$where": func(v any) (bool, error) {
		return v.(domain.Document).Get("a") == 4, nil
	}}))
	s.Matches(s.mtchr.Match(M{"a": 4}))

	s.NoError(s.mtchr.SetQuery(M{"$where": func(v any) (bool, error) {
		return v.(domain.Document).Get("a") == 5, nil
	}}))
	s.NotMatches(s.mtchr.Match(M{"a": 4}))
}

// Should throw an error if the $where function is not, in fact, a function.
func (s *MatcherTestSuite) TestWhereNotAFunction() {
	s.ErrorAs(s.mtchr.SetQuery(M{"$where": "not a function"}), &ErrCompArgType{})
}

// Should throw an error if the $where function returns a non-boolean.
func (s *MatcherTestSuite) TestWhereNonBoolean() {
	s.ErrorAs(s.mtchr.SetQuery(M{"$where": func(domain.Document) string {
		return "not a boolean"
	}}), &ErrCompArgType{})
}

// Should be able to do the complex matching it must be used for.
func (s *MatcherTestSuite) TestWhereComplexMatching() {
	checkEmail := func(v any) (bool, error) {
		doc := v.(domain.Document)
		if !doc.Has("firstName") || !doc.Has("lastName") {
			return false, nil
		}
		fn, ok := doc.Get("firstName").(string)
		if !ok {
			return false, nil
		}
		ln, ok := doc.Get("lastName").(string)
		if !ok {
			return false, nil
		}
		email, ok := doc.Get("email").(string)
		if !ok {
			return false, nil
		}
		return strings.ToLower(fn)+"."+strings.ToLower(ln)+"@mail.com" == email, nil
	}

	s.NoError(s.mtchr.SetQuery(M{"$where": checkEmail}))
	s.Matches(s.mtchr.Match(
		M{
			"firstName": "John",
			"lastName":  "Doe",
			"email":     "john.doe@mail.com",
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"$where": checkEmail}))
	s.Matches(s.mtchr.Match(
		M{
			"firstName": "john",
			"lastName":  "doe",
			"email":     "john.doe@mail.com",
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"$where": checkEmail}))
	s.NotMatches(s.mtchr.Match(
		M{
			"firstName": "Jane",
			"lastName":  "Doe",
			"email":     "john.doe@mail.com",
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"$where": checkEmail}))
	s.NotMatches(s.mtchr.Match(
		M{
			"firstName": "John",
			"lastName":  "Deere",
			"email":     "john.doe@mail.com",
		},
	))

	s.NoError(s.mtchr.SetQuery(M{"$where": checkEmail}))
	s.NotMatches(s.mtchr.Match(
		M{"lastName": "Deere", "email": "john.doe@mail.com"},
	))
}

// Array field equality.
func (s *MatcherTestSuite) TestArrayFieldEquality() {

	s.NoError(s.mtchr.SetQuery(M{"tags": "python"}))
	s.NotMatches(s.mtchr.Match(
		M{"tags": A{"go", "embedded", "db"}},
	))

	s.NoError(s.mtchr.SetQuery(M{"tagss": "go"}))
	s.NotMatches(s.mtchr.Match(
		M{"tags": A{"go", "embedded", "db"}},
	))

	s.NoError(s.mtchr.SetQuery(M{"tags": "go"}))
	s.Matches(s.mtchr.Match(M{"tags": A{"go", "embedded", "db"}}))

	// NOTE: duplicate key test here. not adding

	s.NoError(s.mtchr.SetQuery(M{"tags": "go", "gedb": true}))
	s.Matches(s.mtchr.Match(
		M{"tags": A{"go", "embedded", "db"}, "gedb": true},
	))

	s.NoError(s.mtchr.SetQuery(M{"data.tags": "go"}))
	s.Matches(s.mtchr.Match(
		M{"number": 5, "data": M{"tags": A{"go", "embedded", "db"}}},
	))

	s.NoError(s.mtchr.SetQuery(M{"data.tags": "g"}))
	s.NotMatches(s.mtchr.Match(
		M{"number": 5, "data": M{"tags": A{"go", "embedded", "db"}}},
	))
}

// Array fields with one comparison operator.
func (s *MatcherTestSuite) TestArrayOneComparisonOp() {
	s.NoError(s.mtchr.SetQuery(M{"ages": M{"$lt": 2}}))
	s.NotMatches(s.mtchr.Match(M{"ages": A{3, 7, 12}}))

	s.NoError(s.mtchr.SetQuery(M{"ages": M{"$lt": 3}}))
	s.NotMatches(s.mtchr.Match(M{"ages": A{3, 7, 12}}))

	s.NoError(s.mtchr.SetQuery(M{"ages": M{"$lt": 4}}))
	s.Matches(s.mtchr.Match(M{"ages": A{3, 7, 12}}))

	s.NoError(s.mtchr.SetQuery(M{"ages": M{"$lt": 8}}))
	s.Matches(s.mtchr.Match(M{"ages": A{3, 7, 12}}))

	s.NoError(s.mtchr.SetQuery(M{"ages": M{"$lt": 13}}))
	s.Matches(s.mtchr.Match(M{"ages": A{3, 7, 12}}))
}

// Array fields work with arrays that are in subdocuments.
func (s *MatcherTestSuite) TestArraySubDoc() {

	s.NoError(s.mtchr.SetQuery(M{"children.ages": M{"$lt": 2}}))
	s.NotMatches(s.mtchr.Match(M{"children": M{"ages": A{3, 7, 12}}}))

	s.NoError(s.mtchr.SetQuery(M{"children.ages": M{"$lt": 3}}))
	s.NotMatches(s.mtchr.Match(M{"children": M{"ages": A{3, 7, 12}}}))

	s.NoError(s.mtchr.SetQuery(M{"children.ages": M{"$lt": 4}}))
	s.Matches(s.mtchr.Match(M{"children": M{"ages": A{3, 7, 12}}}))

	s.NoError(s.mtchr.SetQuery(M{"children.ages": M{"$lt": 8}}))
	s.Matches(s.mtchr.Match(M{"children": M{"ages": A{3, 7, 12}}}))

	s.NoError(s.mtchr.SetQuery(M{"children.ages": M{"$lt": 13}}))
	s.Matches(s.mtchr.Match(M{"children": M{"ages": A{3, 7, 12}}}))
}

// Can query inside arrays thanks to dot notation.
func (s *MatcherTestSuite) TestQueryInsideArray() {

	s.NoError(s.mtchr.SetQuery(M{"children.age": M{"$lt": 2}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.age": M{"$lt": 3}}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.age": M{"$lt": 4}}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.age": M{"$lt": 5}}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.age": M{"$lt": 13}}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.name": "Lois"}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.name": "Louie"}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.name": "Lewi"}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))
}

// Can query for a specific element inside arrays thanks to dot notation.
func (s *MatcherTestSuite) TestMatchArrayOnIndex() {
	doc := M{"children": A{
		M{"name": "Huey", "age": 3},
		M{"name": "Dewey", "age": 7},
		M{"name": "Louie", "age": 12},
	}}
	s.NoError(s.mtchr.SetQuery(M{"children.0.name": "Louie"}))
	s.NotMatches(s.mtchr.Match(doc))

	s.NoError(s.mtchr.SetQuery(M{"children.1.name": "Louie"}))
	s.NotMatches(s.mtchr.Match(doc))

	s.NoError(s.mtchr.SetQuery(M{"children.2.name": "Louie"}))
	s.Matches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))

	s.NoError(s.mtchr.SetQuery(M{"children.3.name": "Louie"}))
	s.NotMatches(s.mtchr.Match(
		M{"children": A{
			M{"name": "Huey", "age": 3},
			M{"name": "Dewey", "age": 7},
			M{"name": "Louie", "age": 12},
		}},
	))
}

func (s *MatcherTestSuite) TestMatchTime() {
	now := time.Now()
	s.NoError(s.mtchr.SetQuery(M{"now": now}))
	s.Matches(s.mtchr.Match(M{"now": now}))
	s.NotMatches(s.mtchr.Match(M{"now": now.Add(time.Second)}))
}

func (s *MatcherTestSuite) TestMatchEmptyObj() {
	s.NoError(s.mtchr.SetQuery(M{"now": M{}}))
	s.Matches(s.mtchr.Match(M{"now": M{}}))
}

func (s *MatcherTestSuite) TestEqualObjFailedDocumentFactory() {
	errDocFac := fmt.Errorf("document factory error")
	s.mtchr.documentFactory = func(any) (domain.Document, error) {
		return nil, errDocFac
	}

	s.ErrorIs(s.mtchr.SetQuery(M{"a": M{"b": "c"}}), errDocFac)

}

// A single array-specific operator and the query is treated as array specific.
func (s *MatcherTestSuite) TestArraySpecificQuery() {
	s.ErrorIs(s.mtchr.SetQuery(M{"children": M{"Dewey": true, "$size": 3}}), ErrMixedOperators)
}

// Can mix queries on array fields and non array filds with array specific
// operators.
func (s *MatcherTestSuite) TestMixArrayAndNonArrayOps() {
	doc := M{
		"uncle":   "Donald",
		"nephews": A{"Huguinho", "Zezinho", "Luisinho"},
	}

	s.NoError(s.mtchr.SetQuery(M{"nephews": M{"$size": 2}, "uncle": "Donald"}))
	s.NotMatches(s.mtchr.Match(doc))
	s.NoError(s.mtchr.SetQuery(M{"nephews": M{"$size": 3}, "uncle": "Donald"}))
	s.Matches(s.mtchr.Match(doc))
	s.NoError(s.mtchr.SetQuery(M{"nephews": M{"$size": 4}, "uncle": "Donald"}))
	s.NotMatches(s.mtchr.Match(doc))

	s.NoError(s.mtchr.SetQuery(M{"nephews": M{"$size": 3}, "uncle": "Patinhas"}))
	s.NotMatches(s.mtchr.Match(doc))
	s.NoError(s.mtchr.SetQuery(M{"nephews": M{"$size": 3}, "uncle": "Donald"}))
	s.Matches(s.mtchr.Match(doc))
	s.NoError(s.mtchr.SetQuery(M{"nephews": M{"$size": 3}, "uncle": "Margarida"}))
	s.NotMatches(s.mtchr.Match(doc))
}

// can match non-doc queries.
func (s *MatcherTestSuite) TestNonDocMatch() {
	// $regex

	s.NoError(s.mtchr.SetQuery(M{"$regex": regexp.MustCompile(`^a$`)}))
	s.Matches(s.mtchr.Match("a"))

	s.NoError(s.mtchr.SetQuery(M{"$regex": regexp.MustCompile(`^b$`)}))
	s.NotMatches(s.mtchr.Match("a"))

	// $nin
	s.NoError(s.mtchr.SetQuery(M{"$nin": A{11, 13, 15}}))
	s.Matches(s.mtchr.Match(12))

	s.NoError(s.mtchr.SetQuery(M{"$nin": A{11, 12, 13}}))
	s.NotMatches(s.mtchr.Match(12))

	// $lt
	s.NoError(s.mtchr.SetQuery(M{"$lt": 13}))
	s.Matches(s.mtchr.Match(12))

	s.NoError(s.mtchr.SetQuery(M{"$lt": 12}))
	s.NotMatches(s.mtchr.Match(12))

	// $gte
	s.NoError(s.mtchr.SetQuery(M{"$gte": 12}))
	s.Matches(s.mtchr.Match(12))

	s.NoError(s.mtchr.SetQuery(M{"$gte": 13}))
	s.NotMatches(s.mtchr.Match(12))

	// $lte
	s.NoError(s.mtchr.SetQuery(M{"$lte": 12}))
	s.Matches(s.mtchr.Match(12))

	s.NoError(s.mtchr.SetQuery(M{"$lte": 11}))
	s.NotMatches(s.mtchr.Match(12))

	// $gt
	s.NoError(s.mtchr.SetQuery(M{"$gt": 11}))
	s.Matches(s.mtchr.Match(12))

	s.NoError(s.mtchr.SetQuery(M{"$gt": 12}))
	s.NotMatches(s.mtchr.Match(12))

	// $ne
	s.NoError(s.mtchr.SetQuery(M{"$ne": 11}))
	s.Matches(s.mtchr.Match(12))

	s.NoError(s.mtchr.SetQuery(M{"$ne": 12}))
	s.NotMatches(s.mtchr.Match(12))

	// $in
	s.NoError(s.mtchr.SetQuery(M{"$in": A{11, 12, 13}}))
	s.Matches(s.mtchr.Match(12))

	s.NoError(s.mtchr.SetQuery(M{"$in": A{11, 13, 15}}))
	s.NotMatches(s.mtchr.Match(12))

	// $exists
	s.NoError(s.mtchr.SetQuery(M{"$exists": true}))
	s.Matches(s.mtchr.Match(12))

	s.NoError(s.mtchr.SetQuery(M{"$exists": false}))
	s.NotMatches(s.mtchr.Match(12))

	// $size
	s.NoError(s.mtchr.SetQuery(M{"$size": 2}))
	s.Matches(s.mtchr.Match(A{1, 2}))

	s.NoError(s.mtchr.SetQuery(M{"$size": 2}))
	s.NotMatches(s.mtchr.Match(A{1, 2, 3}))

	s.ErrorAs(s.mtchr.SetQuery(M{"$size": false}), &ErrCompArgType{})

	// $elemMatch

	s.NoError(s.mtchr.SetQuery(M{"$elemMatch": 2}))
	s.Matches(s.mtchr.Match(A{1, 2}))

	s.NoError(s.mtchr.SetQuery(M{"$elemMatch": 4}))
	s.NotMatches(s.mtchr.Match(A{1, 2, 3}))
}

// Will not match query if second argument is not a doc.
func (s *MatcherTestSuite) TestNonDocQuery() {
	s.NoError(s.mtchr.SetQuery("a"))
	s.NotMatches(s.mtchr.Match(M{"a": "value"}))
	s.NoError(s.mtchr.SetQuery("value"))
	s.NotMatches(s.mtchr.Match(M{"a": "value"}))
	s.NoError(s.mtchr.SetQuery("test"))
	s.NotMatches(s.mtchr.Match(M{"you": M{"expected": "a"}}))
	s.NoError(s.mtchr.SetQuery("dio"))
	s.NotMatches(s.mtchr.Match(M{"but": A{"it", "was", "me"}}))
}

func (s *MatcherTestSuite) TestNonDocFailNewDoc() {
	errDocFac := fmt.Errorf("document factory error")
	df := func(any) (domain.Document, error) {
		return nil, errDocFac
	}
	s.mtchr = NewMatcher(WithDocumentFactory(df)).(*Matcher)

	// when creating a query for a non-document and when creating a query
	// from a non-object value, [Matcher] behaves as if the given item was
	// the value of the "needAKey" document key. To do so, an empty document
	// is used, adding the key "needAKey" with the given argument as value.
	// That document is created once, when needed, and reused whenever a new
	// document-independent query is created.
	s.NoError(s.mtchr.SetQuery("a"))
	m, err := s.mtchr.Match("a")
	s.ErrorIs(err, errDocFac)
	s.False(m)

	s.NoError(s.mtchr.SetQuery("a"))
	m, err = s.mtchr.Match("a")
	s.ErrorIs(err, errDocFac)
	s.False(m)

	s.mtchr.documentFactory = data.NewDocument

	s.NoError(s.mtchr.SetQuery("a"))
	m, err = s.mtchr.Match("a")
	s.NoError(err)
	s.True(m)
}

// cannot mix normal fields and operators in queries.
func (s *MatcherTestSuite) TestMixOperators() {
	// without operator works

	s.NoError(s.mtchr.SetQuery(M{"a": 1}))
	s.Matches(s.mtchr.Match(
		M{"a": 1},
	))

	// without operator too

	s.NoError(s.mtchr.SetQuery(M{"$and": A{M{"a": 1}, M{"a": M{"$gt": 0}}}}))
	s.Matches(s.mtchr.Match(
		M{"a": 1},
	))

	// cannot combine them

	s.ErrorIs(s.mtchr.SetQuery(M{"a": 1, "$and": A{M{"a": 1}, M{"a": M{"$gt": 0}}}}), ErrMixedOperators)
}

func (s *MatcherTestSuite) TestNilQuery() {
	s.NoError(s.mtchr.SetQuery(nil))
	s.Matches(s.mtchr.Match("anything"))
	s.Matches(s.mtchr.Match([]string{"is"}))
	s.Matches(s.mtchr.Match(func() string { return "valid" }))
}

func (s *MatcherTestSuite) TestGetter() {
	s.NoError(s.mtchr.SetQuery(M{"a": GetterImpl{"value"}}))
}

func (s *MatcherTestSuite) TestErrorMessages() {
	tests := []struct {
		err  error
		want string
	}{
		{
			err:  ErrUnknownOperator{Operator: "$new"},
			want: "unknown operator \"$new\"",
		},
		{
			err:  ErrUnknownComparison{Comparison: "a"},
			want: "unknown comparison \"a\"",
		},
		{
			err: ErrCompArgType{
				Comp:   "$a",
				Want:   "b",
				Actual: time.Now(),
			},
			want: "$a value should be of type b, got time.Time",
		},
	}

	for _, tt := range tests {
		s.Equal(tt.want, tt.err.Error())
	}
}

func (s *MatcherTestSuite) Matches(matches bool, err error) {
	s.NoError(err)
	s.True(matches)
}

func (s *MatcherTestSuite) NotMatches(matches bool, err error) {
	s.NoError(err)
	s.False(matches)
}

func (s *MatcherTestSuite) SetupTest() {
	s.mtchr = NewMatcher().(*Matcher)
}

func (s *MatcherTestSuite) SetupSubTest() {
	s.SetupTest()
}

func TestMatcherTestSuite(t *testing.T) {
	suite.Run(t, new(MatcherTestSuite))
}
