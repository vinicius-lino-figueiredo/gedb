package domain_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/hasher"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

type DomainTestSuite struct {
	suite.Suite
}

func (s *DomainTestSuite) TestOptions() {
	var fos domain.FindOptions
	fo := []domain.FindOption{
		domain.WithProjection(1),
		domain.WithSkip(-2),
		domain.WithLimit(-3),
		domain.WithSort(domain.Sort{{Key: "a", Order: -4}}),
	}
	for _, opt := range fo {
		opt(&fos)
	}
	s.Equal(domain.FindOptions{
		Projection: 1,
		Skip:       -2,
		Limit:      -3,
		Sort:       domain.Sort{{Key: "a", Order: -4}},
	}, fos)

	var uos domain.UpdateOptions
	uo := []domain.UpdateOption{
		domain.WithUpdateMulti(true),
		domain.WithUpsert(true),
	}
	for _, opt := range uo {
		opt(&uos)
	}
	s.Equal(domain.UpdateOptions{Multi: true, Upsert: true}, uos)

	var ros domain.RemoveOptions
	domain.WithRemoveMulti(true)(&ros)
	s.Equal(domain.RemoveOptions{Multi: true}, ros)

	var eios domain.EnsureIndexOptions
	eio := []domain.EnsureIndexOption{
		domain.WithFields("a", "b", "c"),
		domain.WithUnique(true),
		domain.WithSparse(true),
		domain.WithTTL(12),
	}
	for _, opt := range eio {
		opt(&eios)
	}
	s.Equal(domain.EnsureIndexOptions{
		FieldNames:  []string{"a", "b", "c"},
		Unique:      true,
		Sparse:      true,
		ExpireAfter: 12,
	}, eios)

	var qos domain.QueryOptions
	qo := []domain.QueryOption{
		domain.WithQuery(1),
		domain.WithQueryLimit(2),
		domain.WithQuerySkip(3),
		domain.WithQuerySort(domain.Sort{{Key: "a", Order: 5}}),
		domain.WithQueryProjection(map[string]uint8{"b": 6}),
		domain.WithQueryCap(7),
	}
	for _, opt := range qo {
		opt(&qos)
	}
	s.Equal(domain.QueryOptions{
		Query:      1,
		Limit:      2,
		Skip:       3,
		Sort:       domain.Sort{{Key: "a", Order: 5}},
		Projection: map[string]uint8{"b": 6},
		Cap:        7,
	}, qos)

	dec := decoder.NewDecoder()
	var cos domain.CursorOptions
	domain.WithCursorDecoder(dec)(&cos)
	s.Equal(domain.CursorOptions{Decoder: dec}, cos)

	comp := comparer.NewComparer()
	ha := hasher.NewHasher()
	fn := fieldnavigator.NewFieldNavigator(data.NewDocument)
	var ios domain.IndexOptions
	io := []domain.IndexOption{
		domain.WithIndexFieldName("a"),
		domain.WithIndexUnique(true),
		domain.WithIndexSparse(true),
		domain.WithIndexExpireAfter(1),
		domain.WithIndexDocumentFactory(nil),
		domain.WithIndexComparer(comp),
		domain.WithIndexHasher(ha),
		domain.WithIndexFieldNavigator(fn),
	}
	for _, opt := range io {
		opt(&ios)
	}
	s.Equal(domain.IndexOptions{
		FieldName:       "a",
		Unique:          true,
		Sparse:          true,
		ExpireAfter:     1,
		DocumentFactory: nil,
		Comparer:        comp,
		Hasher:          ha,
		FieldNavigator:  fn,
	}, ios)
}

func (s *DomainTestSuite) TestErrorMessages() {
	var e error

	e = domain.ErrFieldName{Field: "a", Reason: "I don't like it"}
	s.Equal(`invalid field name "a": I don't like it`, e.Error())

	e = domain.ErrDatafileName{Name: "b", Reason: "nope!"}
	s.Equal(`invalid datafile name "b": nope!`, e.Error())

	e = domain.ErrDocumentType{Reason: "nah"}
	s.Equal("invalid doc instantiation: nah", e.Error())

	e = domain.ErrCannotCompare{A: "a", B: 2}
	s.Equal(`cannot compare a and 2`, e.Error())

	e = domain.ErrCorruptFiles{
		CorruptionRate:        1,
		CorruptItems:          10,
		DataLength:            10,
		CorruptAlertThreshold: 0.5,
	}
	s.Equal("corrupted 100.00% (10 of 10) exceeded threshold 50.00%", e.Error())

	e = domain.ErrDecode{Source: 123, Target: "a"}
	s.Equal("cannot decode %!s(int=+123) into string", e.Error())
}

func TestDomainTestSuite(t *testing.T) {
	suite.Run(t, new(DomainTestSuite))
}
