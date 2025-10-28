package querier

import (
	"slices"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/matcher"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/projector"
)

// Querier implements [domain.Querier].
type Querier struct {
	mtchr domain.Matcher
	cmpr  domain.Comparer
	fn    domain.FieldNavigator
	proj  domain.Projector
}

// NewQuerier returns a new implementation of [domain.Querier].
func NewQuerier(opts ...domain.QuerierOption) domain.Querier {
	options := domain.QuerierOptions{
		DocFac:   data.NewDocument,
		Comparer: comparer.NewComparer(),
	}
	for _, opt := range opts {
		opt(&options)
	}
	if options.FieldNavigator == nil {
		options.FieldNavigator = fieldnavigator.NewFieldNavigator(
			options.DocFac,
		)
	}
	if options.Projector == nil {
		options.Projector = projector.NewProjector(
			domain.WithProjectorDocumentFactory(options.DocFac),
			domain.WithProjectorFieldNavigator(options.FieldNavigator),
		)
	}
	if options.Matcher == nil {
		options.Matcher = matcher.NewMatcher(
			domain.WithMatcherComparer(options.Comparer),
			domain.WithMatcherDocumentFactory(options.DocFac),
			domain.WithMatcherFieldNavigator(options.FieldNavigator),
		)
	}
	return &Querier{
		mtchr: options.Matcher,
		cmpr:  options.Comparer,
		fn:    options.FieldNavigator,
		proj:  options.Projector,
	}
}

// Query implements [domain.Querier].
func (q *Querier) Query(data []domain.Document, opts ...domain.QueryOption) ([]domain.Document, error) {
	var options domain.QueryOptions
	for _, opt := range opts {
		opt(&options)
	}

	var skipped int64
	res := make([]domain.Document, 0, len(data))
	for _, doc := range data {
		if options.Query != nil {
			matches, err := q.mtchr.Match(doc, options.Query)
			if err != nil {
				return nil, err
			}
			if !matches {
				continue
			}
		}
		if options.Sort == nil {
			if skipped < options.Skip {
				skipped++
				continue
			}
			if options.Limit > 0 && int64(len(res)) == options.Limit {
				return q.proj.Project(res, options.Projection)
			}
		}
		res = append(res, doc)
	}

	if options.Sort != nil {
		sorted, err := q.sort(res, options.Sort)
		if err != nil {
			return nil, err
		}
		res = q.skipAndLimit(sorted, options.Skip, options.Limit)
	}

	return q.proj.Project(res, options.Projection)
}

func (q *Querier) sort(data []domain.Document, sort domain.Sort) ([]domain.Document, error) {
	res := slices.Clone(data)
	var err error
	slices.SortFunc(res, func(a, b domain.Document) int {
		if err != nil {
			return 0
		}
		for _, crit := range sort {
			comp, cErr := q.compareByCriterion(a, b, crit)
			if cErr != nil {
				err = cErr
				return 0
			}
			if comp != 0 {
				return comp
			}
		}
		return 0
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (q *Querier) compareByCriterion(a, b domain.Document, crit domain.SortName) (int, error) {
	addr, err := q.fn.GetAddress(crit.Key)
	if err != nil {
		return 0, err
	}

	criterionA, _, err := q.fn.GetField(a, addr...)
	if err != nil {
		return 0, err
	}
	criterionB, _, err := q.fn.GetField(b, addr...)
	if err != nil {
		return 0, err
	}

	critA := q.listFields(criterionA)
	critB := q.listFields(criterionB)

	comp, err := q.cmpr.Compare(critA, critB)
	if err != nil {
		return 0, err
	}
	return comp * int(crit.Order), nil
}

func (q *Querier) listFields(g []domain.GetSetter) []any {
	res := make([]any, len(g))
	for n, v := range g {
		res[n] = v
	}
	return res
}

func (q *Querier) skipAndLimit(data []domain.Document, skip, limit int64) []domain.Document {

	length := int64(len(data))

	skip = max(skip, 0)      // skip cannot be negative
	skip = min(skip, length) // cannot skip more than length

	limit = min(skip+limit, length) // limit cannot be greather than length
	if limit == skip {              // if limit is zero, return all data
		limit = length
	}

	return data[skip:limit]
}
