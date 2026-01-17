// Package querier contains the default [domain.Querier] implementation.
package querier

import (
	"fmt"
	"iter"
	"slices"

	"github.com/vinicius-lino-figueiredo/gedb/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/matcher"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/projector"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Querier implements [domain.Querier].
type Querier struct {
	mtchr  domain.Matcher
	cmpr   domain.Comparer
	fn     domain.FieldNavigator
	proj   domain.Projector
	docFac domain.DocumentFactory
}

// NewQuerier returns a new implementation of [domain.Querier].
func NewQuerier(opts ...Option) domain.Querier {
	q := Querier{
		docFac: data.NewDocument,
		cmpr:   comparer.NewComparer(),
	}
	for _, opt := range opts {
		opt(&q)
	}
	if q.fn == nil {
		q.fn = fieldnavigator.NewFieldNavigator(q.docFac)
	}
	if q.proj == nil {
		q.proj = projector.NewProjector(
			projector.WithDocumentFactory(q.docFac),
			projector.WithFieldNavigator(q.fn),
		)
	}
	if q.mtchr == nil {
		q.mtchr = matcher.NewMatcher(
			matcher.WithComparer(q.cmpr),
			matcher.WithDocumentFactory(q.docFac),
			matcher.WithFieldNavigator(q.fn),
		)
	}
	return &q
}

// Query implements [domain.Querier].
func (q *Querier) Query(data iter.Seq2[domain.Document, error], opts ...domain.QueryOption) ([]domain.Document, error) {
	if data == nil {
		return make([]domain.Document, 0), nil
	}

	options := domain.QueryOptions{Cap: 256}
	for _, opt := range opts {
		opt(&options)
	}

	res, finished, err := q.filter(data, options)
	if err != nil {
		return nil, err
	}

	if finished {
		return res, nil
	}

	if options.Sort != nil {
		sorted, err := q.sort(res, options.Sort)
		if err != nil {
			return nil, fmt.Errorf("sorting: %w", err)
		}
		res = q.skipAndLimit(sorted, options.Skip, options.Limit)
	}

	res, err = q.proj.Project(res, options.Projection)
	if err != nil {
		return nil, fmt.Errorf("projecting: %w", err)
	}
	return res, nil
}

func (q *Querier) filter(data iter.Seq2[domain.Document, error], opts domain.QueryOptions) ([]domain.Document, bool, error) {
	var skipped int64
	res := make([]domain.Document, 0, opts.Cap)

	if opts.Query != nil {
		if err := q.mtchr.SetQuery(opts.Query); err != nil {
			return nil, false, err
		}
	}

	for doc, err := range data {
		if err != nil {
			return nil, false, err
		}
		if opts.Query != nil {
			matches, err := q.mtchr.Match(doc)
			if err != nil {
				return nil, false, fmt.Errorf("matching document: %w", err)
			}
			if !matches {
				continue
			}
		}
		if opts.Sort == nil {
			if skipped < opts.Skip {
				skipped++
				continue
			}
			if opts.Limit > 0 && int64(len(res)) == opts.Limit {
				res, err := q.proj.Project(res, opts.Projection)
				if err != nil {
					return nil, false, fmt.Errorf("projecting: %w", err)
				}
				return res, true, nil
			}
		}
		res = append(res, doc)
	}
	return res, false, nil
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
		return 0, fmt.Errorf("getting address: %w", err)
	}

	criterionA, _, err := q.fn.GetField(a, addr...)
	if err != nil {
		return 0, fmt.Errorf("getting field: %w", err)
	}
	criterionB, _, err := q.fn.GetField(b, addr...)
	if err != nil {
		return 0, fmt.Errorf("getting field: %w", err)
	}

	critA := q.listFields(criterionA)
	critB := q.listFields(criterionB)

	comp, err := q.cmpr.Compare(critA, critB)
	if err != nil {
		return 0, fmt.Errorf("comparing: %w", err)
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
