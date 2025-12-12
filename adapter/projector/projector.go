// Package projector contains the default [domain.Projector] implementation.
package projector

import (
	"errors"

	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var (
	// ErrMixOmitType is returned when user provides a projection object
	// with mixed "omit" and "show" operators.
	ErrMixOmitType = errors.New("can't both keep and omit fields except for _id")
)

// Projector implements [domain.Projector].
type Projector struct {
	fn     domain.FieldNavigator
	docFac domain.DocumentFactory
}

// NewProjector returns a new implementation of [domain.Projector].
func NewProjector(opts ...Option) domain.Projector {
	p := Projector{docFac: data.NewDocument}
	for _, opt := range opts {
		opt(&p)
	}
	if p.fn == nil {
		p.fn = fieldnavigator.NewFieldNavigator(p.docFac)
	}
	return &p
}

// Project implements [domain.Projector].
func (q *Projector) Project(docs []domain.Document, proj map[string]uint8) ([]domain.Document, error) {
	if len(proj) == 0 {
		return docs, nil
	}

	id, idMentioned := proj["_id"]
	keepID := !idMentioned || id != 0
	_projection := make([][]string, 0, len(proj))

	fields := 0
	oneFields := 0
	for field, value := range proj {
		if field == "_id" {
			continue
		}
		fields++
		if value > 0 {
			oneFields++
		}
		if oneFields > 0 && oneFields != fields {
			return nil, ErrMixOmitType
		}
		addr, err := q.fn.GetAddress(field)
		if err != nil {
			return nil, err
		}
		_projection = append(_projection, addr)
	}

	if !idMentioned && oneFields > 1 {
		_projection = append(_projection, []string{"_id"})
	}

	res := make([]domain.Document, len(docs))
	for n, doc := range docs {
		projected, err := q.projectDoc(doc, _projection, oneFields != 0)
		if err != nil {
			return nil, err
		}

		if keepID {
			projected.Set("_id", doc.ID())
		} else {
			projected.Unset("_id")
		}
		res[n] = projected
	}

	return res, nil
}

func (q *Projector) projectDoc(doc domain.Document, p [][]string, add bool) (domain.Document, error) {
	if add {
		return q.positiveProject(doc, p)
	}
	return q.negativeProject(doc, p)
}

func (q *Projector) positiveProject(doc domain.Document, p [][]string) (domain.Document, error) {
	res, err := q.docFac(nil)
	if err != nil {
		return nil, err
	}

	for _, field := range p {
		values, expanded, err := q.fn.GetField(doc, field...)
		if err != nil {
			return nil, err
		}
		fieldValues, ok := q.readFields(values, expanded)
		if !ok {
			continue
		}
		created, err := q.fn.EnsureField(res, field...)
		if err != nil {
			return nil, err
		}
		for _, c := range created {
			c.Set(fieldValues)
		}
	}
	return res, nil
}

func (q *Projector) readFields(f []domain.GetSetter, expanded bool) (any, bool) {
	if !expanded {
		return f[0].Get()
	}
	res := make([]any, len(f))
	for n, field := range f {
		value, _ := field.Get()
		res[n] = value
	}
	return res, true
}

func (q *Projector) negativeProject(doc domain.Document, p [][]string) (domain.Document, error) {
	res, err := q.docFac(doc)
	if err != nil {
		return nil, err
	}
	for _, field := range p {
		values, _, err := q.fn.GetField(res, field...)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			value.Unset()
		}
	}
	return res, nil
}
