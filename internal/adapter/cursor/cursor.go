package cursor

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/matcher"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
)

// Cursor implements domain.Cursor.
type Cursor struct {
	data           []domain.Document
	ctx            context.Context
	mu             *ctxsync.Mutex
	dec            domain.Decoder
	started        bool
	storedErr      error
	fieldNavigator domain.FieldNavigator
}

// NewCursor returns a new implementation of Cursor.
func NewCursor(ctx context.Context, dt []domain.Document, options ...domain.CursorOption) (domain.Cursor, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	docFac := data.NewDocument
	fn := fieldnavigator.NewFieldNavigator(docFac)
	comp := comparer.NewComparer()
	m := matcher.NewMatcher(
		domain.WithMatcherDocumentFactory(docFac),
		domain.WithMatcherComparer(comp),
		domain.WithMatcherFieldNavigator(fn),
	)
	opts := domain.CursorOptions{
		FieldNavigator:  fn,
		Matcher:         m,
		Decoder:         decoder.NewDecoder(),
		DocumentFactory: docFac,
		Comparer:        comp,
	}

	for _, option := range options {
		option(&opts)
	}

	if len(dt) == 0 || int64(len(dt)) < opts.Skip {
		return &Cursor{ctx: ctx, mu: ctxsync.NewMutex()}, nil
	}

	res := make([]domain.Document, 0, len(dt))

	var doesMatch bool
	var err error
	var added int64
	var skipped int64
	for _, doc := range dt {
		doesMatch, err = opts.Matcher.Match(doc, opts.Query)
		if err != nil {
			return nil, err
		}
		if !doesMatch {
			continue
		}
		if opts.Sort != nil {
			res = append(res, doc)
			continue
		}
		if opts.Skip > skipped {
			skipped++
			continue
		}

		res = append(res, doc)
		added++

		if opts.Limit > 0 && opts.Limit <= added {
			break
		}
	}
	cur := &Cursor{
		ctx:            ctx,
		mu:             ctxsync.NewMutex(),
		dec:            opts.Decoder,
		fieldNavigator: opts.FieldNavigator,
	}

	if len(opts.Sort) != 0 && len(res) != 0 {
		res, err = cur.sort(res, opts)
		if err != nil {
			return nil, err
		}
	}

	// just making sure the iteration won't receive a negative number
	opts.Skip = max(0, opts.Skip)
	res = res[opts.Skip:]

	if opts.Limit <= 0 {
		opts.Limit = int64(len(res))
	}
	opts.Limit = min(int64(len(res)), opts.Limit)

	res = res[:opts.Limit]

	if len(opts.Projection) != 0 && len(res) != 0 {
		res, err = cur.project(res, opts)
		if err != nil {
			return nil, err
		}
	}

	values := slices.Clone(res)

	cur.data = values

	return cur, nil
}

func (c *Cursor) addField(doc map[string]any, candidate domain.Document, proj string) error {
	projFields, err := c.fieldNavigator.GetAddress(proj)
	if err != nil {
		return err
	}
	fieldValues, expanded, err := c.fieldNavigator.GetField(candidate, projFields...)
	if err != nil {
		return err
	}

	if !expanded {
		if _, isSet := fieldValues[0].Get(); !isSet {
			return nil
		}
	}

	values := make([]any, len(fieldValues))
	for n, fieldValue := range fieldValues {
		value, isSet := fieldValue.Get()
		if !expanded && !isSet {
			return nil
		}
		values[n] = value
	}

	curr := doc
	for i, field := range projFields {
		if i == len(projFields)-1 {
			curr[field] = values
			break
		}
		inner, ok := curr[field]
		if !ok {
			inner = make(map[string]any)
			curr[field] = inner
		}
		innerDoc, ok := inner.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected type %T in doc. expected %T", inner, innerDoc)
		}
		curr = innerDoc
	}
	return nil
}

func (c *Cursor) asMap(doc domain.Document) map[string]any {
	res := make(map[string]any, doc.Len())
	for k, v := range doc.Iter() {
		if k == "_id" {
			continue
		}
		if subDoc, ok := v.(domain.Document); ok {
			res[k] = c.asMap(subDoc)
		} else {
			res[k] = v
		}
	}
	return res
}

func (c *Cursor) compareByCriterion(a, b domain.Document, comparer domain.Comparer, criterion string, direction int) (int, error) {

	addr, err := c.fieldNavigator.GetAddress(criterion)
	if err != nil {
		return 0, err
	}

	criterionA, _, err := c.fieldNavigator.GetField(a, addr...)
	if err != nil {
		return 0, err
	}
	criterionB, _, err := c.fieldNavigator.GetField(b, addr...)
	if err != nil {
		return 0, err
	}

	critA := c.listFields(criterionA)
	critB := c.listFields(criterionB)

	comp, err := comparer.Compare(critA, critB)
	if err != nil {
		return 0, err
	}
	return comp * direction, nil
}

// Err implements domain.Cursor.
func (c *Cursor) Err() error {
	return c.storedErr
}

// Exec implements domain.Cursor.
func (c *Cursor) Exec(ctx context.Context, target any) error {
	innerCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		select {
		case <-ctx.Done():
			cancel(context.Cause(ctx))
		case <-c.ctx.Done():
			cancel(context.Cause(innerCtx))
		case <-innerCtx.Done():
		}
	}()
	if err := c.mu.LockWithContext(innerCtx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.storedErr != nil {
		return c.storedErr
	}
	if !c.started {
		return fmt.Errorf("called Exec before calling Next")
	}
	if len(c.data) == 0 {
		return fmt.Errorf("called Exec on empty Cursor")
	}
	dt := c.data[0]
	return c.dec.Decode(dt, target)
}

// Close implements domain.Cursor.
func (c *Cursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.data) > 0 {
		c.storedErr = fmt.Errorf("cursor is closed")
	}
	c.data = nil
	return nil
}

// Next implements domain.Cursor.
func (c *Cursor) Next() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.data) == 0 {
		return false
	}
	if c.started {
		c.data = c.data[1:]
	}
	c.started = true
	return len(c.data) > 0
}

func (c *Cursor) project(candidates []domain.Document, options domain.CursorOptions) ([]domain.Document, error) {

	id, idMentioned := options.Projection["_id"]
	keepID := !idMentioned || id != 0
	_projection := make(map[string]uint64, len(options.Projection))
	for field, value := range options.Projection {
		if field != "_id" {
			_projection[field] = value
		}
	}

	var expectsZero bool
	if len(_projection) != 0 {
		for _, dir := range _projection {
			expectsZero = dir == 0
			break
		}
		for _, dir := range _projection {
			gotZero := dir == 0
			if expectsZero != gotZero {
				return nil, fmt.Errorf("can't both keep and omit fields except for _id")
			}
		}
	}

	res := make([]domain.Document, len(candidates))
	for n, candidate := range candidates {
		var newCandidate map[string]any
		if expectsZero {
			newCandidate = c.asMap(candidate)
			for proj := range _projection {
				c.removeField(newCandidate, proj)
			}
		} else {
			newCandidate = make(map[string]any)
			for proj := range _projection {
				if err := c.addField(newCandidate, candidate, proj); err != nil {
					return nil, err
				}
			}
		}
		if keepID {
			newCandidate["_id"] = candidate.ID()
		}
		candidateDoc, err := options.DocumentFactory(newCandidate)
		if err != nil {
			return nil, err
		}
		res[n] = candidateDoc
	}
	return res, nil

}

func (c *Cursor) removeField(doc map[string]any, proj string) {
	projParts := strings.Split(proj, ".")
	if len(projParts) == 0 {
		return
	}

	lastPart := projParts[len(projParts)-1]
	projParts = projParts[:len(projParts)-1]
	cur := doc
	var slice []any
	for n, p := range projParts {
		slice = nil
		val, ok := cur[p]
		if !ok {
			return
		}
		switch v := val.(type) {
		// Projecting "hello.0": 0 on an array of data replaces the
		// value with null, preserving the index. It is unclear if this
		// is intended, but it was replicated regardless
		case []any:
			if n == len(projParts)-1 {
				idx, err := strconv.Atoi(lastPart)
				if err != nil || idx < 0 || idx >= len(v) {
					return
				}
				v[idx] = nil
				return
			}
			m := make(map[string]any, len(v))
			for index, item := range v {
				m[strconv.Itoa(index)] = item
			}
			cur = m
		case map[string]any:
			cur = v
		default:
			return
		}
	}
	if slice != nil {
		i, _ := strconv.Atoi(lastPart)
		slice[i] = nil
	}
	delete(doc, lastPart)
}

func (c *Cursor) sort(candidates []domain.Document, options domain.CursorOptions) ([]domain.Document, error) {

	res := slices.Clone(candidates)

	var err error
	slices.SortFunc(res, func(a, b domain.Document) int {
		if err != nil {
			return 0
		}
		for criterion, direction := range options.Sort {
			comp, cErr := c.compareByCriterion(a, b, options.Comparer, criterion, int(direction))
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

func (c *Cursor) listFields(g []domain.GetSetter) []any {
	res := make([]any, len(g))
	for n, v := range g {
		res[n] = v
	}
	return res
}
