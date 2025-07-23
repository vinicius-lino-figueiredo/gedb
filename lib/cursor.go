package lib

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/vinicius-lino-figueiredo/gedb"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
)

// Cursor implements gedb.Cursor.
type Cursor struct {
	data      []any
	ctx       context.Context
	mu        *ctxsync.Mutex
	dec       gedb.Decoder
	once      sync.Once
	started   bool
	closed    bool
	storedErr error
}

// NewCursor returns a new implementation of Cursor
func NewCursor(ctx context.Context, data []gedb.Document, options gedb.CursorOptions) (gedb.Cursor, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if options.Matcher == nil {
		options.Matcher = NewMatcher(options.DocumentFactory, options.Comparer)
	}
	if options.Decoder == nil {
		options.Decoder = NewDecoder()
	}
	if options.DocumentFactory == nil {
		options.DocumentFactory = NewDocument
	}

	if len(data) == 0 || int64(len(data)) < options.Skip {
		return &Cursor{ctx: ctx, mu: ctxsync.NewMutex()}, nil
	}

	res := make([]gedb.Document, 0, len(data))

	var doesMatch bool
	var err error
	var added int64
	var skiped int64
	for _, doc := range data {
		doesMatch, err = options.Matcher.Match(doc, options.Query)
		if err != nil {
			return nil, err
		}
		if !doesMatch {
			continue
		}
		if options.Sort != nil {
			res = append(res, doc)
			continue
		}
		if options.Skip > skiped {
			skiped++
			continue
		}

		res = append(res, doc)
		added++

		if options.Limit > 0 && options.Limit <= added {
			break
		}
	}
	cur := &Cursor{
		ctx: ctx,
		mu:  ctxsync.NewMutex(),
		dec: options.Decoder,
	}

	if len(options.Sort) != 0 && len(res) != 0 {
		res, err = cur.sort(res, options)
		if err != nil {
			return nil, err
		}
	}

	// just making sure the iteration wont receive a negative number
	options.Skip = max(0, options.Skip)
	res = res[options.Skip:]

	if options.Limit <= 0 {
		options.Limit = int64(len(res))
	}
	options.Limit = min(int64(len(res)), options.Limit)

	res = res[:options.Limit]

	if len(options.Projection) != 0 && len(res) != 0 {
		res, err = cur.project(res, options)
		if err != nil {
			return nil, err
		}
	}

	values := make([]any, len(res))
	for n, doc := range res {
		values[n] = doc
	}

	cur.data = values

	return cur, nil
}

func (c *Cursor) addField(doc map[string]any, candidate gedb.Document, proj string) error {
	projFields := strings.Split(proj, ".")
	val, ok, err := getDotValuesOk(candidate, projFields...)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	curr := doc
	for i, field := range projFields {
		if i == len(projFields)-1 {
			curr[field] = val
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

func (c *Cursor) asMap(doc gedb.Document) map[string]any {
	res := make(map[string]any, doc.Len())
	for k, v := range doc.Iter() {
		if k == "_id" {
			continue
		}
		if subDoc, ok := v.(gedb.Document); ok {
			res[k] = c.asMap(subDoc)
		} else {
			res[k] = v
		}
	}
	return res
}

func (c *Cursor) compareByCriterion(a, b gedb.Document, comparer gedb.Comparer, criterion string, direction int) (int, error) {
	criterionA, err := getDotValue(a, criterion)
	if err != nil {
		return 0, err
	}
	criterionB, err := getDotValue(b, criterion)
	if err != nil {
		return 0, err
	}
	comp, err := comparer.Compare(criterionA, criterionB)
	if err != nil {
		return 0, err
	}
	return comp * direction, nil
}

// Err implements gedb.Cursor.
func (c *Cursor) Err() error {
	return c.storedErr
}

// Exec implements gedb.Cursor.
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
	data := c.data[0]
	return c.dec.Decode(data, target)
}

// Close implements gedb.Cursor.
func (c *Cursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.data) > 0 {
		c.storedErr = fmt.Errorf("cursor is closed")
	}
	c.data = nil
	return nil
}

// Next implements gedb.Cursor.
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

func (c *Cursor) project(candidates []gedb.Document, options gedb.CursorOptions) ([]gedb.Document, error) {

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

	res := make([]gedb.Document, len(candidates))
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
		// value with null, preserving the index. Not sure if this is
		// intended, but I'm replicating the behavior.
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

func (c *Cursor) sort(candidates []gedb.Document, options gedb.CursorOptions) ([]gedb.Document, error) {

	res := slices.Clone(candidates)

	var err error
	slices.SortFunc(res, func(a, b gedb.Document) int {
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
