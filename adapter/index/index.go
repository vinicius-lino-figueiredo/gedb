// Package index contains the default [domain.Index] implementation.
package index

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/bst/adapter/unbalanced"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/hasher"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/uncomparable"
)

// Index implements [domain.Index].
type Index struct {
	fieldName string
	_fields   []string
	unique    bool
	sparse    bool
	// Exported to allow testing. Should not be a problem because Index is
	// used as interface.
	Tree           bst.BST[any, domain.Document]
	comparer       domain.Comparer
	bstComparer    bst.Comparer[any, domain.Document]
	hasher         domain.Hasher
	fieldNavigator domain.FieldNavigator
}

// FieldName implements [domain.Index].
func (i *Index) FieldName() string {
	return i.fieldName
}

// Sparse implements [domain.Index].
func (i *Index) Sparse() bool {
	return i.sparse
}

// Unique implements [domain.Index].
func (i *Index) Unique() bool {
	return i.unique
}

// NewIndex returns a new implementation of domain.Index.
func NewIndex(options ...domain.IndexOption) (domain.Index, error) {

	opts := domain.IndexOptions{
		FieldName:       "",
		Unique:          false,
		Sparse:          false,
		ExpireAfter:     0,
		DocumentFactory: data.NewDocument,
		Comparer:        comparer.NewComparer(),
		Hasher:          hasher.NewHasher(),
	}
	for _, option := range options {
		option(&opts)
	}

	if opts.FieldNavigator == nil {
		opts.FieldNavigator = fieldnavigator.NewFieldNavigator(opts.DocumentFactory)
	}

	fields, err := opts.FieldNavigator.SplitFields(opts.FieldName)
	if err != nil {
		return nil, err
	}

	bstComparer := NewBSTComparer(opts.Comparer)

	return &Index{
		fieldName:      opts.FieldName,
		_fields:        fields,
		unique:         opts.Unique,
		sparse:         opts.Sparse,
		Tree:           unbalanced.NewBST(opts.Unique, 8, bstComparer),
		comparer:       opts.Comparer,
		bstComparer:    bstComparer,
		hasher:         opts.Hasher,
		fieldNavigator: opts.FieldNavigator,
	}, nil
}

// Reset implements [domain.Index].
func (i *Index) Reset(ctx context.Context, newData ...domain.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	i.Tree = unbalanced.NewBST(i.unique, 8, i.bstComparer)
	return i.Insert(ctx, newData...)
}

func (i *Index) getKeys(doc domain.Document) ([]any, error) {

	// When a dotted field path references multiple array elements, each
	// element is treated as an individual key and inserted separately into
	// the index
	if len(i._fields) != 1 {
		return i.getKeysMultiField(doc)
	}

	addr, err := i.fieldNavigator.GetAddress(i._fields[0])
	if err != nil {
		return nil, err
	}

	fieldValues, _, err := i.fieldNavigator.GetField(doc, addr...)
	if err != nil {
		return nil, err
	}

	keysAlt := make([]any, len(fieldValues))
	ok := false
	for n, fieldValue := range fieldValues {
		keyAlt, isSet := fieldValue.Get()
		if isSet && !ok {
			ok = true
		}
		keysAlt[n] = keyAlt
	}

	if i.sparse && !ok {
		return nil, nil
	}

	if len(keysAlt) == 0 {
		return []any{nil}, nil
	}

	if l, ok := keysAlt[0].([]any); ok {
		return l, nil
	}

	return keysAlt, nil

}

func (i *Index) getKeysMultiField(doc domain.Document) ([]any, error) {
	var containsKey bool
	k := make(data.M)
	for _, field := range i._fields {

		addr, err := i.fieldNavigator.GetAddress(field)
		if err != nil {
			return nil, err
		}

		key, _, err := i.fieldNavigator.GetField(doc, addr...)
		if err != nil {
			return nil, err
		}

		k[field] = nil
		values := make([]any, len(key))
		ok := false
		for n, v := range key {
			value, isSet := v.Get()
			if isSet && !ok {
				ok = true
			}
			values[n] = value
		}

		if ok { // if undefined, treat as nil
			k[field] = values[0]
		}

		containsKey = containsKey || k[field] != nil
	}
	if i.sparse && !containsKey {
		return nil, nil
	}
	return []any{k}, nil
}

// Insert implements [domain.Index].
func (i *Index) Insert(ctx context.Context, docs ...domain.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	type kv struct {
		key  any
		docs []domain.Document
	}

	keys := make(map[uint64]kv, len(docs))

	var (
		err error
		h   uint64
		ds  []domain.Document
	)
DocInsertion:
	for _, d := range docs {
		var l []any
		l, err = i.getKeys(d)
		if err != nil {
			break
		}

		slices.SortFunc(l, i.compareThings)
		l = slices.CompactFunc(l, func(a, b any) bool { return i.compareThings(a, b) == 0 })

		for _, k := range l {
			h, err = i.hasher.Hash(k)
			if err != nil {
				break DocInsertion
			}

			if err = i.Tree.Insert(k, d); err != nil {
				if e := new(bst.ErrUniqueViolated); errors.As(err, e) {
					err = fmt.Errorf("%w: %w", domain.ErrConstraintViolated, err)
				}
				break DocInsertion
			}

			ds = append(keys[h].docs, d)
			keys[h] = kv{key: k, docs: ds}
		}
	}
	if err != nil {
		nErrs := make([]error, 1, len(keys)+1)
		nErrs[0] = err
		for _, v := range keys {
			for _, d := range v.docs {
				if err := i.Tree.Delete(v.key, &d); err != nil {
					nErrs = append(nErrs, err)
				}
			}
		}
		if len(nErrs) > 1 {
			return errors.Join(nErrs...)
		}
		return err
	}
	return nil
}

// Remove implements [domain.Index].
func (i *Index) Remove(ctx context.Context, docs ...domain.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	for _, d := range docs {
		var keys []any
		noValidField := false
		for _, field := range i._fields {
			addr, err := i.fieldNavigator.GetAddress(field)
			if err != nil {
				return err
			}
			key, _, err := i.fieldNavigator.GetField(d, addr...)
			if err != nil {
				return err
			}

			hasAnyField := false
			for _, k := range key {
				value, isSet := k.Get()
				if isSet {
					hasAnyField = true
				}
				if kl, ok := value.([]any); ok {
					keys = append(keys, kl...)
				} else {
					keys = append(keys, value)
				}
			}

			noValidField = noValidField || hasAnyField
		}

		if i.sparse && noValidField {
			return nil
		}

		uniq := slices.Clone(keys)
		slices.SortFunc(uniq, i.compareThings)
		uniq = slices.Compact(uniq)
		for _, _key := range uniq {
			i.Tree.Delete(_key, &d)
		}

		i.Tree.Delete(keys, &d)
	}

	return nil
}

// Update implements [domain.Index].
func (i *Index) Update(ctx context.Context, oldDoc, newDoc domain.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := i.Remove(ctx, oldDoc)
	if err != nil {
		return err
	}
	err = i.Insert(ctx, newDoc)
	if err != nil {
		_ = i.Insert(context.WithoutCancel(context.Background()), oldDoc)
		return err
	}
	return nil
}

// UpdateMultipleDocs implements [domain.Index].
func (i *Index) UpdateMultipleDocs(ctx context.Context, pairs ...domain.Update) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var failingIndex int
	var err error

	subCtx := context.WithoutCancel(ctx)
	for _, pair := range pairs {
		_ = i.Remove(subCtx, pair.OldDoc)
	}

Loop:
	for n, pair := range pairs {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			failingIndex = n
			break Loop
		default:
		}

		if err = i.Insert(ctx, pair.NewDoc); err != nil {
			failingIndex = n
			break
		}
	}

	if err != nil {
		ctx := context.WithoutCancel(ctx)
		for n := range failingIndex {
			_ = i.Remove(ctx, pairs[n].NewDoc)
		}
		for _, pair := range pairs {
			_ = i.Insert(ctx, pair.OldDoc)
		}
	}

	return err
}

// RevertUpdate implements [domain.Index].
func (i *Index) RevertUpdate(ctx context.Context, oldDoc, newDoc domain.Document) error {
	return i.Update(ctx, newDoc, oldDoc)
}

// RevertMultipleUpdates implements [domain.Index].
func (i *Index) RevertMultipleUpdates(ctx context.Context, pairs ...domain.Update) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	revert := make([]domain.Update, len(pairs))
	for n, pair := range pairs {
		revert[n] = domain.Update{OldDoc: pair.NewDoc, NewDoc: pair.OldDoc}
	}
	return i.UpdateMultipleDocs(ctx, revert...)
}

// GetMatching implements [domain.Index].
func (i *Index) GetMatching(value ...any) ([]domain.Document, error) {
	res := []domain.Document{}
	_res := uncomparable.New[[]domain.Document](i.hasher, i.comparer)
	for _, v := range value {
		found, err := i.Tree.Search(v)
		if err != nil {
			return nil, err
		}
		if found == nil || len(found.Values) == 0 {
			continue
		}
		foundDocs := slices.Clone(found.Values)
		if err := _res.Set(found.Key, foundDocs); err != nil {
			return nil, err
		}
	}
	keys := slices.Collect(_res.Keys())
	var err error
	slices.SortFunc(keys, func(a, b any) int {
		if err != nil {
			return 0
		}
		comp, compErr := i.comparer.Compare(a, b)
		if compErr != nil {
			err = compErr
		}
		return comp
	})
	if err != nil {
		return nil, err
	}
	for _, _id := range keys {
		v, _, err := _res.Get(_id)
		if err != nil {
			return nil, err
		}
		res = append(res, v...)
	}
	return res, nil
}

// GetBetweenBounds implements [domain.Index].
func (i *Index) GetBetweenBounds(ctx context.Context, query domain.Document) ([]domain.Document, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var qry bst.Query[any]
	for k, v := range query.Iter() {
		switch k {
		case "$gt":
			qry.GreaterThan = &bst.Bound[any]{Value: v, IncludeEqual: false}
		case "$gte":
			qry.GreaterThan = &bst.Bound[any]{Value: v, IncludeEqual: true}
		case "$lt":
			qry.LowerThan = &bst.Bound[any]{Value: v, IncludeEqual: false}
		case "$lte":
			qry.LowerThan = &bst.Bound[any]{Value: v, IncludeEqual: true}
		}
	}

	found := i.Tree.Query(qry)
	res := make([]domain.Document, 0, i.GetNumberOfKeys())
	for f, err := range found {
		if err != nil {
			return nil, err
		}
		res = append(res, f)
	}
	return res, nil
}

// GetAll implements [domain.Index].
func (i *Index) GetAll() []domain.Document {
	return slices.Collect(i.Tree.GetAll())
}

// GetNumberOfKeys implements [domain.Index].
func (i *Index) GetNumberOfKeys() int {
	return i.Tree.GetNumberOfKeys()
}

func (i *Index) compareThings(a any, b any) int {
	comp, _ := i.comparer.Compare(a, b)
	return comp
}
