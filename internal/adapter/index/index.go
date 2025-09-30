package index

import (
	"context"
	"maps"
	"slices"

	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/hasher"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/uncomparablemap"
)

// Index implements domain.Index.
type Index struct {
	fieldName string
	_fields   []string
	unique    bool
	sparse    bool
	// Exported to allow testing. Should not be a problem because Index is
	// used as interface.
	Tree           *bst.BinarySearchTree
	treeOptions    bst.Options
	comparer       domain.Comparer
	hasher         domain.Hasher
	fieldNavigator domain.FieldNavigator
}

// FieldName implements domain.Index.
func (i *Index) FieldName() string {
	return i.fieldName
}

// Sparse implements domain.Index.
func (i *Index) Sparse() bool {
	return i.sparse
}

// Unique implements domain.Index.
func (i *Index) Unique() bool {
	return i.unique
}

// NewIndex returns a new implementation of domain.Index.
func NewIndex(options ...domain.IndexOption) (domain.Index, error) {

	docFac := data.NewDocument
	opts := domain.IndexOptions{
		FieldName:       "",
		Unique:          false,
		Sparse:          false,
		ExpireAfter:     0,
		DocumentFactory: docFac,
		Comparer:        comparer.NewComparer(),
		Hasher:          hasher.NewHasher(),
		FieldNavigator:  fieldnavigator.NewFieldNavigator(docFac),
	}
	for _, option := range options {
		option(&opts)
	}

	if opts.Comparer == nil {
		opts.Comparer = comparer.NewComparer()
	}

	treeOptions := bst.Options{
		Unique: opts.Unique,
		CompareKeys: func(a, b any) int {
			comp, _ := opts.Comparer.Compare(a, b)
			return comp
		},
	}
	if opts.DocumentFactory == nil {
		opts.DocumentFactory = data.NewDocument
	}
	if opts.Hasher == nil {
		opts.Hasher = hasher.NewHasher()
	}
	if opts.FieldNavigator == nil {
		opts.FieldNavigator = fieldnavigator.NewFieldNavigator(opts.DocumentFactory)
	}
	// fields := strings.Split(options.FieldName, ",")
	fields, err := opts.FieldNavigator.SplitFields(opts.FieldName)
	if err != nil {
		return nil, err
	}
	return &Index{
		fieldName:      opts.FieldName,
		_fields:        fields,
		unique:         opts.Unique,
		sparse:         opts.Sparse,
		treeOptions:    treeOptions,
		Tree:           bst.NewBinarySearchTree(treeOptions),
		comparer:       opts.Comparer,
		hasher:         opts.Hasher,
		fieldNavigator: opts.FieldNavigator,
	}, nil
}

// Reset implements domain.Index.
func (i *Index) Reset(ctx context.Context, newData ...domain.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	i.Tree = bst.NewBinarySearchTree(i.treeOptions)
	return i.Insert(ctx, newData...)
}

// Insert2 implements domain.Index.
func (i *Index) getKeys(doc domain.Document) ([]any, error) {

	// When a dotted field path references multiple array elements, each
	// element is treated as an individual key and inserted separately into
	// the index
	if len(i._fields) != 1 {
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

// Insert implements domain.Index.
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
			if err = i.Tree.Insert(k, d); err != nil {
				break DocInsertion
			}

			h, err = i.hasher.Hash(k)
			if err != nil {
				break DocInsertion
			}

			ds = append(keys[h].docs, d)
			keys[h] = kv{key: k, docs: ds}
		}
	}
	if err != nil {
		for _, v := range keys {
			for _, d := range v.docs {
				i.Tree.Delete(v.key, d)
			}
		}
		return err
	}
	return nil
}

// Remove implements domain.Index.
func (i *Index) Remove(ctx context.Context, docs ...domain.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	for _, d := range docs {
		var keys []any
		NoValid := false
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

			NoValid = NoValid || hasAnyField
		}

		if i.sparse && NoValid {
			return nil
		}

		uniq := slices.Clone(keys)
		slices.SortFunc(uniq, i.compareThings)
		uniq = slices.Compact(uniq)
		for _, _key := range uniq {
			i.Tree.Delete(_key, d)
		}

		i.Tree.Delete(keys, d)
	}

	return nil
}

// Update implements domain.Index.
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

// UpdateMultipleDocs implements domain.Index.
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
		for n := range failingIndex {
			_ = i.Remove(ctx, pairs[n].NewDoc)
		}
		for _, pair := range pairs {
			_ = i.Insert(ctx, pair.OldDoc)
		}
	}

	return err
}

// RevertUpdate implements domain.Index.
func (i *Index) RevertUpdate(ctx context.Context, oldDoc, newDoc domain.Document) error {
	return i.Update(ctx, newDoc, oldDoc)
}

// RevertMultipleUpdates implements domain.Index.
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

// GetMatching implements domain.Index.
func (i *Index) GetMatching(value ...any) ([]domain.Document, error) {
	res := []domain.Document{}
	_res := uncomparablemap.New[[]domain.Document](i.hasher, i.comparer)
	for _, v := range value {
		found := i.Tree.Search(v)
		if len(found) == 0 {
			continue
		}
		id := found[0].(domain.Document).ID()
		foundDocs := make([]domain.Document, len(found))
		for n, d := range found {
			foundDocs[n] = d.(domain.Document)
		}
		_res.Set(id, foundDocs)
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
	for _, _id := range keys {
		v, _, err := _res.Get(_id)
		if err != nil {
			return nil, err
		}
		res = append(res, v...)
	}
	return res, nil
}

// GetBetweenBounds implements domain.Index.
func (i *Index) GetBetweenBounds(ctx context.Context, query domain.Document) ([]domain.Document, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m := maps.Collect(query.Iter())

	found := i.Tree.BetweenBounds(m, nil, nil)
	res := make([]domain.Document, len(found))
	for n, f := range found {
		res[n] = f.(domain.Document)
	}
	return res, nil
}

// GetAll implements domain.Index.
func (i *Index) GetAll() []domain.Document {
	var res []domain.Document
	i.Tree.ExecuteOnEveryNode(func(bst *bst.BinarySearchTree) {
		for _, data := range bst.Data() {
			res = append(res, data.(domain.Document))
		}
	})
	return res
}

// GetNumberOfKeys implements domain.Index.
func (i *Index) GetNumberOfKeys() int {
	return i.Tree.GetNumberOfKeys()
}

func (i *Index) compareThings(a any, b any) int {
	comp, _ := i.comparer.Compare(a, b)
	return comp
}
