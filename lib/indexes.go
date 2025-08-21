package lib

import (
	"context"
	"maps"
	"slices"

	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/gedb"
)

// Index implements gedb.Index.
type Index struct {
	fieldName   string
	_fields     []string
	unique      bool
	sparse      bool
	tree        *bst.BinarySearchTree
	treeOptions bst.Options
	comparer    gedb.Comparer
	hasher      gedb.Hasher
	fieldGetter gedb.FieldGetter
}

// FieldName implements gedb.Index.
func (i *Index) FieldName() string {
	return i.fieldName
}

// Sparse implements gedb.Index.
func (i *Index) Sparse() bool {
	return i.sparse
}

// Unique implements gedb.Index.
func (i *Index) Unique() bool {
	return i.unique
}

// NewIndex returns a new implementation of gedb.Index.
func NewIndex(options gedb.IndexOptions) (gedb.Index, error) {
	if options.Comparer == nil {
		options.Comparer = NewComparer()
	}

	treeOptions := bst.Options{
		Unique: options.Unique,
		CompareKeys: func(a, b any) int {
			comp, _ := options.Comparer.Compare(a, b)
			return comp
		},
	}
	if options.DocumentFactory == nil {
		options.DocumentFactory = NewDocument
	}
	if options.Hasher == nil {
		options.Hasher = NewHasher()
	}
	if options.FieldGetter == nil {
		options.FieldGetter = NewFieldGetter()
	}
	// fields := strings.Split(options.FieldName, ",")
	fields, err := options.FieldGetter.SplitFields(options.FieldName)
	if err != nil {
		return nil, err
	}
	return &Index{
		fieldName:   options.FieldName,
		_fields:     fields,
		unique:      options.Unique,
		sparse:      options.Sparse,
		treeOptions: treeOptions,
		tree:        bst.NewBinarySearchTree(treeOptions),
		comparer:    options.Comparer,
		hasher:      options.Hasher,
		fieldGetter: options.FieldGetter,
	}, nil
}

// Reset implements gedb.Index.
func (i *Index) Reset(ctx context.Context, newData ...gedb.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	i.tree = bst.NewBinarySearchTree(i.treeOptions)
	return i.Insert(ctx, newData...)
}

// Insert2 implements gedb.Index.
func (i *Index) getKeys(doc gedb.Document) ([]any, error) {

	// When a dotted field path references multiple array elements, each
	// element is treated as an individual key and inserted separately into
	// the index
	if len(i._fields) != 1 {
		var containsKey bool
		k := make(Document)
		for _, field := range i._fields {

			key, ok, err := i.fieldGetter.GetField(doc, field)
			if err != nil {
				return nil, err
			}

			if ok { // if undefined, treat as nil
				k[field] = key[0]
			} else {
				k[field] = nil
			}

			containsKey = containsKey || k[field] != nil
		}
		if i.sparse && !containsKey {
			return nil, nil
		}
		return []any{k}, nil
	}

	keysAlt, ok, err := i.fieldGetter.GetField(doc, i._fields[0])
	if err != nil {
		return nil, err
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

// Insert implements gedb.Index.
func (i *Index) Insert(ctx context.Context, docs ...gedb.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	type kv struct {
		key  any
		docs []gedb.Document
	}

	keys := make(map[uint64]kv, len(docs))

	var (
		err error
		h   uint64
		ds  []gedb.Document
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
			if err = i.tree.Insert(k, d); err != nil {
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
				i.tree.Delete(v.key, d)
			}
		}
		return err
	}
	return nil
}

// Remove implements gedb.Index.
func (i *Index) Remove(ctx context.Context, docs ...gedb.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	for _, d := range docs {
		var keys []any
		NoValid := false
		for _, field := range i._fields {
			key, ok, err := i.fieldGetter.GetField(d, field)
			if err != nil {
				return err
			}

			for _, k := range key {
				if kl, ok := k.([]any); ok {
					keys = append(keys, kl...)
				} else {
					keys = append(keys, k)
				}
			}

			NoValid = NoValid || ok
		}

		if i.sparse && NoValid {
			return nil
		}

		uniq := slices.Clone(keys)
		slices.SortFunc(uniq, i.compareThings)
		uniq = slices.Compact(uniq)
		for _, _key := range uniq {
			i.tree.Delete(_key, d)
		}

		i.tree.Delete(keys, d)
	}

	return nil
}

// Update implements gedb.Index.
func (i *Index) Update(ctx context.Context, oldDoc, newDoc gedb.Document) error {
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

// UpdateMultipleDocs implements gedb.Index.
func (i *Index) UpdateMultipleDocs(ctx context.Context, pairs ...gedb.Update) error {
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

// RevertUpdate implements gedb.Index.
func (i *Index) RevertUpdate(ctx context.Context, oldDoc, newDoc gedb.Document) error {
	return i.Update(ctx, newDoc, oldDoc)
}

// RevertMultipleUpdates implements gedb.Index.
func (i *Index) RevertMultipleUpdates(ctx context.Context, pairs ...gedb.Update) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	revert := make([]gedb.Update, len(pairs))
	for n, pair := range pairs {
		revert[n] = gedb.Update{OldDoc: pair.NewDoc, NewDoc: pair.OldDoc}
	}
	return i.UpdateMultipleDocs(ctx, revert...)
}

// GetMatching implements gedb.Index.
func (i *Index) GetMatching(value ...any) ([]gedb.Document, error) {
	res := []gedb.Document{}
	_res := newNonComparableMap[[]gedb.Document](i.hasher, i.comparer)
	for _, v := range value {
		found := i.tree.Search(v)
		if len(found) == 0 {
			continue
		}
		id := found[0].(gedb.Document).ID()
		foundDocs := make([]gedb.Document, len(found))
		for n, d := range found {
			foundDocs[n] = d.(gedb.Document)
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

// GetBetweenBounds implements gedb.Index.
func (i *Index) GetBetweenBounds(ctx context.Context, query gedb.Document) ([]gedb.Document, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m := maps.Collect(query.Iter())

	found := i.tree.BetweenBounds(m, nil, nil)
	res := make([]gedb.Document, len(found))
	for n, f := range found {
		res[n] = f.(gedb.Document)
	}
	return res, nil
}

// GetAll implements gedb.Index.
func (i *Index) GetAll() []gedb.Document {
	var res []gedb.Document
	i.tree.ExecuteOnEveryNode(func(bst *bst.BinarySearchTree) {
		for _, data := range bst.Data() {
			res = append(res, data.(gedb.Document))
		}
	})
	return res
}

// GetNumberOfKeys implements gedb.Index.
func (i *Index) GetNumberOfKeys() int {
	return i.tree.GetNumberOfKeys()
}

func (i *Index) compareThings(a any, b any) int {
	comp, _ := i.comparer.Compare(a, b)
	return comp
}
