package lib

import (
	"context"
	"encoding/json"
	"hash"
	"hash/fnv"
	"maps"
	"slices"
	"strings"

	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/gedb"
)

// Index implements gedb.Index.
type Index struct {
	fieldName       string
	_fields         []string
	unique          bool
	sparse          bool
	tree            *bst.BinarySearchTree
	treeOptions     bst.Options
	documentFactory func(any) (gedb.Document, error)
	comparer        gedb.Comparer
	hasher          gedb.Hasher
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
func NewIndex(options gedb.IndexOptions) gedb.Index {
	if options.Comparer == nil {
		options.Comparer = NewComparer()
	}

	if options.DTO != nil {
		options.FieldName = options.DTO.IndexCreated.FieldName
		options.Unique = options.DTO.IndexCreated.Unique
		options.Sparse = options.DTO.IndexCreated.Sparse
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
	return &Index{
		fieldName:       options.FieldName,
		_fields:         strings.Split(options.FieldName, ","),
		unique:          options.Unique,
		sparse:          options.Sparse,
		treeOptions:     treeOptions,
		tree:            bst.NewBinarySearchTree(treeOptions),
		documentFactory: options.DocumentFactory,
		comparer:        options.Comparer,
		hasher:          options.Hasher,
	}
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

	var err error
	var key any
	var h uint64
	var ds []gedb.Document
	var b []byte
	var hasher hash.Hash64
DocInsertion:
	for _, d := range docs {
		key, err = getDotValues(d, i._fields)
		if err != nil {
			return err
		}

		oKey, isObj := key.(gedb.Document)
		if i.sparse && (key == nil || (isObj && !slices.ContainsFunc(slices.Collect(oKey.Values()), func(el any) bool { return el != nil }))) {
			return nil
		}

		l, ok := key.([]any)
		if !ok {
			l = []any{key}
		}

		slices.SortFunc(l, i.compareThings)
		l = slices.CompactFunc(l, func(a, b any) bool { return i.compareThings(a, b) == 0 })

		for _, k := range l {
			if err = i.tree.Insert(k, d); err != nil {
				break DocInsertion
			}

			b, err = json.Marshal(k)
			if err != nil {
				break
			}
			hasher = fnv.New64a()
			_, err = hasher.Write(b)
			if err != nil {
				break
			}
			h = hasher.Sum64()
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

	var key any
	var err error
	for _, d := range docs {
		key, err = getDotValue(d, i._fields...)
		if err != nil {
			return err
		}

		if key == nil && i.sparse {
			return nil
		}

		if l, ok := key.([]any); ok {
			uniq := slices.Clone(l)
			slices.SortFunc(uniq, i.compareThings)
			uniq = slices.Compact(uniq)
			for _, _key := range uniq {
				i.tree.Delete(_key, d)
			}
		}
		i.tree.Delete(key, d)
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
func (i *Index) GetBetweenBounds(ctx context.Context, query any) ([]gedb.Document, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	d, err := i.documentFactory(query)
	if err != nil {
		return nil, err
	}

	m := maps.Collect(d.Iter())

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

func (i *Index) compareThings(a any, b any) int {
	comp, _ := i.comparer.Compare(a, b)
	return comp
}
