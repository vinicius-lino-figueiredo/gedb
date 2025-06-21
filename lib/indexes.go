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
	"github.com/vinicius-lino-figueiredo/nedb"
)

type index struct {
	fieldName   string
	_fields     []string
	unique      bool
	sparse      bool
	tree        *bst.BinarySearchTree
	treeOptions bst.Options
}

func NewIndex(options nedb.IndexOptions) nedb.Index {
	treeOptions := bst.Options{
		Unique:      options.Unique,
		CompareKeys: compareThingsFunc(nil),
	}
	return &index{
		fieldName:   options.FieldName,
		_fields:     strings.Split(options.FieldName, ","),
		unique:      options.Unique,
		sparse:      options.Sparse,
		treeOptions: treeOptions,
		tree:        bst.NewBinarySearchTree(treeOptions),
	}
}

func (i *index) Reset(ctx context.Context, newData ...nedb.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	i.tree = bst.NewBinarySearchTree(i.treeOptions)
	return i.Insert(ctx, newData...)
}

func (i *index) Insert(ctx context.Context, docs ...nedb.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	type kv struct {
		key  any
		docs []nedb.Document
	}

	keys := make(map[uint64]kv, len(docs))

	var err error
	var key any
	var h uint64
	var ds []nedb.Document
	var b []byte
	var hasher hash.Hash64
DocInsertion:
	for _, d := range docs {
		key, err = getDotValues(d, i._fields)
		if err != nil {
			return err
		}

		oKey, isObj := key.(Document)
		if i.sparse && (key == nil || (isObj && !slices.ContainsFunc(slices.Collect(maps.Values(oKey)), func(el any) bool { return el != nil }))) {
			return nil
		}

		l, ok := key.(list)
		if !ok {
			l = list{key}
		}

		slices.SortFunc(l, compareThingsFunc(nil))
		l = slices.CompactFunc(l, func(a, b any) bool { return compareThingsFunc(nil)(a, b) == 0 })

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

func (i *index) Remove(ctx context.Context, docs ...nedb.Document) error {
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

		if l, ok := key.(list); ok {
			uniq := slices.Clone(l)
			slices.SortFunc(uniq, compareThingsFunc(nil))
			uniq = slices.Compact(uniq)
			for _, _key := range uniq {
				i.tree.Delete(_key, d)
			}
		}
		i.tree.Delete(key, d)
	}

	return nil
}

func (i *index) Update(ctx context.Context, oldDoc, newDoc nedb.Document) error {
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

func (i *index) UpdateMultipleDocs(ctx context.Context, pairs ...nedb.Update) error {
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

func (i *index) RevertUpdate(ctx context.Context, oldDoc, newDoc nedb.Document) error {
	return i.Update(ctx, newDoc, oldDoc)
}

func (i *index) RevertMultipleUpdates(ctx context.Context, pairs ...nedb.Update) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	revert := make([]nedb.Update, len(pairs))
	for n, pair := range pairs {
		revert[n] = nedb.Update{OldDoc: pair.NewDoc, NewDoc: pair.OldDoc}
	}
	return i.UpdateMultipleDocs(ctx, revert...)
}

func (i *index) GetMatching(value ...any) []nedb.Document {
	res := []nedb.Document{}
	_res := make(map[string][]nedb.Document)
	for _, v := range value {
		found := i.tree.Search(v)
		if len(found) == 0 {
			continue
		}
		id := found[0].(nedb.Document).ID()
		foundDocs := make([]nedb.Document, len(found))
		for n, d := range found {
			foundDocs[n] = d.(nedb.Document)
		}
		_res[id] = foundDocs
	}
	for _, v := range _res {
		res = append(res, v...)
	}
	return res
}

func (i *index) GetBetweenBounds(ctx context.Context, query any) ([]nedb.Document, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	d, err := asDoc(query)
	if err != nil {
		return nil, err
	}
	found := i.tree.BetweenBounds(d, nil, nil)
	res := make([]nedb.Document, len(found))
	for n, f := range found {
		res[n] = f.(nedb.Document)
	}
	return res, nil
}

func (i *index) GetAll() []nedb.Document {
	var res []nedb.Document
	i.tree.ExecuteOnEveryNode(func(bst *bst.BinarySearchTree) {
		for _, data := range bst.Data() {
			res = append(res, data.(Document))
		}
	})
	return res
}
