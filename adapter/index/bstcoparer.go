package index

import (
	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

type bstComparer struct {
	comparer domain.Comparer
}

// NewBSTComparer TODO
func NewBSTComparer(comparer domain.Comparer) bst.Comparer[any, domain.Document] {
	return &bstComparer{
		comparer: comparer,
	}
}

// CompareKeys implements bst.Comparer.
func (bc *bstComparer) CompareKeys(a any, b any) (int, error) {
	return bc.comparer.Compare(a, b)
}

// CompareValues implements bst.Comparer.
func (bc *bstComparer) CompareValues(a domain.Document, b domain.Document) (bool, error) {
	c, err := bc.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return c == 0, nil
}
