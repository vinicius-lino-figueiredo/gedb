package lib

import (
	"iter"
	"slices"

	"github.com/vinicius-lino-figueiredo/gedb"
)

type nonComparableMap[T any] struct {
	buckets  [][]kv[T]
	hasher   gedb.Hasher
	comparer gedb.Comparer
}

func newNonComparableMap[T any](hasher gedb.Hasher, comparer gedb.Comparer) *nonComparableMap[T] {
	return &nonComparableMap[T]{
		buckets:  make([][]kv[T], 8),
		hasher:   hasher,
		comparer: comparer,
	}
}

func (m *nonComparableMap[T]) Delete(key any) error {
	bucketIndex, err := m.getBucketIndex(key)
	if err != nil {
		return err
	}

	bucket := m.buckets[bucketIndex]

	for n, keyVal := range bucket {
		c, err := m.comparer.Compare(key, keyVal.key)
		if err != nil {
			return err
		}
		if c == 0 {
			m.buckets[bucketIndex] = slices.Delete(bucket, n, n+1)
			return nil
		}
	}
	return nil
}

func (m *nonComparableMap[T]) Get(key any) (T, bool, error) {
	bucketIndex, err := m.getBucketIndex(key)
	if err != nil {
		return *new(T), false, err
	}

	bucket := m.buckets[bucketIndex]

	for _, keyVal := range bucket {
		c, err := m.comparer.Compare(key, keyVal.key)
		if err != nil {
			return *new(T), false, err
		}
		if c == 0 {
			return keyVal.value, true, nil
		}
	}
	return *new(T), false, nil
}

func (m *nonComparableMap[T]) getBucketIndex(key any) (uint64, error) {
	h, err := m.hasher.Hash(key)
	if err != nil {
		return 0, err
	}
	return h % uint64(len(m.buckets)), nil
}

func (m *nonComparableMap[T]) Keys() iter.Seq[any] {
	return func(yield func(any) bool) {
		for _, bucket := range m.buckets {
			for _, v := range bucket {
				if !yield(v.key) {
					return
				}
			}
		}
	}
}

func (m *nonComparableMap[T]) Set(key any, value T) error {
	bucketIndex, err := m.getBucketIndex(key)
	if err != nil {
		return err
	}

	bucket := m.buckets[bucketIndex]
	for n, v := range bucket {
		c, err := m.comparer.Compare(key, v.key)
		if err != nil {
			return err
		}
		if c == 0 {
			m.buckets[bucketIndex][n] = kv[T]{
				key:   key,
				value: value,
			}
			return nil
		}
	}

	m.buckets[bucketIndex] = append(bucket, kv[T]{
		key:   key,
		value: value,
	})

	return nil
}

func (m *nonComparableMap[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, bucket := range m.buckets {
			for _, v := range bucket {
				if !yield(v.value) {
					return
				}
			}
		}
	}
}

type kv[T any] struct {
	key   any
	value T
}
