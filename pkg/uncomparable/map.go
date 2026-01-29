// Package uncomparable contains an implementation that allows user to create
// a map with key of type [any] using the given compare function, which returns
// an error instead of panicking.
package uncomparable

import (
	"iter"
	"slices"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Map represents a map[K]T, where T does not need to be [comparable].
type Map[T any] struct {
	buckets  [][]kv[T]
	hasher   domain.Hasher
	comparer domain.Comparer
	length   int
}

// New returns a new instance of [Map] with the given [domain.Hasher] and
// [domain.Comparer].
func New[T any](hasher domain.Hasher, comparer domain.Comparer) *Map[T] {
	return &Map[T]{
		buckets:  make([][]kv[T], 8),
		hasher:   hasher,
		comparer: comparer,
	}
}

// Delete removes a given key from the map, if it exists. If the given key could
// not be hashed or some comparison failed, it returns the error.
func (m *Map[T]) Delete(key any) error {
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
			m.length--
			m.buckets[bucketIndex] = slices.Delete(bucket, n, n+1)
			return nil
		}
	}
	return nil
}

// Get returns the value for the given key with a bool to indicate whether it
// exists in the map or not. If hash or comparison fails, returns an error.
func (m *Map[T]) Get(key any) (T, bool, error) {
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

func (m *Map[T]) getBucketIndex(key any) (uint64, error) {
	h, err := m.hasher.Hash(key)
	if err != nil {
		return 0, err
	}
	return h % uint64(len(m.buckets)), nil
}

// Keys returns an unordered [iter.Seq] containing all the stored keys.
func (m *Map[T]) Keys() iter.Seq[any] {
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

// Len returns the amount of stored values.
func (m *Map[T]) Len() int {
	return m.length
}

// Iter returns an unordered [iter.Seq2] containing all the key+value pairs.
func (m *Map[T]) Iter() iter.Seq2[any, T] {
	return func(yield func(any, T) bool) {
		for _, bucket := range m.buckets {
			for _, v := range bucket {
				if !yield(v.key, v.value) {
					return
				}
			}
		}
	}
}

// Set adds or replaces the given key in the map, returning error on hash or
// comparison failure.
func (m *Map[T]) Set(key any, value T) error {
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
			m.length++
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

// Values returns an unordered [iter.Seq] containing all the stored values.
func (m *Map[T]) Values() iter.Seq[T] {
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
