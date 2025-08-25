package hasher

import (
	"encoding/json"
	"hash/fnv"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Hasher implements domain.Hasher.
type Hasher struct{}

func NewHasher() domain.Hasher {
	return &Hasher{}
}

// Hash implements domain.Hasher.
func (h *Hasher) Hash(a any) (uint64, error) {
	b, err := json.Marshal(a)
	if err != nil {
		return 0, err
	}
	hasher := fnv.New64a()
	_, err = hasher.Write(b)
	if err != nil {
		return 0, err
	}
	return hasher.Sum64(), nil
}
