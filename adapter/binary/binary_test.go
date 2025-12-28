package binary

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	value := map[string]string{
		"exemplo":  "ttestes",
		"funciona": "dsakd\najksndd",
	}
	allocs := testing.AllocsPerRun(100, func() {

		obj, err := NewDoc(value)
		if err != nil || obj == nil {
			t.FailNow()
		}

	})
	fmt.Printf("alocações: %.2f", allocs)
}
