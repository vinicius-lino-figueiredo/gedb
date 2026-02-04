package gedb_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/vinicius-lino-figueiredo/gedb"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

type M = map[string]any

func BenchmarkCreateInMemory(b *testing.B) {
	b.Run("InMemory=true", func(b *testing.B) {
		for b.Loop() {
			gedb.NewDB()
		}
	})

	b.Run("InMemory=false", func(b *testing.B) {
		file := filepath.Join(b.TempDir(), "file.db")
		for b.Loop() {
			gedb.NewDB(gedb.WithFilename(file))
		}
	})
}

func BenchmarkInsert(b *testing.B) {
	ctx := context.Background()

	m := M{"jo": "jo"}

	b.Run("InMemory=true", func(b *testing.B) {
		db, _ := gedb.NewDB()

		for b.Loop() {
			db.Insert(ctx, m)
		}
	})

	b.Run("InMemory=false", func(b *testing.B) {
		file := filepath.Join(b.TempDir(), "file.db")
		db, _ := gedb.NewDB(gedb.WithFilename(file))

		for b.Loop() {
			db.Insert(ctx, m)
		}
	})
}

func BenchmarkInsertBatch(b *testing.B) {

	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			m := make([]any, size)
			for n := range size {
				m[n] = M{"part": n + 1}
			}

			db, _ := gedb.NewDB()
			for b.Loop() {
				cur, err := db.Insert(ctx, m...)
				if err != nil {
					b.FailNow()
				}
				_ = cur
			}

			perItem := float64(b.Elapsed().Nanoseconds()) / float64(b.N*size)

			b.ReportMetric(perItem, "ns/item")

		})
	}

}

func BenchmarkFind(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"code": n})
			}

			b.Run("Existing", func(b *testing.B) {
				for b.Loop() {
					cur, err := db.Find(ctx, M{"code": rand.Intn(size)})
					if err != nil {
						b.FailNow()
					}
					_ = cur
				}
			})

			b.Run("NonExisting", func(b *testing.B) {
				m := M{"code": size + 12}
				for b.Loop() {
					cur, err := db.Find(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = cur
				}
			})

		})
	}
}

func BenchmarkFindWithIndex(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"_id": n})
			}

			b.Run("Existing", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					id := rand.Intn(size)
					b.StartTimer()
					cur, err := db.Find(ctx, M{"_id": id})
					if err != nil {
						b.FailNow()
					}
					_ = cur
				}
			})

			b.Run("NonExisting", func(b *testing.B) {
				m := M{"_id": size + 12}
				for b.Loop() {
					cur, err := db.Find(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = cur
				}
			})

		})
	}
}

func BenchmarkFindOne(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	var t M

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"code": n})
			}

			b.Run("Existing", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					id := rand.Intn(size)
					b.StartTimer()
					err := db.FindOne(ctx, M{"code": id}, &t)
					if err != nil {
						b.Log(err.Error())
						b.FailNow()
					}
				}
			})

			b.Run("NonExisting", func(b *testing.B) {
				m := M{"code": size + 12}
				for b.Loop() {
					err := db.FindOne(ctx, m, &t)
					if err == nil {
						b.FailNow()
					}
				}
			})

		})
	}
}

func BenchmarkCount(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"phantom": n})
			}

			b.Run("amount=100%", func(b *testing.B) {
				for b.Loop() {
					c, err := db.Count(ctx, nil)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})

			db, _ = gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"tendency": n, "value": n % 2})
			}

			b.Run("amount=50%", func(b *testing.B) {
				m := M{"value": 1}
				for b.Loop() {
					c, err := db.Count(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})

			db, _ = gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"stardust": n, "value": n % 3})
			}

			b.Run("amount=33%", func(b *testing.B) {
				m := M{"value": 1}
				for b.Loop() {
					c, err := db.Count(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})

			db, _ = gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"diamond": n, "value": n % 4})
			}

			b.Run("amount=25%", func(b *testing.B) {
				m := M{"value": 1}
				for b.Loop() {
					c, err := db.Count(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})
		})
	}
}

func BenchmarkCountWithIndex(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			db.EnsureIndex(ctx, gedb.WithFields("wind"))
			for n := range size {
				db.Insert(ctx, M{"wind": n})
			}

			b.Run("amount=100%", func(b *testing.B) {
				for b.Loop() {
					c, err := db.Count(ctx, M{"wind": M{"$ne": "a"}})
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})

			db, _ = gedb.NewDB()
			db.EnsureIndex(ctx, gedb.WithFields("value"))
			for n := range size {
				db.Insert(ctx, M{"ocean": n, "value": n % 2})
			}

			b.Run("amount=50%", func(b *testing.B) {
				m := M{"value": 1}
				for b.Loop() {
					c, err := db.Count(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})

			db, _ = gedb.NewDB()
			db.EnsureIndex(ctx, gedb.WithFields("value"))
			for n := range size {
				db.Insert(ctx, M{"steel": n, "value": n % 3})
			}

			b.Run("amount=33%", func(b *testing.B) {
				m := M{"value": 1}
				for b.Loop() {
					c, err := db.Count(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})

			db, _ = gedb.NewDB()
			db.EnsureIndex(ctx, gedb.WithFields("value"))
			for n := range size {
				db.Insert(ctx, M{"lion": n, "value": n % 4})
			}

			b.Run("amount=25%", func(b *testing.B) {
				m := M{"value": 1}
				for b.Loop() {
					c, err := db.Count(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})
		})
	}
}

func BenchmarkUpdate(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	nw := M{"up": "dated"}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"code": n})
			}

			b.Run("Existing", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					m := M{"code": rand.Intn(size)}
					b.StartTimer()
					cur, err := db.Update(ctx, m, nw)
					if err != nil {
						b.FailNow()
					}
					_ = cur
				}
			})

			b.Run("NonExisting", func(b *testing.B) {
				m := M{"code": size + 12}
				for b.Loop() {
					cur, err := db.Update(ctx, m, nw)
					if err != nil {
						b.FailNow()
					}
					_ = cur
				}
			})

		})
	}
}

func BenchmarkRemove(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"code": n})
			}

			b.Run("Existing", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					m := M{"code": rand.Intn(size)}
					b.StartTimer()
					c, err := db.Remove(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})

			b.Run("NonExisting", func(b *testing.B) {
				m := M{"code": size + 12}
				for b.Loop() {
					c, err := db.Remove(ctx, m)
					if err != nil {
						b.FailNow()
					}
					_ = c
				}
			})

		})
	}
}

func BenchmarkEnsureIndex(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{0, 1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"code": n, "data": n + 1})
			}

			b.Run("Existing", func(b *testing.B) {
				for b.Loop() {
					db.EnsureIndex(ctx,
						domain.WithFields("code"),
					)
				}
			})

			b.Run("NonExisting", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					db.RemoveIndex(ctx, "nope")
					b.StartTimer()
					db.EnsureIndex(ctx,
						domain.WithFields("nope"),
					)
				}
			})

			b.Run("ExistingCompose", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					db.RemoveIndex(ctx, "code", "data")
					b.StartTimer()
					db.EnsureIndex(ctx,
						domain.WithFields("code", "data"),
					)
				}
			})

			b.Run("PartiallyExistingCompose", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					db.RemoveIndex(ctx, "code", "nope")
					b.StartTimer()
					db.EnsureIndex(ctx,
						domain.WithFields("code", "nope"),
					)
				}
			})

			b.Run("NonExistingCompose", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					db.RemoveIndex(ctx, "nope", "nah")
					b.StartTimer()
					db.EnsureIndex(ctx,
						domain.WithFields("nope", "nah"),
					)
				}
			})

		})
	}
}

func BenchmarkGetAllData(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{0, 1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"code": n})
			}

			cur, err := db.GetAllData(ctx)
			if err != nil {
				b.FailNow()
			}
			_ = cur

		})
	}
}

func BenchmarkInsertWithPersistence(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			m := make([]any, size)
			for n := range size {
				m[n] = M{"part": n + 1}
			}

			db, _ := gedb.NewDB()
			for b.Loop() {
				cur, err := db.Insert(ctx, m...)
				if err != nil {
					b.FailNow()
				}
				_ = cur
			}

			perItem := float64(b.Elapsed().Nanoseconds()) / float64(b.N*size)

			b.ReportMetric(perItem, "ns/item")

		})
	}

}

func BenchmarkCompactDatafile(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{0, 1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"code": n})
			}

			for b.Loop() {
				err := db.CompactDatafile(ctx)
				if err != nil {
					b.FailNow()
				}
			}

		})
	}
}

func BenchmarkFindWithSort(b *testing.B) {
	ctx := context.Background()

	sizes := [...]int{1, 10, 100, 1_000, 10_000, 100_000}

	for _, size := range sizes {

		values := make([]int, size)
		for n := range size {
			values[n] = n
		}

		rand.Shuffle(size, func(i, j int) {
			values[i], values[j] = values[j], values[i]
		})

		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			db, _ := gedb.NewDB()
			for n := range size {
				db.Insert(ctx, M{"code": values[n]})
			}

			for b.Loop() {
				cur, err := db.Find(ctx, nil, domain.WithSort(gedb.Sort{{Key: "code"}}))
				if err != nil {
					b.FailNow()
				}
				_ = cur
			}

		})
	}
}
