# GEDB

[![Go Version](https://img.shields.io/github/go-mod/go-version/vinicius-lino-figueiredo/gedb)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/vinicius-lino-figueiredo/gedb.svg)](https://pkg.go.dev/github.com/vinicius-lino-figueiredo/gedb)
[![Go Report Card](https://goreportcard.com/badge/github.com/vinicius-lino-figueiredo/gedb)](https://goreportcard.com/report/github.com/vinicius-lino-figueiredo/gedb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](https://github.com/vinicius-lino-figueiredo/gedb)

An embedded MongoDB-like database for Go, compatible with [NeDB](https://github.com/louischatriot/nedb).

GEDB provides a lightweight, pure-Go embedded database with a familiar
MongoDB-style query API. Perfect for CLI tools, desktop applications, small
services, or anywhere you need local data persistence without external
dependencies.

## Features

- **MongoDB-style queries** — `$and`, `$or`, `$in`, `$gt`, `$lt`, `$regex`, `$elemMatch`, and more
- **Indexing** — Single field, compound, unique, sparse, and TTL indexes
- **Persistence** — Append-only file format with crash recovery
- **In-memory mode** — Run without persistence for testing or caching
- **Context support** — All operations respect context cancellation
- **Concurrency safe** — Safe to use from multiple goroutines
- **Fully modular** — Replace any component (serializer, comparer, matcher, etc.)
- **100% test coverage** — Thoroughly tested codebase

## Installation

```bash
go get github.com/vinicius-lino-figueiredo/gedb
```

Requires Go 1.24+

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/vinicius-lino-figueiredo/gedb"
)

type M = map[string]any

func main() {
    // Create an in-memory database
    db, _ := gedb.NewDB()
    ctx := context.Background()

    // Insert documents
    db.Insert(ctx,
        M{"name": "Alice", "age": 30, "city": "Recife"},
        M{"name": "Bob", "age": 25, "city": "Salvador"},
        M{"name": "Carol", "age": 35, "city": "Recife"},
    )

    // Find documents
    cur, _ := db.Find(ctx, M{"city": "Recife"})
    defer cur.Close()

    for cur.Next() {
        var doc M
        cur.Scan(ctx, &doc)
        fmt.Println(doc["name"]) // Alice, Carol
    }
}
```

## Persistence

```go
// Create a persistent database
db, _ := gedb.NewDB(gedb.WithFilename("mydata.db"))

ctx := context.Background()

// Load existing data from file
db.LoadDatabase(ctx)

db.CompactDatafile(ctx)
```

## Query Operators

### Comparison

```go
// Equality
db.Find(ctx, M{"age": 25})

// Greater than / less than
db.Find(ctx, M{"age": M{"$gt": 20}})
db.Find(ctx, M{"age": M{"$gte": 20}})
db.Find(ctx, M{"age": M{"$lt": 30}})
db.Find(ctx, M{"age": M{"$lte": 30}})

// Not equal
db.Find(ctx, M{"status": M{"$ne": "inactive"}})

// In / Not in
db.Find(ctx, M{"city": M{"$in": []any{"Recife", "Salvador"}}})
db.Find(ctx, M{"city": M{"$nin": []any{"Teresina"}}})

// Exists
db.Find(ctx, M{"email": M{"$exists": true}})
```

### Logical

```go
// AND (implicit)
db.Find(ctx, M{"age": 25, "city": "Recife"})

// AND (explicit)
db.Find(ctx, M{"$and": []any{
    M{"age": M{"$gte": 20}},
    M{"age": M{"$lte": 30}},
}})

// OR
db.Find(ctx, M{"$or": []any{
    M{"city": "Recife"},
    M{"city": "Salvador"},
}})

// NOT
db.Find(ctx, M{"$not": M{"status": "inactive"}})
```

### Array & Pattern

```go
// Regex
db.Find(ctx, M{"name": M{"$regex": regexp.MustCompile("^A")}})

// Array size
db.Find(ctx, M{"tags": M{"$size": 3}})

// Element match
db.Find(ctx, M{"scores": M{"$elemMatch": M{"$gte": 80}}})
```

### Custom Functions

```go
// $where with custom function
db.Find(ctx, M{"$where": func(doc any) (bool, error) {
    d := doc.(gedb.Document)
    age, _ := d.Get("age").(int)
    return age > 21, nil
}})
```

## Query Options

```go
cur, _ := db.Find(ctx, M{"city": "Recife"},
    // Select fields to return (1 = include, 0 = exclude)
    gedb.WithProjection(M{"name": 1, "age": 1, "_id": 0}),

    // Sort results (positive = ascending, negative = descending)
    gedb.WithSort(gedb.Sort{{Key: "age", Order: -1}}),

    // Pagination
    gedb.WithSkip(10),
    gedb.WithLimit(5),
)
```

## Update Operations

```go
// Update single document
db.Update(ctx,
    M{"name": "Alice"},      // query
    M{"$set": M{"age": 31}}, // update
)

// Update multiple documents
db.Update(ctx,
    M{"city": "Recife"},
    M{"$set": M{"country": "Brasil"}},
    gedb.WithUpdateMulti(true),
)

// Upsert (insert if not found)
db.Update(ctx,
    M{"name": "Davi"},
    M{"name": "Davi", "age": 28},
    gedb.WithUpsert(true),
)
```

## Remove Operations

```go
// Remove single document
db.Remove(ctx, M{"name": "André"})

// Remove multiple documents
db.Remove(ctx, M{"status": "inactive"}, gedb.WithRemoveMulti(true))
```

## Indexes

```go
// Create a unique index
db.EnsureIndex(ctx, gedb.WithFields("email"), gedb.WithUnique(true))

// Create a compound index
db.EnsureIndex(ctx, gedb.WithFields("lastName", "firstName"))

// Create a sparse index (excludes documents without the field)
db.EnsureIndex(ctx, gedb.WithFields("optionalField"), gedb.WithSparse(true))

// Create a TTL index (auto-delete after duration)
db.EnsureIndex(ctx, gedb.WithFields("createdAt"), gedb.WithTTL(24 * time.Hour))

// Remove an index
db.RemoveIndex(ctx, "email")
```

## Using Structs

```go
type User struct {
    Name     string   `gedb:"name"`
    Email    string   `gedb:"email,omitempty"`
    Age      int      `gedb:"age,omitzero"`
    Tags     []string `gedb:"tags,omitempty"`
    Internal string   `gedb:"-"` // ignored
}

// Insert struct
db.Insert(ctx, User{Name: "Alice", Age: 30})

// Query into struct
var user User
db.FindOne(ctx, M{"name": "Alice"}, &user)
```

## Configuration Options

```go
db, _ := gedb.NewDB(
    // Persistence
    gedb.WithFilename("data.db"),
    gedb.WithInMemoryOnly(false),
    gedb.WithFileMode(0644),
    gedb.WithDirMode(0755),

    // Behavior
    gedb.WithTimestamps(true),  // auto createdAt/updatedAt
    gedb.WithCorruptionThreshold(0.1),  // fail if >10% corrupt

    // Custom implementations (for advanced use)
    gedb.WithSerializer(customSerializer),
    gedb.WithDeserializer(customDeserializer),
    gedb.WithComparer(customComparer),
    gedb.WithMatcher(customMatcher),
    // ... and more
)
```

## Benchmarks

| Operation       | 1 doc | 100 docs   | 1,000 docs | 10,000 docs |
|-----------------|-------|------------|------------|-------------|
| Insert (batch)  | 7µs   | 5.8µs/item | 6.3µs/item | 7µs/item    |
| Find (indexed)  | 9µs   | 11µs       | 18µs       | 60µs        |
| Count (indexed) | 7µs   | 25µs       | 207µs      | 2.6ms       |
| Update          | 10µs  | 29µs       | 228µs      | 2.8ms       |
| Remove          | 5µs   | 5µs        | 7µs        | 2.3ms       |

Run benchmarks yourself:

```bash
go test -bench=.
```

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

MIT License - see [LICENSE](LICENSE) for details.
