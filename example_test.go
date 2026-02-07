package gedb_test

import (
	"context"
	"fmt"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/cursor"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/index"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

func ExampleNewDB() {
	// To create a new database, [NewDB] should be called. It creates a new
	// instance of a datastore, loading default values and interface
	// implementations. If no file is provided, starts a in-memory-only db.
	db, _ := gedb.NewDB(
		// Name of the datafile. If not set, the database will be
		// created as in-memory-only. Filename cannot end with '~', as
		// that is reserved for the crash-safe backup file.
		gedb.WithFilename(""),
		// If set to true, no file will be used to store the data.
		// Instead, all operations will be in-memory-only.
		gedb.WithInMemoryOnly(true),
		// Permission to open the datafile with.
		gedb.WithFileMode(0666),
		// Permission to open the datafile directory with.
		gedb.WithDirMode(0666),
		// If set to true, new docs will be created with the fields
		// `createdAt` and `updatedAt`. If the document is modified,
		// field `updatedAt` will be updated.
		gedb.WithTimestamps(false),
		// Sets corruption threshold. If this percentage (or more ) of
		// records cannot be read when loading the database, load fails
		// and returns an error. Defaults to 0.1 (10%).
		gedb.WithCorruptionThreshold(0.1),

		// Interfaces
		//
		// These are specific interfaces that can be reimplemented by
		// the user. Every behavior in this package is controlled by
		// those interfaces, and can easily be replaced to have modified
		// or new features; or mocked to make testing easy.
		gedb.WithComparer(nil),
		gedb.WithCursorFactory(cursor.NewCursor),
		gedb.WithDecoder(nil),
		gedb.WithDeserializer(nil),
		gedb.WithDocumentFactory(nil),
		gedb.WithFieldNavigator(nil),
		gedb.WithHasher(nil),
		gedb.WithIDGenerator(nil),
		gedb.WithIndexFactory(index.NewIndex),
		gedb.WithMatcher(nil),
		gedb.WithModifier(nil),
		gedb.WithPersistence(nil),
		gedb.WithRandomReader(nil),
		gedb.WithSerializer(nil),
		gedb.WithStorage(nil),
		gedb.WithTimeGetter(nil),
	)

	// Every function in the DB receive a context argument. This should make
	// concurrently using the db possible. Context allows the user to
	// imediately stop waiting if cancellation occurs before starting the
	// action.
	ctx := context.Background()

	// The method [GEDB.LoadDatabase] should be used right after instancing
	// a new [GEDB] so it loads file content. It is not necessary for
	// in-memory-only databases, as they do not have anything to load.
	// Calling this method from a in-memory-only database should be done
	// cautiously, as it resets all indexes and leaves the db empty.
	_ = db.LoadDatabase(ctx)

	cur, _ := db.GetAllData(ctx)

	data := make([]M, 0, 10)
	for cur.Next() {
		var m M
		_ = cur.Scan(ctx, &m)
		data = append(data, m)
	}

	fmt.Printf("%v", data)
	// Output: []
}

func ExampleGEDB_Insert() {
	db, _ := gedb.NewDB()

	ctx := context.Background()

	// A struct can be defined to make working with the db easier. The
	// struct does not need to be exported, but the fields do.
	type Character struct {
		// untagged exported fields are named as they are
		Name string
		// tagged exported fields are named after the gedb tag
		Sty string `gedb:"style"`
		// unexported fields are ignored
		country string
		// fields with "-" at the gedb tag are also ignored
		Clothes string `gedb:"-"`
		// omitempty flag does not allow nil fields.
		Spells   []string `gedb:",omitempty"`
		Specials []string `gedb:",omitempty"`
		// omitzero flag does not allow zero-value fields.
		SpecialDmg []float64 `gedb:",omitzero"`
		Games      []float64 `gedb:",omitzero"`
	}

	gief := Character{
		Name:    "Zangief",
		Sty:     "grappler",
		country: "URSS",
		Clothes: "red",
		// empty lists are ignored by omitempty directive
		Spells: []string{},
		// omitempty does not affect non-empty lists
		Specials: []string{"SPD", "Siberian express"},
		// nil values are ignored by omitempty directive
		SpecialDmg: nil,
		// omitzero does not affect non-empty lists
		Games: []float64{2, 3.5, 4, 5, 6},
	}

	// The method [GEDB.Insert] can be called with any object-like document.
	// Since data is converted into [Document], an internal data type, the
	// received object is not used afterwards. Instead, a copy is made. In
	// general, maps with a string-type key and structs are accepted as
	// valid documents, but keys should not start with "$", since that is
	// reserved for querying and updating. For structs, untagged fields
	// names will keep unchanged. Fields with the tag "gedb" will be
	// renamed. Unexported fields are not included.
	cur, _ := db.Insert(ctx, gief)

	// When successfully inserted, a cursor containing all inserted data is
	// returned. If the field _id does not exist, it is created. Fields
	// containing creation and last update timestamps might be added,
	// depending on db configurations set with the option [WithTimestamps].
	// Open cursors should always be closed after use.
	defer cur.Close()

	var inserted map[string]any
	for cur.Next() {
		_ = cur.Scan(ctx, &inserted)
	}

	fmt.Println(len(inserted))
	fmt.Println(len(inserted["_id"].(string)) > 0)
	fmt.Println(inserted["Name"])
	fmt.Println(inserted["style"])
	fmt.Println(inserted["Specials"].([]any)[0])
	fmt.Println(inserted["Specials"].([]any)[1])
	fmt.Println(inserted["Games"])

	// Output:
	// 5
	// true
	// Zangief
	// grappler
	// SPD
	// Siberian express
	// [2 3.5 4 5 6]
}

func ExampleGEDB_Find() {
	db, _ := gedb.NewDB()

	ctx := context.Background()

	docs := []any{
		map[string]any{"pos": 1, "Type": "wh.mage"},
		map[string]any{"pos": 2, "Type": "bl.mage"},
		map[string]any{"pos": 3, "Type": "fighter"},
		map[string]any{"pos": 4, "Type": "rogue"},
	}

	_, _ = db.Insert(ctx, docs...)

	// Method [GEDB.Find] uses a mongodb-like api to fetch data from the db.
	// Maps and structs can be used to shape the query as you like.
	cur, _ := db.Find(ctx,
		M{"pos": M{"$lte": 3}},
		gedb.WithSort(gedb.Sort{{Key: "Type", Order: 1}}),
		gedb.WithProjection(M{"_id": 0, "Type": 1}),
		gedb.WithSkip(1),
		gedb.WithLimit(2),
	)

	types := make([]M, 0, 2)
	for cur.Next() {
		var m M
		_ = cur.Scan(ctx, &m)
		types = append(types, m)
	}

	fmt.Printf("%v", types)
	// Output: [map[Type:fighter] map[Type:wh.mage]]
}

func ExampleGEDB_Update() {
	db, _ := gedb.NewDB()

	ctx := context.Background()

	docs := []any{
		M{"date": "yesterday"},
	}

	_, _ = db.Insert(ctx, docs...)

	_, _ = db.Update(ctx,
		M{"date": "yesterday"},
		M{"date": "today"},

		gedb.WithUpdateMulti(true),
		gedb.WithUpsert(false),
	)

	var m M
	_ = db.FindOne(ctx, nil, &m, domain.WithProjection(M{"_id": 0}))

	fmt.Printf("%v", m)
	// Output: map[date:today]
}

func ExampleGEDB_Remove() {
	db, _ := gedb.NewDB()

	ctx := context.Background()

	docs := []any{
		M{"_id": 1, "valid": true},
		M{"_id": 2, "valid": true},
		M{"_id": 3, "valid": false},
		M{"_id": 4, "valid": true},
		M{"_id": 5, "valid": false},
	}

	_, _ = db.Insert(ctx, docs...)

	_, _ = db.Remove(ctx, M{"valid": false}, gedb.WithRemoveMulti(true))

	cur, _ := db.Find(ctx, nil, domain.WithProjection(M{"valid": 0}))

	data := make([]M, 0, 4)
	for cur.Next() {
		var m M
		_ = cur.Scan(ctx, &m)
		data = append(data, m)
	}

	fmt.Printf("%v", data)
	// Output: [map[_id:1] map[_id:2] map[_id:4]]
}

func ExampleGEDB_EnsureIndex() {
	db, _ := gedb.NewDB()

	ctx := context.Background()

	err := db.EnsureIndex(ctx,
		gedb.WithFields("field"),
		gedb.WithUnique(true),
		gedb.WithSparse(true),
		gedb.WithTTL(0*time.Second),
	)

	fmt.Println(err)
	// Output: <nil>
}
