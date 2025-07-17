package lib

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/errs"
)

// I don't like this approach, but go test uses package dir as path, and I want
// to keep the workspace dir where it is in original code.
const testDb = "../workspace/test.db"

var p *Persistence

type testStorage struct {
	DefaultStorage
}

func (ts testStorage) EnsureFileDoesntExist(filename string) error {
	exists, err := ts.Exists(filename)
	if err != nil || !exists {
		return err
	}
	return os.Remove(filename)
}

// NOTE: The original persistence test file depends on the Datastore
// implementation, which is not done just yet. Since this implementation
// is not the same as the original, as there is no interdependence between those
// types and only Datatastore should depend on Persistence, I'm implementing a
// test suite without Datastore. ProcessRawData is being removed and tests
// including this function are not being added.
type PersistenceTestSuite struct {
	suite.Suite
	storage    testStorage
	serializer gedb.Serializer
	comparer   gedb.Comparer
}

func (s *PersistenceTestSuite) SetupTest() {

	s.comparer = NewComparer()

	s.serializer = NewSerializer(s.comparer)
	if err := s.storage.EnsureParentDirectoryExists(testDb, DefaultDirMode); err != nil {
		s.FailNow("could not ensure parent directory", err)
	}

	exists, err := s.storage.Exists(testDb)
	if err != nil {
		s.FailNow("could not check datafile")
	}

	if exists {
		if err := os.Remove(testDb); err != nil {
			s.FailNow("could not remove datafile")
		}
	}

	if err := s.storage.EnsureDatafileIntegrity(testDb, DefaultFileMode); err != nil {
		s.FailNow("could not ensure datafile integrity", err)
	}

	per, err := NewPersistence(gedb.PersistenceOptions{Filename: testDb})
	s.NoError(err)

	s.Equal(testDb, per.filename)

	p = per
}

// Every line represents a document (with stream)
func (s *PersistenceTestSuite) TestEveryLineIsADocStream() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()
	rawData1, err1 := s.serializer.Serialize(ctx, Document{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, Document{"_id": "2", "hello": "world"})
	rawData3, err3 := s.serializer.Serialize(ctx, Document{"_id": "3", "nested": Document{"today": now}})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))

	treatedData, _, err := p.TreadRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)
	slices.SortFunc(treatedData, func(a, b gedb.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 3)
	s.Equal(Document{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(Document{"_id": "2", "hello": "world"}, treatedData[1])
	s.Equal(Document{"_id": "3", "nested": Document{"today": now}}, treatedData[2])
}

// Badly formatted lines have no impact on the treated data (with stream)
func (s *PersistenceTestSuite) TestBadlyFormatedLinesStream() {
	p.SetCorruptAlertThreshold(1) // to prevent a corruption alert

	ctx := context.Background()

	now := float64((time.Time{}).Unix())
	rawData1, err1 := s.serializer.Serialize(ctx, Document{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, Document{"_id": "3", "nested": Document{"today": now}})
	s.NoError(err1)
	s.NoError(err2)
	rawData := []byte(string(rawData1) + "\n" + "garbage" + "\n" + string(rawData2))
	treatedData, _, err := p.TreadRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)

	slices.SortFunc(treatedData, func(a, b gedb.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(Document{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(Document{"_id": "3", "nested": Document{"today": now}}, treatedData[1])
}

// Well formatted lines that have no _id are not included in the data (with stream)
func (s *PersistenceTestSuite) TestWellFormatedNoIDStream() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()

	rawData1, err1 := s.serializer.Serialize(ctx, Document{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, Document{"_id": "2", "hello": "world"})
	rawData3, err3 := s.serializer.Serialize(ctx, Document{"nested": Document{"today": now}})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))
	treatedData, _, err := p.TreadRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)

	slices.SortFunc(treatedData, func(a, b gedb.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(Document{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(Document{"_id": "2", "hello": "world"}, treatedData[1])
}

// If two lines concern the same doc (= same _id), the last one is the good version (with stream)
func (s *PersistenceTestSuite) TestRepeatedID() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()

	rawData1, err1 := s.serializer.Serialize(ctx, Document{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, Document{"_id": "2", "hello": "world"})
	rawData3, err3 := s.serializer.Serialize(ctx, Document{"_id": "1", "nested": Document{"today": now}})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))
	treatedData, _, err := p.TreadRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)
	_ = treatedData

	slices.SortFunc(treatedData, func(a, b gedb.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(Document{"_id": "1", "nested": Document{"today": now}}, treatedData[0])
	s.Equal(Document{"_id": "2", "hello": "world"}, treatedData[1])
}

// If a doc contains $$deleted: true, that means we need to remove it from the data (with stream)
func (s *PersistenceTestSuite) TestDeleteDoc() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()
	rawData1, err1 := s.serializer.Serialize(ctx, Document{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, Document{"_id": "2", "hello": "world"})
	rawData3, err3 := s.serializer.Serialize(ctx, Document{"_id": "1", "$$deleted": true})
	rawData4, err4 := s.serializer.Serialize(ctx, Document{"_id": "3", "today": now})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	s.NoError(err4)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3) + "\n" + string(rawData4))

	treatedData, _, err := p.TreadRawStream(ctx, bytes.NewReader(rawData))
	slices.SortFunc(treatedData, func(a, b gedb.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.NoError(err)
	s.Len(treatedData, 2)
	s.Equal(Document{"_id": "2", "hello": "world"}, treatedData[0])
	s.Equal(Document{"_id": "3", "today": now}, treatedData[1])
}

// If a doc contains $$deleted: true, no error is thrown if the doc wasnt in the []any before (with stream)
func (s *PersistenceTestSuite) TestDeleteUnexistentDoc() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()
	rawData1, err1 := s.serializer.Serialize(ctx, Document{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, Document{"_id": "2", "$$deleted": true})
	rawData3, err3 := s.serializer.Serialize(ctx, Document{"_id": "3", "today": now})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))

	treatedData, _, err := p.TreadRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)
	slices.SortFunc(treatedData, func(a, b gedb.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(Document{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(Document{"_id": "3", "today": now}, treatedData[1])
}

// If a doc contains $$indexCreated, no error is thrown during treatRawData and we can get the index options (with stream)
func (s *PersistenceTestSuite) TestIndexCreated() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()
	rawData1, err1 := s.serializer.Serialize(ctx, Document{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, Document{"$$indexCreated": Document{"fieldName": "test", "unique": true}})
	rawData3, err3 := s.serializer.Serialize(ctx, Document{"_id": "3", "today": now})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))

	treatedData, indexes, err := p.TreadRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)
	s.Len(indexes, 1)
	s.Equal(gedb.IndexDTO{IndexCreated: gedb.IndexCreated{FieldName: "test", Unique: true}}, indexes["test"])

	slices.SortFunc(treatedData, func(a, b gedb.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(Document{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(Document{"_id": "3", "today": now}, treatedData[1])
}

// Compact database on load
func (s *PersistenceTestSuite) TestCompactOnLoad() {
	now := time.Now().Unix()

	id1 := uuid.New()

	docs := []gedb.Document{
		Document{"_id": id1, "a": 2, "createdAt": now, "updatedAt": now},
		Document{"_id": uuid.New().String(), "a": 4, "createdAt": now, "updatedAt": now},
		Document{"_id": id1, "a": 2, "$$deleted": true},
	}
	ctx := context.Background()
	s.NoError(p.PersistNewState(ctx, docs...))
	f, err := os.ReadFile(p.filename)
	s.NoError(err)
	data := strings.Split(string(f), "\n")
	filledCount := 0

	for _, item := range data {
		if len(item) > 0 {
			filledCount++
		}
	}

	s.Equal(3, filledCount)
	// the original Persistence.PersistCachedDatabaseAsync calls
	// this.db.getAllData. That was removed, data has to be passed instead.
	_, _, err = p.LoadDatabase(ctx)
	s.NoError(err)
	f, err = os.ReadFile(p.filename)
	s.NoError(err)
	data = strings.Split(string(f), "\n")
	filledCount = 0

	for _, item := range data {
		if len(item) > 0 {
			filledCount++
		}
	}
	s.Equal(1, filledCount)
}

// NOTE: Missing some tests that would not be valid without a datastore:
// 'Calling loadDatabase after the data was modified doesnt change its contents'
// 'Calling loadDatabase after the datafile was removed will reset the database'
// 'Calling loadDatabase after the datafile was modified loads the new data'

// Calling loadDatabase after the datafile was removed will reset the database
//
// TODO: Reimplement with datastore (not testable as original version)
func (s *PersistenceTestSuite) TestCallAfterRemovingDatafile() {
	d1 := Document{"_id": uuid.New().String(), "a": 1}
	d2 := Document{"_id": uuid.New().String(), "a": 2}
	d := [][]gedb.Document{
		{d1},
		{d1, d2},
	}

	ctx := context.Background()
	_, _, err := p.LoadDatabase(ctx)
	s.NoError(err)

	s.NoError(p.PersistNewState(ctx, d[0]...))
	s.NoError(p.PersistNewState(ctx, d[1]...))

	s.NoError(os.Remove(testDb))

	allData, _, err := p.LoadDatabase(ctx)
	s.NoError(err)

	s.Len(allData, 0)
}

// When treating raw data, refuse to proceed if too much data is corrupt, to avoid data loss
func (s *PersistenceTestSuite) TestRefuseIfTooMuchIsCorrup() {
	const corruptTestFileName = "../workspace/corruptTest.db"
	fakeData := "{\"_id\":\"one\",\"hello\":\"world\"}\n" + "Some corrupt data\n" + "{\"_id\":\"two\",\"hello\":\"earth\"}\n" + "{\"_id\":\"three\",\"hello\":\"you\"}\n"
	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))

	var err error
	p, err = NewPersistence(gedb.PersistenceOptions{Filename: corruptTestFileName, CorruptAlertThreshold: -1}) // defaults to 0.1 when negative
	s.NoError(err)

	ctx := context.Background()
	_, _, err = p.LoadDatabase(ctx)
	e := &errs.ErrCorruptFiles{}
	s.ErrorAs(err, e)
	s.Equal(0.25, e.CorruptionRate)
	s.Equal(1, e.CorruptItems)
	s.Equal(4, e.DataLength)

	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))
	p, err = NewPersistence(gedb.PersistenceOptions{Filename: corruptTestFileName, CorruptAlertThreshold: 1})
	s.NoError(err)

	ctx = context.Background()
	_, _, err = p.LoadDatabase(ctx)
	s.NoError(err, e)

	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))
	p, err = NewPersistence(gedb.PersistenceOptions{Filename: corruptTestFileName})
	s.NoError(err)

	ctx = context.Background()
	_, _, err = p.LoadDatabase(ctx)
	s.ErrorAs(err, e)
	s.Equal(0.25, e.CorruptionRate)
	s.Equal(1, e.CorruptItems)
	s.Equal(4, e.DataLength)
}

// Can []anyen to compaction events
func (s *PersistenceTestSuite) TestListenEvent() {
	done := make(chan struct{})
	ctx := context.Background()
	go func() {
		s.NoError(p.WaitCompaction(ctx))
		close(done)
	}()
	// Persistence.compactDatafile is deprecated and was not added
	s.NoError(p.PersistCachedDatabase(ctx, nil, nil))
	<-done
}

func (s *PersistenceTestSuite) TestSetializers() {
	//	describe('Serialization hooks', function () {
	//	  const se = function (s) { return 'before_' + s + '_after' }
	//	  const bd = function (s) { return s.substring(7, s.length - 6) }
	//
	se := gedb.SerializeFunc(func(ctx context.Context, v any) ([]byte, error) {
		s, err := s.serializer.Serialize(ctx, v)
		if err != nil {
			return nil, err
		}
		return []byte("before_" + string(s) + "_after"), nil
	})
	de := gedb.DeserializeFunc(func(ctx context.Context, b []byte, v any) error {
		return NewDeserializer(NewDecoder()).Deserialize(ctx, b[7:len(b)-6], v)
	})

	// Declare only one hook will not cause an error
	//
	// NOTE: The original code would throw an error when declaring only one
	// hook, but this check was not added to this code.
	s.Run("DeclareEitherSerializerOrDeserializer", func() {
		const hookTestFilename = "../workspace/hookTest.db"
		s.NoError(s.storage.EnsureFileDoesntExist(hookTestFilename))
		s.NoError(os.WriteFile(hookTestFilename, []byte("Some content"), 0666))

		_, err := NewPersistence(gedb.PersistenceOptions{
			Filename:   hookTestFilename,
			Serializer: se,
		})
		s.NoError(err) // original would throw

		b, err := os.ReadFile(hookTestFilename)
		s.NoError(err)
		s.Equal("Some content", string(b)) // Data file left untouched

		_, err = NewPersistence(gedb.PersistenceOptions{
			Filename:     hookTestFilename,
			Deserializer: de,
		})
		s.NoError(err) // original would throw

		b, err = os.ReadFile(hookTestFilename)
		s.NoError(err)
		s.Equal("Some content", string(b)) // Data file left untouched

		_, err = NewPersistence(gedb.PersistenceOptions{
			Filename:     hookTestFilename,
			Serializer:   se,
			Deserializer: de,
		})
		s.NoError(err)

		b, err = os.ReadFile(hookTestFilename)
		s.NoError(err)
		s.Equal("Some content", string(b)) // Data file left untouched
	})

	s.Run("UseSerializerWhenPersistingOrCompacting", func() {
		const hookTestFilename = "../workspace/hookTest.db"
		s.NoError(s.storage.EnsureFileDoesntExist(hookTestFilename))
		var err error
		p, err = NewPersistence(gedb.PersistenceOptions{
			Filename:     hookTestFilename,
			Serializer:   se,
			Deserializer: de,
		})
		s.NoError(err)

		id1 := uuid.New().String()

		ctx := context.Background()
		s.NoError(p.PersistNewState(ctx, Document{"_id": id1, "hello": "world"}))
		s.NoError(p.PersistNewState(ctx, Document{"_id": id1, "hello": "earth"}))
		s.NoError(p.PersistNewState(ctx, Document{"$$indexCreated": Document{"fieldName": "idefix"}}))

		_data, err := os.ReadFile(hookTestFilename)
		s.NoError(err)

		data := bytes.Split(_data, []byte("\n"))

		var doc0, doc1 Document
		var idx gedb.IndexDTO

		s.Len(data, 4)

		s.NoError(de(ctx, data[0], &doc0))
		s.Len(doc0, 2)
		s.Equal("world", doc0["hello"])

		s.NoError(de(ctx, data[1], &doc1))
		s.Len(doc1, 2)
		s.Equal("earth", doc1["hello"])

		s.NoError(de(ctx, data[2], &idx))
		s.Equal(gedb.IndexDTO{IndexCreated: gedb.IndexCreated{FieldName: "idefix"}}, idx)

		allData := []gedb.Document{
			Document{"_id": id1, "hello": "earth"},
		}
		s.NoError(p.PersistCachedDatabase(ctx, allData, nil))
	})

	s.Run("LoadData", func() {
		const hookTestFilename = "../workspace/hookTest.db"
		s.NoError(s.storage.EnsureFileDoesntExist(hookTestFilename))
		var err error
		p, err = NewPersistence(gedb.PersistenceOptions{
			Filename:     hookTestFilename,
			Serializer:   se,
			Deserializer: de,
		})
		s.NoError(err)
		ctx := context.Background()
		_id := uuid.New().String()
		id2 := uuid.New().String()
		s.NoError(p.PersistNewState(ctx, Document{"_id": _id, "hello": "world"}))
		s.NoError(p.PersistNewState(ctx, Document{"_id": id2, "ya": "ya"}))
		s.NoError(p.PersistNewState(ctx, Document{"_id": _id, "hello": "earth"}))
		s.NoError(p.PersistNewState(ctx, Document{"_id": id2, "$$deleted": true}))
		s.NoError(p.PersistNewState(ctx, Document{"$$indexCreated": Document{"fieldName": "idefix"}}))
		_data, err := os.ReadFile(hookTestFilename)
		s.NoError(err)
		data := bytes.Split(_data, []byte("\n"))
		s.Len(data, 6)

		// Everything is deserialized correctly, including deletes and indexes
		p, err = NewPersistence(gedb.PersistenceOptions{
			Filename:     hookTestFilename,
			Serializer:   se,
			Deserializer: de,
		})
		docs, indexes, err := p.LoadDatabase(ctx)
		s.Len(docs, 1)
		s.Equal("earth", docs[0].Get("hello"))
		s.Equal(_id, docs[0].Get("_id"))
		s.Len(indexes, 1) // Original is one, but we're not using datastore
		s.Contains(indexes, "idefix")
	})
} // ==== End of 'Serialization hooks' ==== //

func (s *PersistenceTestSuite) TestPreventDataloss() {
	// Creating a datastore with in memory as true and a bad filename wont cause an error
	s.Run("InMemoryBadFilenameNoError", func() {
		_, err := NewPersistence(gedb.PersistenceOptions{Filename: "../workspace/bad.db~", InMemoryOnly: true})
		s.NoError(err)
	})

	// Creating a persistent datastore with a bad filename will cause an error
	s.Run("PersistentBadFilenameError", func() {
		_, err := NewPersistence(gedb.PersistenceOptions{Filename: "../workspace/bad.db~"})
		s.Error(err)
	})

	// If no file stat, ensureDatafileIntegrity creates an empty datafile
	s.Run("CreateEmptyFileIfNoFileStat", func() {
		p, err := NewPersistence(gedb.PersistenceOptions{InMemoryOnly: false, Filename: "../workspace/it.db"})
		s.NoError(err)

		fileExists, err := s.storage.Exists("../workspace/it.db")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/it.db"))
		}
		fileExists, err = s.storage.Exists("../workspace/it.db~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/it.db~"))
		}

		s.NoFileExists("../workspace/it.db")
		s.NoFileExists("../workspace/it.db~")

		s.NoError(s.storage.EnsureDatafileIntegrity(p.filename, 0666))

		s.FileExists("../workspace/it.db")
		s.NoFileExists("../workspace/it.db~")

		b, err := os.ReadFile("../workspace/it.db")
		s.NoError(err)
		s.Len(b, 0)

	})

	// If only datafile stat, ensureDatafileIntegrity will use it
	s.Run("UseDatafileIfExists", func() {
		p, err := NewPersistence(gedb.PersistenceOptions{InMemoryOnly: false, Filename: "../workspace/it.db"})
		s.NoError(err)

		fileExists, err := s.storage.Exists("../workspace/it.db")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/it.db"))
		}
		fileExists, err = s.storage.Exists("../workspace/it.db~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/it.db~"))
		}

		s.NoError(os.WriteFile("../workspace/it.db", []byte("something"), 0666))

		s.FileExists("../workspace/it.db")
		s.NoFileExists("../workspace/it.db~")

		s.NoError(s.storage.EnsureDatafileIntegrity(p.filename, 0666))

		s.FileExists("../workspace/it.db")
		s.NoFileExists("../workspace/it.db~")

		b, err := os.ReadFile("../workspace/it.db")
		s.NoError(err)
		s.Equal("something", string(b))
	})

	// If temp datafile stat and datafile doesnt, ensureDatafileIntegrity will use it (cannot happen except upon first use)
	s.Run("UseTempDatafileIfExistsUponFirstUse", func() {
		p, err := NewPersistence(gedb.PersistenceOptions{InMemoryOnly: false, Filename: "../workspace/it.db"})
		s.NoError(err)

		fileExists, err := s.storage.Exists("../workspace/it.db")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/it.db"))
		}
		fileExists, err = s.storage.Exists("../workspace/it.db~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/it.db~"))
		}

		s.NoError(os.WriteFile("../workspace/it.db~", []byte("something"), 0666))

		s.NoFileExists("../workspace/it.db")
		s.FileExists("../workspace/it.db~")

		s.NoError(s.storage.EnsureDatafileIntegrity(p.filename, 0666))

		s.FileExists("../workspace/it.db")
		s.NoFileExists("../workspace/it.db~")

		b, err := os.ReadFile("../workspace/it.db")
		s.NoError(err)
		s.Equal("something", string(b))
	})

	// If both temp and current datafiles exist, ensureDatafileIntegrity
	// will use the datafile, as it means that the write of the temp file
	// failed
	//
	// Technically it could also mean the write was successful but the
	// rename wasn't, but there is in any case no guarantee that the data in
	// the temp file is whole so we have to discard the whole file
	s.Run("UseDatafileIfBothExist", func() {
		p, err := NewPersistence(gedb.PersistenceOptions{Filename: "../workspace/it.db"})
		s.NoError(err)

		fileExists, err := s.storage.Exists("../workspace/it.db")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/it.db"))
		}
		fileExists, err = s.storage.Exists("../workspace/it.db~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/it.db~"))
		}

		s.NoError(os.WriteFile("../workspace/it.db", []byte("{\"_id\":\"0\",\"hello\":\"world\"}"), 0666))
		s.NoError(os.WriteFile("../workspace/it.db~", []byte("{\"_id\":\"0\",\"hello\":\"other\"}"), 0666))

		s.FileExists("../workspace/it.db")
		s.FileExists("../workspace/it.db~")

		s.NoError(s.storage.EnsureDatafileIntegrity(p.filename, 0666))

		s.FileExists("../workspace/it.db")
		s.FileExists("../workspace/it.db~")

		b, err := os.ReadFile("../workspace/it.db")
		s.NoError(err)
		s.Equal("{\"_id\":\"0\",\"hello\":\"world\"}", string(b))
		b, err = os.ReadFile("../workspace/it.db~")
		s.NoError(err)
		s.Equal("{\"_id\":\"0\",\"hello\":\"other\"}", string(b))

		ctx := context.Background()

		docs, _, err := p.LoadDatabase(ctx)
		s.NoError(err)

		s.Len(docs, 1)
		s.Equal("world", docs[0].Get("hello"))

		s.FileExists("../workspace/it.db")
		s.NoFileExists("../workspace/it.db~")
	})

	// persistCachedDatabase should update the contents of the datafile and leave a clean state
	s.Run("CleanDatafile", func() {
		ctx := context.Background()
		_id := uuid.New().String()
		s.NoError(p.PersistNewState(ctx, Document{"_id": _id, "hello": "world"}))

		fileExists, err := s.storage.Exists(testDb)
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(testDb))
		}
		fileExists, err = s.storage.Exists(testDb + "~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(testDb + "~"))
		}
		s.NoFileExists(testDb)

		s.NoError(os.WriteFile(testDb+"~", []byte("something"), 0666))
		s.FileExists(testDb + "~")

		s.NoError(p.PersistCachedDatabase(ctx, []gedb.Document{Document{"_id": _id, "hello": "world"}}, nil))
		contents, err := os.ReadFile(testDb)
		s.NoError(err)

		fileExists, err = s.storage.Exists(testDb)
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(testDb))
		}
		s.True(fileExists)
		fileExists, err = s.storage.Exists(testDb + "~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(testDb + "~"))
		}

		s.False(fileExists)

		d := make(Document)
		s.NoError(json.NewDecoder(bytes.NewReader(contents)).Decode(&d))
		s.Len(d, 2)
		s.Equal("world", d["hello"])
		s.Regexp(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`, d["_id"])
	})

	// After a persistCachedDatabase, there should be no temp or old filename
	s.Run("NoTempFileAfterPersistCachedDatabase", func() {
		ctx := context.Background()
		_id := uuid.New().String()
		s.NoError(p.PersistNewState(ctx, Document{"_id": _id, "hello": "world"}))

		fileExists, err := s.storage.Exists(testDb)
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(testDb))
		}
		fileExists, err = s.storage.Exists(testDb + "~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(testDb + "~"))
		}

		s.NoFileExists(testDb)
		s.NoFileExists(testDb + "~")

		s.NoError(os.WriteFile(testDb+"~", []byte("bloup"), 0666))
		s.FileExists(testDb + "~")
		s.NoError(p.PersistCachedDatabase(ctx, []gedb.Document{Document{"_id": _id, "hello": "world"}}, nil))
		contents, err := os.ReadFile(testDb)
		s.NoError(err)
		s.FileExists(testDb)
		s.NoFileExists(testDb + "~")
		d := make(Document)
		s.NoError(json.NewDecoder(bytes.NewReader(contents)).Decode(&d))
		s.Len(d, 2)
		s.Equal("world", d["hello"])
		s.Regexp(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`, d["_id"])
	})

	// persistCachedDatabase should update the contents of the datafile and leave a clean state even if there is a temp datafile
	s.Run("CleanDatafileIfTempFileExists", func() {
		ctx := context.Background()
		_id := uuid.New().String()
		s.NoError(p.PersistNewState(ctx, Document{"_id": _id, "hello": "world"}))

		fileExists, err := s.storage.Exists(testDb)
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(testDb))
		}
		s.NoError(os.WriteFile(testDb+"~", []byte("blabla"), 0666))
		s.NoFileExists(testDb)
		s.FileExists(testDb + "~")

		s.NoError(p.PersistCachedDatabase(ctx, []gedb.Document{Document{"_id": _id, "hello": "world"}}, nil))
		contents, err := os.ReadFile(testDb)
		s.FileExists(testDb)
		s.NoFileExists(testDb + "~")
		d := make(Document)
		s.NoError(json.NewDecoder(bytes.NewReader(contents)).Decode(&d))
		s.Len(d, 2)
		s.Equal("world", d["hello"])
		s.Regexp(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`, d["_id"])
	})

	// persistCachedDatabase should update the contents of the datafile and leave a clean state even if there is a temp datafile
	s.Run("CleanDatafileIfEmptyTempFileEists", func() {
		const dbFile = "../workspace/test2.db"

		fileExists, err := s.storage.Exists(dbFile)
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(dbFile))
		}
		fileExists, err = s.storage.Exists(dbFile + "~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove(dbFile + "~"))
		}

		p, err := NewPersistence(gedb.PersistenceOptions{Filename: dbFile})
		s.NoError(err)

		ctx := context.Background()
		_, _, err = p.LoadDatabase(ctx)
		s.NoError(err)
		contents, err := os.ReadFile(dbFile)
		s.NoError(err)
		s.FileExists(dbFile)
		s.NoFileExists(dbFile + "~")
		s.Len(contents, 0)
	})

	// Persistence works as expected when everything goes fine
	s.Run("WorkAsExpected", func() {
		const dbFile = "../workspace/test2.db"

		s.NoError(s.storage.EnsureFileDoesntExist(dbFile))
		s.NoError(s.storage.EnsureFileDoesntExist(dbFile + "~"))

		p, err := NewPersistence(gedb.PersistenceOptions{Filename: dbFile})
		s.NoError(err)

		ctx := context.Background()
		docs, _, err := p.LoadDatabase(ctx)
		s.NoError(err)
		s.Len(docs, 0)

		doc1 := Document{"_id": uuid.New().String(), "a": "hello"}
		doc2 := Document{"_id": uuid.New().String(), "a": "world"}

		s.NoError(p.PersistNewState(ctx, doc1, doc2))

		docs, _, err = p.LoadDatabase(ctx)
		s.NoError(err)
		s.Len(docs, 2)
		s.Equal("hello", docs[slices.IndexFunc(docs, func(a gedb.Document) bool { return a.ID() == doc1.ID() })].Get("a"))
		s.Equal("world", docs[slices.IndexFunc(docs, func(a gedb.Document) bool { return a.ID() == doc2.ID() })].Get("a"))

		s.FileExists(dbFile)
		s.NoFileExists(dbFile + "~")

		p2, err := NewPersistence(gedb.PersistenceOptions{Filename: dbFile})
		s.NoError(err)
		docs, _, err = p2.LoadDatabase(ctx)
		s.NoError(err)
		s.Len(docs, 2)
		s.Equal("hello", docs[slices.IndexFunc(docs, func(a gedb.Document) bool { return a.ID() == doc1.ID() })].Get("a"))
		s.Equal("world", docs[slices.IndexFunc(docs, func(a gedb.Document) bool { return a.ID() == doc2.ID() })].Get("a"))

		s.FileExists(dbFile)
		s.NoFileExists(dbFile + "~")
	})

	s.Run("KeepOldVersionOnCrash", func() {
		const N = 500
		toWrite := new(bytes.Buffer)
		i := 0
		// let docI

		// Ensuring the state is clean
		fileExists, err := s.storage.Exists("../workspace/lac.db")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/lac.db"))
		}
		fileExists, err = s.storage.Exists("../workspace/lac.db~")
		s.NoError(err)
		if fileExists {
			s.NoError(os.Remove("../workspace/lac.db~"))
		}

		// Creating a db file with 150k records (a bit long to load)
		encoder := json.NewEncoder(toWrite)
		for i = range N {
			s.NoError(encoder.Encode(Document{"_id": fmt.Sprintf("anid_%d", i), "hello": "world"}))
		}
		s.NoError(os.WriteFile("../workspace/lac.db", toWrite.Bytes(), 0666))

		datafile, err := os.ReadFile("../workspace/lac.db")
		s.NoError(err)
		datafileLength := len(datafile)

		s.Greater(datafileLength, 5000)

		// Loading it in a separate process that we will crash before finishing the loadDatabase
		s.Run("loadAndCrash", func() {
			// dont really like this approach, but testing by
			// running a main package
			cmd := exec.Command("go", "run", "../test_lac/")
			err := cmd.Run()
			e := &exec.ExitError{}
			s.ErrorAs(err, &e)
			status := e.Sys().(syscall.WaitStatus).ExitStatus()
			s.Equal(1, status)
			s.FileExists("../workspace/lac.db")
			s.FileExists("../workspace/lac.db~")
			f, err := os.ReadFile("../workspace/lac.db")
			s.NoError(err)
			s.Len(f, datafileLength)
			f, err = os.ReadFile("../workspace/lac.db~")
			s.NoError(err)
			s.Len(f, 5000)

			per, err := NewPersistence(gedb.PersistenceOptions{Filename: "../workspace/lac.db"})
			s.NoError(err)

			ctx := context.Background()
			docs, _, err := per.LoadDatabase(ctx)
			s.NoError(err)
			s.FileExists("../workspace/lac.db")
			s.NoFileExists("../workspace/lac.db~")

			f, err = os.ReadFile("../workspace/lac.db")
			s.NoError(err)
			s.Len(f, datafileLength)

			slices.SortFunc(docs, func(a, b gedb.Document) int {
				idA, idB := a.ID(), b.ID()
				if len(idA) != len(idB) {
					return cmp.Compare(len(idA), len(idB))
				}
				return cmp.Compare(a.ID(), b.ID())
			})

			s.Len(docs, N)
			for i, doc := range docs {
				s.Equal(Document{"_id": fmt.Sprintf("anid_%d", i), "hello": "world"}, doc)
			}
		})
	})

	// Cannot cause EMFILE errors by opening too many file descriptors
	//
	// Not run on Windows as there is no clean way to set maximum file
	// descriptors. Not an issue as the code itself is tested.
	s.Run("Cannot cause EMFILE errors by opening too many file descriptors", func() {
		ctx, cancel := context.WithTimeout(s.T().Context(), 5000*time.Millisecond)
		defer cancel()

		if runtime.GOOS == "windows" {
			return
		}

		// not creating another file
		s.Run("openFdsLaunch", func() {
			N := 64

			var originalRLimit syscall.Rlimit
			syscall.Getrlimit(syscall.RLIMIT_NOFILE, &originalRLimit)

			rLimit := syscall.Rlimit{
				Cur: 128,
				Max: originalRLimit.Max,
			}
			s.NoError(syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit))
			defer func() {
				s.NoError(syscall.Setrlimit(syscall.RLIMIT_NOFILE, &originalRLimit))
			}()
			var filehandles []*os.File
			var err error
			for range N * 2 {
				var filehandle *os.File
				filehandle, err = os.OpenFile("../test_lac/openFdsTestFile", os.O_RDONLY|os.O_CREATE, 0666)
				if err != nil {
					break
				}
				filehandles = append(filehandles, filehandle)
			}

			s.ErrorIs(err, syscall.EMFILE)
			for _, fh := range filehandles {
				fh.Close()
			}
			filehandles = filehandles[:0]

			for range N {
				var filehandle *os.File
				filehandle, err = os.OpenFile("../test_lac/openFdsTestFile2", os.O_RDONLY|os.O_CREATE, 0666)
				if err != nil {
					break
				}
				filehandles = append(filehandles, filehandle)
			}
			s.NoError(err)
			for _, fh := range filehandles {
				fh.Close()
			}
			p, err := NewPersistence(gedb.PersistenceOptions{Filename: "../workspace/openfds.db"})
			s.NoError(err)
			docs, _, err := p.LoadDatabase(ctx)
			s.NoError(err)

			removed := make([]gedb.Document, len(docs))
			for n, doc := range docs {
				removed[n] = Document{"_id": doc.ID()}
			}
			s.NoError(p.PersistNewState(ctx, removed...))
			s.NoError(p.PersistNewState(ctx, Document{"_id": uuid.New().String(), "hello": "world"}))
			for range N * 2 {
				if err = p.PersistCachedDatabase(ctx, docs, nil); err != nil {
					break
				}
			}
			s.NoError(err)
		})

		select {
		case <-ctx.Done():
			s.Fail(ctx.Err().Error())
		default:
		}
	})
} // ==== End of 'Prevent dataloss when persisting data' ====

// NOTE: Most part of the this original test suite are Datastore related, not
// persistence. Im just adding a few tests so this is not empty
//
// Original tests:
//
// deletes data in memory
// deletes data in memory & on disk
// check that executor is drained before drop
// check that autocompaction is stopped
// check that we can reload and insert afterwards (added)
// check that we can dropDatatabase if the file is already deleted (added)
// Check that TTL indexes are reset
// Check that the buffer is reset (added)
func (s *PersistenceTestSuite) TestDropDatabase() {
	s.Run("RemovesFile", func() {
		s.FileExists(testDb)
		ctx := context.Background()
		s.NoError(p.DropDatabase(ctx))
		s.NoFileExists(testDb)
	})
	s.SetupTest() // datafile is needed for the next test
	s.Run("ReloadAfterwards", func() {
		s.FileExists(testDb)
		ctx := context.Background()
		s.NoError(p.PersistNewState(ctx, Document{"_id": uuid.New().String(), "hello": "world"}))
		s.FileExists(testDb)
		b, err := os.ReadFile(testDb)
		s.NoError(err)
		var lines [][]byte
		for line := range bytes.SplitSeq(b, []byte("\n")) {
			if len(line) > 0 {
				lines = append(lines, line)
			}
		}
		s.Len(lines, 1)
		s.NoError(p.DropDatabase(ctx))
		s.NoFileExists(testDb)
		s.NoError(p.PersistNewState(ctx, Document{"_id": uuid.New().String(), "hello": "world"}))
		s.FileExists(testDb)
		b, err = os.ReadFile(testDb)
		s.NoError(err)
		lines = lines[:0]
		for line := range bytes.SplitSeq(b, []byte("\n")) {
			if len(line) > 0 {
				lines = append(lines, line)
			}
		}
		s.Len(lines, 1)
	})
	s.SetupTest()
	s.Run("CanDropMultipleTimes", func() {
		s.FileExists(testDb)
		ctx := context.Background()
		s.NoError(p.DropDatabase(ctx))
		s.NoFileExists(testDb)
		s.NoError(p.DropDatabase(ctx))
		s.NoFileExists(testDb)
	})
	s.SetupTest()
	s.Run("ReseBuffer", func() {
		ctx := context.Background()
		s.NoError(p.DropDatabase(ctx))
		docs := []gedb.Document{
			Document{"_id": uuid.New().String(), "hello": "world"},
			Document{"_id": uuid.New().String(), "hello": "world"},
			Document{"_id": uuid.New().String(), "hello": "world"},
		}
		s.NoError(p.PersistCachedDatabase(ctx, docs, nil))
		s.NoError(p.DropDatabase(ctx))
		s.NoError(p.PersistNewState(ctx, Document{"_id": uuid.New().String(), "hi": "world"}))
		docs, _, err := p.LoadDatabase(ctx)
		s.NoError(err)
		s.Len(docs, 1)
	})
}

func (s *PersistenceTestSuite) compareThings(a any, b any) int {
	comp, _ := s.comparer.Compare(a, b)
	return comp
}

func TestPersistenceTestSuite(t *testing.T) {
	suite.Run(t, new(PersistenceTestSuite))
}
