package persistence

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/deserializer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/hasher"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/serializer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/storage"
)

type serializeFunc func(context.Context, any) ([]byte, error)

func (s serializeFunc) Serialize(ctx context.Context, v any) ([]byte, error) { return s(ctx, v) }

type deserializeFunc func(context.Context, []byte, any) error

func (s deserializeFunc) Deserialize(ctx context.Context, b []byte, v any) error {
	return s(ctx, b, v)
}

type readerMock struct{ mock.Mock }

// Read implements io.Reader.
func (r *readerMock) Read(p []byte) (n int, err error) {
	call := r.Called(p)
	return call.Int(0), call.Error(1)
}

type comparerMock struct{ mock.Mock }

// Comparable implements domain.Comparer.
func (c *comparerMock) Comparable(a any, b any) bool {
	return c.Called(a, b).Bool(0)
}

// Compare implements domain.Comparer.
func (c *comparerMock) Compare(a any, b any) (int, error) {
	call := c.Called(a, b)
	return call.Int(0), call.Error(1)
}

type storageMock struct{ mock.Mock }

// AppendFile implements domain.Storage.
func (s *storageMock) AppendFile(f string, m os.FileMode, b []byte) (int, error) {
	call := s.Called(f, m, b)
	return call.Int(0), call.Error(1)
}

// CrashSafeWriteFileLines implements domain.Storage.
func (s *storageMock) CrashSafeWriteFileLines(f string, l [][]byte, m1 os.FileMode, m2 os.FileMode) error {
	return s.Called(f, l, m1, m2).Error(0)
}

// EnsureDatafileIntegrity implements domain.Storage.
func (s *storageMock) EnsureDatafileIntegrity(f string, m os.FileMode) error {
	return s.Called(f, m).Error(0)
}

// EnsureParentDirectoryExists implements domain.Storage.
func (s *storageMock) EnsureParentDirectoryExists(f string, m os.FileMode) error {
	return s.Called(f, m).Error(0)
}

// Exists implements domain.Storage.
func (s *storageMock) Exists(f string) (bool, error) {
	call := s.Called(f)
	return call.Bool(0), call.Error(1)
}

// ReadFileStream implements domain.Storage.
func (s *storageMock) ReadFileStream(f string, m os.FileMode) (io.ReadCloser, error) {
	call := s.Called(f, m)
	return call.Get(0).(io.ReadCloser), call.Error(1)
}

// Remove implements domain.Storage.
func (s *storageMock) Remove(f string) error {
	return s.Called(f).Error(0)
}

var p *Persistence

type testStorage struct {
	*storage.Storage
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
	serializer domain.Serializer
	comparer   domain.Comparer
	testDbDir  string
	testDb     string
}

func (s *PersistenceTestSuite) SetupTest() {
	s.storage.Storage = storage.NewStorage().(*storage.Storage)
	s.comparer = comparer.NewComparer()

	s.testDbDir = s.T().TempDir()
	s.testDb = filepath.Join(s.testDbDir, "test.db")

	s.serializer = serializer.NewSerializer(s.comparer, data.NewDocument)
	if err := s.storage.EnsureParentDirectoryExists(s.testDb, DefaultDirMode); err != nil {
		s.FailNow("could not ensure parent directory", err)
	}

	exists, err := s.storage.Exists(s.testDb)
	if err != nil {
		s.FailNow("could not check datafile")
	}

	if exists {
		if err := os.Remove(s.testDb); err != nil {
			s.FailNow("could not remove datafile")
		}
	}

	if err := s.storage.EnsureDatafileIntegrity(s.testDb, DefaultFileMode); err != nil {
		s.FailNow("could not ensure datafile integrity", err)
	}

	per, err := NewPersistence(
		WithFilename(s.testDb),
		WithFileMode(DefaultFileMode),
		WithDirMode(DefaultDirMode),
		WithDecoder(decoder.NewDecoder()),
		WithHasher(hasher.NewHasher()),
	)
	s.NoError(err)

	p = per.(*Persistence)

	s.Equal(s.testDb, p.filename)

}

// Every line represents a document (with stream).
func (s *PersistenceTestSuite) TestEveryLineIsADocStream() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()
	rawData1, err1 := s.serializer.Serialize(ctx, data.M{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, data.M{"_id": "2", "hello": "world"})
	rawData3, err3 := s.serializer.Serialize(ctx, data.M{"_id": "3", "nested": data.M{"today": now}})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))

	treatedData, _, err := p.TreatRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)
	slices.SortFunc(treatedData, func(a, b domain.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 3)
	s.Equal(data.M{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(data.M{"_id": "2", "hello": "world"}, treatedData[1])
	s.Equal(data.M{"_id": "3", "nested": data.M{"today": now}}, treatedData[2])
}

// Badly formatted lines have no impact on the treated data (with stream).
func (s *PersistenceTestSuite) TestBadlyFormatedLinesStream() {
	p.SetCorruptAlertThreshold(1) // to prevent a corruption alert

	ctx := context.Background()

	now := float64((time.Time{}).Unix())
	rawData1, err1 := s.serializer.Serialize(ctx, data.M{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, data.M{"_id": "3", "nested": data.M{"today": now}})
	s.NoError(err1)
	s.NoError(err2)
	rawData := []byte(string(rawData1) + "\n" + "garbage" + "\n" + string(rawData2))
	treatedData, _, err := p.TreatRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)

	slices.SortFunc(treatedData, func(a, b domain.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(data.M{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(data.M{"_id": "3", "nested": data.M{"today": now}}, treatedData[1])
}

// Well formatted lines that have no _id are not included in the data (with stream).
func (s *PersistenceTestSuite) TestWellFormatedNoIDStream() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()

	rawData1, err1 := s.serializer.Serialize(ctx, data.M{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, data.M{"_id": "2", "hello": "world"})
	rawData3, err3 := s.serializer.Serialize(ctx, data.M{"nested": data.M{"today": now}})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))
	treatedData, _, err := p.TreatRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)

	slices.SortFunc(treatedData, func(a, b domain.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(data.M{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(data.M{"_id": "2", "hello": "world"}, treatedData[1])
}

// If two lines concern the same doc (= same _id), the last one is the good
// version (with stream).
func (s *PersistenceTestSuite) TestRepeatedID() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()

	rawData1, err1 := s.serializer.Serialize(ctx, data.M{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, data.M{"_id": "2", "hello": "world"})
	rawData3, err3 := s.serializer.Serialize(ctx, data.M{"_id": "1", "nested": data.M{"today": now}})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))
	treatedData, _, err := p.TreatRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)
	_ = treatedData

	slices.SortFunc(treatedData, func(a, b domain.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(data.M{"_id": "1", "nested": data.M{"today": now}}, treatedData[0])
	s.Equal(data.M{"_id": "2", "hello": "world"}, treatedData[1])
}

// If a doc contains $$deleted: true, that means we need to remove it from the
// data (with stream).
func (s *PersistenceTestSuite) TestDeleteDoc() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()
	rawData1, err1 := s.serializer.Serialize(ctx, data.M{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, data.M{"_id": "2", "hello": "world"})
	rawData3, err3 := s.serializer.Serialize(ctx, data.M{"_id": "1", "$$deleted": true})
	rawData4, err4 := s.serializer.Serialize(ctx, data.M{"_id": "3", "today": now})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	s.NoError(err4)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3) + "\n" + string(rawData4))

	treatedData, _, err := p.TreatRawStream(ctx, bytes.NewReader(rawData))
	slices.SortFunc(treatedData, func(a, b domain.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.NoError(err)
	s.Len(treatedData, 2)
	s.Equal(data.M{"_id": "2", "hello": "world"}, treatedData[0])
	s.Equal(data.M{"_id": "3", "today": now}, treatedData[1])
}

// If a doc contains $$deleted: true, no error is thrown if the doc wasn't in
// the []any before (with stream).
func (s *PersistenceTestSuite) TestDeleteUnexistentDoc() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()
	rawData1, err1 := s.serializer.Serialize(ctx, data.M{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, data.M{"_id": "2", "$$deleted": true})
	rawData3, err3 := s.serializer.Serialize(ctx, data.M{"_id": "3", "today": now})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))

	treatedData, _, err := p.TreatRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)
	slices.SortFunc(treatedData, func(a, b domain.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(data.M{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(data.M{"_id": "3", "today": now}, treatedData[1])
}

// If a doc contains $$indexCreated, no error is thrown during treatRawData and
// we can get the index options (with stream).
func (s *PersistenceTestSuite) TestIndexCreated() {
	now := float64((time.Time{}).Unix())
	ctx := context.Background()
	rawData1, err1 := s.serializer.Serialize(ctx, data.M{"_id": "1", "a": 2, "ages": []any{1, 5, 12}})
	rawData2, err2 := s.serializer.Serialize(ctx, data.M{"$$indexCreated": data.M{"fieldName": "test", "unique": true}})
	rawData3, err3 := s.serializer.Serialize(ctx, data.M{"_id": "3", "today": now})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	rawData := []byte(string(rawData1) + "\n" + string(rawData2) + "\n" + string(rawData3))

	treatedData, indexes, err := p.TreatRawStream(ctx, bytes.NewReader(rawData))
	s.NoError(err)
	s.Len(indexes, 1)
	s.Equal(domain.IndexDTO{IndexCreated: domain.IndexCreated{FieldName: "test", Unique: true}}, indexes["test"])

	slices.SortFunc(treatedData, func(a, b domain.Document) int { return s.compareThings(a.ID(), b.ID()) })
	s.Len(treatedData, 2)
	s.Equal(data.M{"_id": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}}, treatedData[0])
	s.Equal(data.M{"_id": "3", "today": now}, treatedData[1])
}

// Compact database on load.
func (s *PersistenceTestSuite) TestCompactOnLoad() {
	now := time.Now().Unix()

	id1 := uuid.New()

	docs := []domain.Document{
		data.M{"_id": id1, "a": 2, "createdAt": now, "updatedAt": now},
		data.M{"_id": uuid.New().String(), "a": 4, "createdAt": now, "updatedAt": now},
		data.M{"_id": id1, "a": 2, "$$deleted": true},
	}
	ctx := context.Background()
	s.NoError(p.PersistNewState(ctx, docs...))
	f, err := os.ReadFile(p.filename)
	s.NoError(err)
	dt := strings.Split(string(f), "\n")
	filledCount := 0

	for _, item := range dt {
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
	dt = strings.Split(string(f), "\n")
	filledCount = 0

	for _, item := range dt {
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

// Calling loadDatabase after the datafile was removed will reset the database.
//
// TODO: Reimplement with datastore (not testable as original version).
func (s *PersistenceTestSuite) TestCallAfterRemovingDatafile() {
	d1 := data.M{"_id": uuid.New().String(), "a": 1}
	d2 := data.M{"_id": uuid.New().String(), "a": 2}
	d := [][]domain.Document{
		{d1},
		{d1, d2},
	}

	ctx := context.Background()
	_, _, err := p.LoadDatabase(ctx)
	s.NoError(err)

	s.NoError(p.PersistNewState(ctx, d[0]...))
	s.NoError(p.PersistNewState(ctx, d[1]...))

	s.NoError(os.Remove(s.testDb))

	allData, _, err := p.LoadDatabase(ctx)
	s.NoError(err)

	s.Len(allData, 0)
}

// Will return error if Serialize fails.
func (s *PersistenceTestSuite) TestPersistNewStateFailSerializing() {
	e := fmt.Errorf("error")
	p.serializer = serializeFunc(func(context.Context, any) ([]byte, error) {
		return nil, e
	})

	err := p.PersistNewState(context.Background(), data.M{})
	s.ErrorIs(err, e)
}

// Will return error if Write fails.
func (s *PersistenceTestSuite) TestPersistNewStateFailWriting() {
	sr := p.serializer

	ctx, cancel := context.WithCancel(context.Background())

	p.serializer = serializeFunc(func(ctx context.Context, v any) ([]byte, error) {
		ctx = context.WithoutCancel(ctx)
		cancel()
		return sr.Serialize(ctx, v)
	})

	err := p.PersistNewState(ctx, data.M{})
	s.ErrorIs(err, context.Canceled)
}

// will return error if stream fails to be read.
func (s *PersistenceTestSuite) TestTreatRawStreamFailScan() {
	r := new(readerMock)

	errRead := fmt.Errorf("read error")
	r.On("Read", mock.Anything).
		Return(0, errRead).
		Once()

	doc, index, err := p.TreatRawStream(context.Background(), r)
	s.ErrorIs(err, errRead)
	s.Nil(doc)
	s.Nil(index)
}

// empty lines in a stream should not affect resulting data.
func (s *PersistenceTestSuite) TestIgnoreEmptyLines() {
	fakeData := `{"_id":"one","hello":"world"}






{"_id":"two","hello":"earth"}
{"_id":"three","hello":"you"}`

	r := strings.NewReader(fakeData)
	docs, indexes, err := p.TreatRawStream(context.Background(), r)
	s.NoError(err)
	s.Len(indexes, 0)
	s.Len(docs, 3)

}

// When treating raw data, refuse to proceed if too much data is corrupt, to
// avoid data loss.
func (s *PersistenceTestSuite) TestRefuseIfTooMuchIsCorrupt() {
	corruptTestFileName := filepath.Join(s.testDbDir, "corruptTest.db")
	fakeData := "{\"_id\":\"one\",\"hello\":\"world\"}\n" + "Some corrupt data\n" + "{\"_id\":\"two\",\"hello\":\"earth\"}\n" + "{\"_id\":\"three\",\"hello\":\"you\"}\n"
	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))

	var err error
	per, err := NewPersistence(WithFilename(corruptTestFileName))
	s.NoError(err)
	p = per.(*Persistence)

	ctx := context.Background()
	_, _, err = p.LoadDatabase(ctx)
	e := &domain.ErrCorruptFiles{}
	s.ErrorAs(err, e)
	s.Equal(0.25, e.CorruptionRate)
	s.Equal(1, e.CorruptItems)
	s.Equal(4, e.DataLength)

	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))
	per, err = NewPersistence(WithFilename(corruptTestFileName), WithCorruptAlertThreshold(1))
	s.NoError(err)
	p = per.(*Persistence)

	ctx = context.Background()
	_, _, err = p.LoadDatabase(ctx)
	s.NoError(err, e)

	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))
	per, err = NewPersistence(WithFilename(corruptTestFileName))
	s.NoError(err)
	p = per.(*Persistence)

	ctx = context.Background()
	_, _, err = p.LoadDatabase(ctx)
	s.ErrorAs(err, e)
	s.Equal(0.25, e.CorruptionRate)
	s.Equal(1, e.CorruptItems)
	s.Equal(4, e.DataLength)
}

// Treat document factory errors as data corruption.
func (s *PersistenceTestSuite) TestDocFactoryFailsAreCorruption() {
	corruptTestFileName := filepath.Join(s.testDbDir, "corruptTest.db")
	fakeData := "{\"_id\":\"one\",\"hello\":\"world\"}\n" + "{\"_id\":\"two\",\"hello\":\"earth\"}\n" + "{\"_id\":\"three\",\"hello\":\"you\"}\n"
	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))

	docFac := func(v any) (domain.Document, error) {
		d, err := data.NewDocument(v)
		if err != nil {
			return nil, err
		}
		if d.ID() == "two" {
			return nil, fmt.Errorf("error")
		}
		return d, nil
	}

	// Allowing no corrupt data it will fail
	var err error
	per, err := NewPersistence(
		WithFilename(corruptTestFileName),
		WithCorruptAlertThreshold(0),
		WithDocFactory(docFac),
	)
	s.NoError(err)
	p = per.(*Persistence)

	ctx := context.Background()

	docs, indexes, err := p.TreatRawStream(ctx, strings.NewReader(fakeData))
	s.ErrorIs(err, domain.ErrCorruptFiles{
		CorruptionRate:        0.5,
		CorruptItems:          1,
		DataLength:            2,
		CorruptAlertThreshold: 0,
	})
	s.Nil(docs)
	s.Nil(indexes)

	// allowing corrupt data, it will pass
	per, err = NewPersistence(
		WithFilename(corruptTestFileName),
		WithCorruptAlertThreshold(1),
		WithDocFactory(docFac),
	)
	s.NoError(err)
	p = per.(*Persistence)

	docs, indexes, err = p.TreatRawStream(ctx, strings.NewReader(fakeData))
	s.NoError(err)
	s.Len(docs, 2)
	s.Len(indexes, 0)
}

// Treat deleted document errors as data corruption.
func (s *PersistenceTestSuite) TestFailCheckingDeleted() {
	corruptTestFileName := filepath.Join(s.testDbDir, "corruptTest.db")
	fakeData := `{"_id":"two","$$deleted":true}
{"_id":"one","hello":"world"}`

	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))

	comp := new(comparerMock)
	comp.On("Compare", nil, true).
		Return(-1, nil).
		Once()
	comp.On("Compare", true, true).
		Return(0, fmt.Errorf("error")).
		Once()

	// Allowing no corrupt data it will fail
	var err error
	per, err := NewPersistence(
		WithFilename(corruptTestFileName),
		WithCorruptAlertThreshold(0),
		WithComparer(comp),
	)
	s.NoError(err)
	p = per.(*Persistence)

	ctx := context.Background()
	docs, indexes, err := p.TreatRawStream(ctx, strings.NewReader(fakeData))
	s.ErrorIs(err, domain.ErrCorruptFiles{
		CorruptionRate:        1,
		CorruptItems:          1,
		DataLength:            1,
		CorruptAlertThreshold: 0,
	})
	s.Nil(docs)
	s.Nil(indexes)

	comp.On("Compare", nil, true).
		Return(-1, nil).
		Once()
	comp.On("Compare", true, true).
		Return(0, fmt.Errorf("error")).
		Once()

	// Allowing corrupt data it will not fail
	per, err = NewPersistence(
		WithFilename(corruptTestFileName),
		WithCorruptAlertThreshold(1),
		WithComparer(comp),
	)
	s.NoError(err)
	p = per.(*Persistence)

	ctx = context.Background()
	docs, indexes, err = p.TreatRawStream(ctx, strings.NewReader(fakeData))
	s.NoError(err)
	s.Len(docs, 1)
	s.Len(indexes, 0)
}

// Malformed indexes are treated as data corruption.
func (s *PersistenceTestSuite) TestFailIndex() {
	corruptTestFileName := filepath.Join(s.testDbDir, "corruptTest.db")
	fakeData := `{"$$indexCreated": {"fieldName": "n"}, "$$indexRemoved": 1}
{"_id":"one","hello":"world"}`

	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))

	// Allowing no corrupt data it will fail
	var err error
	per, err := NewPersistence(
		WithFilename(corruptTestFileName),
		WithCorruptAlertThreshold(0),
	)
	s.NoError(err)
	p = per.(*Persistence)

	ctx := context.Background()
	docs, indexes, err := p.TreatRawStream(ctx, strings.NewReader(fakeData))
	s.ErrorIs(err, domain.ErrCorruptFiles{
		CorruptionRate:        1,
		CorruptItems:          1,
		DataLength:            1,
		CorruptAlertThreshold: 0,
	})
	s.Nil(docs)
	s.Nil(indexes)

	// Allowing corrupt data it will not fail
	per, err = NewPersistence(
		WithFilename(corruptTestFileName),
		WithCorruptAlertThreshold(1),
	)
	s.NoError(err)
	p = per.(*Persistence)

	ctx = context.Background()
	docs, indexes, err = p.TreatRawStream(ctx, strings.NewReader(fakeData))
	s.NoError(err)
	s.Len(docs, 1)
	s.Len(indexes, 0)
}

// Can remove an index.
func (s *PersistenceTestSuite) TestRemoveIndex() {
	corruptTestFileName := filepath.Join(s.testDbDir, "corruptTest.db")
	fakeData := `{"$$indexCreated": {"fieldName": "a"}}
{"$$indexCreated": {"fieldName": "n"}}
{"$$indexRemoved": "n"}`

	s.NoError(os.WriteFile(corruptTestFileName, []byte(fakeData), 0777))

	var err error
	per, err := NewPersistence(
		WithFilename(corruptTestFileName),
		WithCorruptAlertThreshold(0),
	)
	s.NoError(err)
	p = per.(*Persistence)

	ctx := context.Background()
	docs, indexes, err := p.TreatRawStream(ctx, strings.NewReader(fakeData))
	s.NoError(err)
	s.Len(docs, 0)
	s.Len(indexes, 1)
	s.Contains(indexes, "a")
}

// Can listen to compaction events.
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

// Cannot load database if parent directory was ensured.
func (s *PersistenceTestSuite) TestFailEnsureParentDirectory() {

	st := new(storageMock)

	var err error
	per, err := NewPersistence(
		WithFilename(s.testDb),
		WithStorage(st),
		WithCorruptAlertThreshold(0),
	)
	s.NoError(err)
	p = per.(*Persistence)

	errEnsure := fmt.Errorf("ensure error")
	st.On("EnsureParentDirectoryExists", s.testDb, p.dirMode).
		Return(errEnsure).
		Once()

	docs, indexes, err := p.LoadDatabase(context.Background())
	s.ErrorIs(err, errEnsure)
	s.Nil(docs)
	s.Nil(indexes)
}

// Cannot load database if datafile was ensured.
func (s *PersistenceTestSuite) TestFailEnsureDatafileIntegrity() {

	st := new(storageMock)

	var err error
	per, err := NewPersistence(
		WithFilename(s.testDb),
		WithStorage(st),
		WithCorruptAlertThreshold(0),
	)
	s.NoError(err)
	p = per.(*Persistence)

	st.On("EnsureParentDirectoryExists", s.testDb, p.dirMode).
		Return(nil).
		Once()
	errEnsure := fmt.Errorf("ensure error")
	st.On("EnsureDatafileIntegrity", s.testDb, p.fileMode).
		Return(errEnsure).
		Once()

	docs, indexes, err := p.LoadDatabase(context.Background())
	s.ErrorIs(err, errEnsure)
	s.Nil(docs)
	s.Nil(indexes)
}

// Cannot load database if file stream is not properly read.
func (s *PersistenceTestSuite) TestFailReadFile() {

	st := new(storageMock)

	var err error
	per, err := NewPersistence(
		WithFilename(s.testDb),
		WithStorage(st),
		WithCorruptAlertThreshold(0),
	)
	s.NoError(err)
	p = per.(*Persistence)

	st.On("EnsureParentDirectoryExists", s.testDb, p.dirMode).
		Return(nil).
		Once()
	st.On("EnsureDatafileIntegrity", s.testDb, p.fileMode).
		Return(nil).
		Once()
	errReadFileStream := fmt.Errorf("read file stream error")
	st.On("ReadFileStream", s.testDb, p.fileMode).
		Return(io.NopCloser(nil), errReadFileStream).
		Once()

	docs, indexes, err := p.LoadDatabase(context.Background())
	s.ErrorIs(err, errReadFileStream)
	s.Nil(docs)
	s.Nil(indexes)
}

func (s *PersistenceTestSuite) TestSerializers() {
	se := serializeFunc(func(ctx context.Context, v any) ([]byte, error) {
		s, err := s.serializer.Serialize(ctx, v)
		if err != nil {
			return nil, err
		}
		return []byte("before_" + string(s) + "_after"), nil
	})
	de := deserializeFunc(func(ctx context.Context, b []byte, v any) error {
		return deserializer.NewDeserializer(decoder.NewDecoder()).Deserialize(ctx, b[7:len(b)-6], v)
	})

	// Declare only one hook will not cause an error
	//
	// NOTE: The original code would throw an error when declaring only one
	// hook, but this check was not added to this code.
	s.Run("DeclareEitherSerializerOrDeserializer", func() {
		hookTestFilename := filepath.Join(s.testDbDir, "hookTest.db")
		s.NoError(s.storage.EnsureFileDoesntExist(hookTestFilename))
		s.NoError(os.WriteFile(hookTestFilename, []byte("Some content"), 0666))

		_, err := NewPersistence(
			WithFilename(hookTestFilename),
			WithSerializer(se),
		)
		s.NoError(err) // original would throw

		b, err := os.ReadFile(hookTestFilename)
		s.NoError(err)
		s.Equal("Some content", string(b)) // Data file left untouched

		_, err = NewPersistence(
			WithFilename(hookTestFilename),
			WithDeserializer(de),
		)
		s.NoError(err) // original would throw

		b, err = os.ReadFile(hookTestFilename)
		s.NoError(err)
		s.Equal("Some content", string(b)) // Data file left untouched

		_, err = NewPersistence(
			WithFilename(hookTestFilename),
			WithSerializer(se),
			WithDeserializer(de),
		)
		s.NoError(err)

		b, err = os.ReadFile(hookTestFilename)
		s.NoError(err)
		s.Equal("Some content", string(b)) // Data file left untouched
	})

	s.Run("UseSerializerWhenPersistingOrCompacting", func() {
		hookTestFilename := filepath.Join(s.testDbDir, "hookTest.db")
		s.NoError(s.storage.EnsureFileDoesntExist(hookTestFilename))
		var err error
		per, err := NewPersistence(
			WithFilename(hookTestFilename),
			WithSerializer(se),
			WithDeserializer(de),
		)
		s.NoError(err)
		p = per.(*Persistence)

		id1 := uuid.New().String()

		t := p
		_ = t

		ctx := context.Background()
		s.NoError(p.PersistNewState(ctx, data.M{"_id": id1, "hello": "world"}))
		s.NoError(p.PersistNewState(ctx, data.M{"_id": id1, "hello": "earth"}))
		s.NoError(p.PersistNewState(ctx, data.M{"$$indexCreated": data.M{"fieldName": "idefix"}}))

		_data, err := os.ReadFile(hookTestFilename)
		s.NoError(err)

		dt := bytes.Split(_data, []byte("\n"))

		var doc0, doc1 data.M
		var idx domain.IndexDTO

		s.Len(dt, 4)

		s.NoError(de(ctx, dt[0], &doc0))
		s.Len(doc0, 2)
		s.Equal("world", doc0["hello"])

		s.NoError(de(ctx, dt[1], &doc1))
		s.Len(doc1, 2)
		s.Equal("earth", doc1["hello"])

		s.NoError(de(ctx, dt[2], &idx))
		s.Equal(domain.IndexDTO{IndexCreated: domain.IndexCreated{FieldName: "idefix"}}, idx)

		allData := []domain.Document{
			data.M{"_id": id1, "hello": "earth"},
		}
		s.NoError(p.PersistCachedDatabase(ctx, allData, nil))
	})

	s.Run("LoadData", func() {
		hookTestFilename := filepath.Join(s.testDbDir, "hookTest.db")
		s.NoError(s.storage.EnsureFileDoesntExist(hookTestFilename))
		var err error
		per, err := NewPersistence(
			WithFilename(hookTestFilename),
			WithSerializer(se),
			WithDeserializer(de),
		)
		s.NoError(err)
		p = per.(*Persistence)
		ctx := context.Background()
		_id := uuid.New().String()
		id2 := uuid.New().String()
		s.NoError(p.PersistNewState(ctx, data.M{"_id": _id, "hello": "world"}))
		s.NoError(p.PersistNewState(ctx, data.M{"_id": id2, "ya": "ya"}))
		s.NoError(p.PersistNewState(ctx, data.M{"_id": _id, "hello": "earth"}))
		s.NoError(p.PersistNewState(ctx, data.M{"_id": id2, "$$deleted": true}))
		s.NoError(p.PersistNewState(ctx, data.M{"$$indexCreated": data.M{"fieldName": "idefix"}}))
		_data, err := os.ReadFile(hookTestFilename)
		s.NoError(err)
		dt := bytes.Split(_data, []byte("\n"))
		s.Len(dt, 6)

		// Everything is deserialized correctly, including deletes and indexes
		per, err = NewPersistence(
			WithFilename(hookTestFilename),
			WithSerializer(se),
			WithDeserializer(de),
		)
		s.NoError(err)
		p = per.(*Persistence)
		docs, indexes, err := p.LoadDatabase(ctx)
		s.NoError(err)
		s.Len(docs, 1)
		s.Equal("earth", docs[0].Get("hello"))
		s.Equal(_id, docs[0].Get("_id"))
		s.Len(indexes, 1) // Original is one, but we're not using datastore
		s.Contains(indexes, "idefix")
	})
} // ==== End of 'Serialization hooks' ==== //

// Creating a datastore with in memory as true and a bad filename won't
// cause an error
func (s *PersistenceTestSuite) TestInMemoryBadFilenameNoError() {
	_, err := NewPersistence(WithFilename(filepath.Join(s.testDbDir, "bad.db~")), WithInMemoryOnly(true))
	s.NoError(err)
}

// Creating a persistent datastore with a bad filename will cause an error
func (s *PersistenceTestSuite) TestPersistentBadFilenameError() {
	datafileName := filepath.Join(s.testDbDir, "bad.db~")
	_, err := NewPersistence(WithFilename(datafileName))
	s.ErrorIs(err, domain.ErrDatafileName{
		Name:   datafileName,
		Reason: "cannot end with '~', reserved for backup files",
	})
}

// If no file stat, ensureDatafileIntegrity creates an empty datafile
func (s *PersistenceTestSuite) TestCreateEmptyFileIfNoFileStat() {
	per, err := NewPersistence(WithFilename(filepath.Join(s.testDbDir, "it.db")))
	s.NoError(err)
	p := per.(*Persistence)

	fileExists, err := s.storage.Exists(filepath.Join(s.testDbDir, "it.db"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "it.db")))
	}
	fileExists, err = s.storage.Exists(filepath.Join(s.testDbDir, "it.db~"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "it.db~")))
	}

	s.NoFileExists(filepath.Join(s.testDbDir, "it.db"))
	s.NoFileExists(filepath.Join(s.testDbDir, "it.db~"))

	s.NoError(s.storage.EnsureDatafileIntegrity(p.filename, 0666))

	s.FileExists(filepath.Join(s.testDbDir, "it.db"))
	s.NoFileExists(filepath.Join(s.testDbDir, "it.db~"))

	b, err := os.ReadFile(filepath.Join(s.testDbDir, "it.db"))
	s.NoError(err)
	s.Len(b, 0)

}

// If only datafile stat, ensureDatafileIntegrity will use it
func (s *PersistenceTestSuite) TestUseDatafileIfExists() {
	per, err := NewPersistence(WithFilename(filepath.Join(s.testDbDir, "it.db")))
	s.NoError(err)
	p := per.(*Persistence)

	fileExists, err := s.storage.Exists(filepath.Join(s.testDbDir, "it.db"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "it.db")))
	}
	fileExists, err = s.storage.Exists(filepath.Join(s.testDbDir, "it.db~"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "it.db~")))
	}

	s.NoError(os.WriteFile(filepath.Join(s.testDbDir, "it.db"), []byte("something"), 0666))

	s.FileExists(filepath.Join(s.testDbDir, "it.db"))
	s.NoFileExists(filepath.Join(s.testDbDir, "it.db~"))

	s.NoError(s.storage.EnsureDatafileIntegrity(p.filename, 0666))

	s.FileExists(filepath.Join(s.testDbDir, "it.db"))
	s.NoFileExists(filepath.Join(s.testDbDir, "it.db~"))

	b, err := os.ReadFile(filepath.Join(s.testDbDir, "it.db"))
	s.NoError(err)
	s.Equal("something", string(b))
}

// If temp datafile stat and datafile doesn't, ensureDatafileIntegrity
// will use it (cannot happen except upon first use)
func (s *PersistenceTestSuite) TestUseTempDatafileIfExistsUponFirstUse() {
	per, err := NewPersistence(WithFilename(filepath.Join(s.testDbDir, "it.db")))
	s.NoError(err)
	p := per.(*Persistence)

	fileExists, err := s.storage.Exists(filepath.Join(s.testDbDir, "it.db"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "it.db")))
	}
	fileExists, err = s.storage.Exists(filepath.Join(s.testDbDir, "it.db~"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "it.db~")))
	}

	s.NoError(os.WriteFile(filepath.Join(s.testDbDir, "it.db~"), []byte("something"), 0666))

	s.NoFileExists(filepath.Join(s.testDbDir, "it.db"))
	s.FileExists(filepath.Join(s.testDbDir, "it.db~"))

	s.NoError(s.storage.EnsureDatafileIntegrity(p.filename, 0666))

	s.FileExists(filepath.Join(s.testDbDir, "it.db"))
	s.NoFileExists(filepath.Join(s.testDbDir, "it.db~"))

	b, err := os.ReadFile(filepath.Join(s.testDbDir, "it.db"))
	s.NoError(err)
	s.Equal("something", string(b))
}

// If both temp and current datafiles exist, ensureDatafileIntegrity
// will use the datafile, as it means that the write of the temp file
// failed
//
// Technically it could also mean the write was successful but the
// rename wasn't, but there is in any case no guarantee that the data in
// the temp file is whole so we have to discard the whole file
func (s *PersistenceTestSuite) TestUseDatafileIfBothExist() {
	per, err := NewPersistence(WithFilename(filepath.Join(s.testDbDir, "it.db")))
	s.NoError(err)
	p := per.(*Persistence)

	fileExists, err := s.storage.Exists(filepath.Join(s.testDbDir, "it.db"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "it.db")))
	}
	fileExists, err = s.storage.Exists(filepath.Join(s.testDbDir, "it.db~"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "it.db~")))
	}

	s.NoError(os.WriteFile(filepath.Join(s.testDbDir, "it.db"), []byte("{\"_id\":\"0\",\"hello\":\"world\"}"), 0666))
	s.NoError(os.WriteFile(filepath.Join(s.testDbDir, "it.db~"), []byte("{\"_id\":\"0\",\"hello\":\"other\"}"), 0666))

	s.FileExists(filepath.Join(s.testDbDir, "it.db"))
	s.FileExists(filepath.Join(s.testDbDir, "it.db~"))

	s.NoError(s.storage.EnsureDatafileIntegrity(p.filename, 0666))

	s.FileExists(filepath.Join(s.testDbDir, "it.db"))
	s.FileExists(filepath.Join(s.testDbDir, "it.db~"))

	b, err := os.ReadFile(filepath.Join(s.testDbDir, "it.db"))
	s.NoError(err)
	s.Equal("{\"_id\":\"0\",\"hello\":\"world\"}", string(b))
	b, err = os.ReadFile(filepath.Join(s.testDbDir, "it.db~"))
	s.NoError(err)
	s.Equal("{\"_id\":\"0\",\"hello\":\"other\"}", string(b))

	ctx := context.Background()

	docs, _, err := p.LoadDatabase(ctx)
	s.NoError(err)

	s.Len(docs, 1)
	s.Equal("world", docs[0].Get("hello"))

	s.FileExists(filepath.Join(s.testDbDir, "it.db"))
	s.NoFileExists(filepath.Join(s.testDbDir, "it.db~"))
}

// persistCachedDatabase should update the contents of the datafile and leave a clean state
func (s *PersistenceTestSuite) TestCleanDatafile() {
	ctx := context.Background()
	_id := uuid.New().String()
	s.NoError(p.PersistNewState(ctx, data.M{"_id": _id, "hello": "world"}))

	fileExists, err := s.storage.Exists(s.testDb)
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(s.testDb))
	}
	fileExists, err = s.storage.Exists(s.testDb + "~")
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(s.testDb + "~"))
	}
	s.NoFileExists(s.testDb)

	s.NoError(os.WriteFile(s.testDb+"~", []byte("something"), 0666))
	s.FileExists(s.testDb + "~")

	s.NoError(p.PersistCachedDatabase(ctx, []domain.Document{data.M{"_id": _id, "hello": "world"}}, nil))
	contents, err := os.ReadFile(s.testDb)
	s.NoError(err)

	fileExists, err = s.storage.Exists(s.testDb)
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(s.testDb))
	}
	s.True(fileExists)
	fileExists, err = s.storage.Exists(s.testDb + "~")
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(s.testDb + "~"))
	}

	s.False(fileExists)

	d := make(data.M)
	s.NoError(json.NewDecoder(bytes.NewReader(contents)).Decode(&d))
	s.Len(d, 2)
	s.Equal("world", d["hello"])
	s.Regexp(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`, d["_id"])
}

// After a persistCachedDatabase, there should be no temp or old filename
func (s *PersistenceTestSuite) TestNoTempFileAfterPersistCachedDatabase() {
	ctx := context.Background()
	_id := uuid.New().String()
	s.NoError(p.PersistNewState(ctx, data.M{"_id": _id, "hello": "world"}))

	fileExists, err := s.storage.Exists(s.testDb)
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(s.testDb))
	}
	fileExists, err = s.storage.Exists(s.testDb + "~")
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(s.testDb + "~"))
	}

	s.NoFileExists(s.testDb)
	s.NoFileExists(s.testDb + "~")

	s.NoError(os.WriteFile(s.testDb+"~", []byte("bloup"), 0666))
	s.FileExists(s.testDb + "~")
	s.NoError(p.PersistCachedDatabase(ctx, []domain.Document{data.M{"_id": _id, "hello": "world"}}, nil))
	contents, err := os.ReadFile(s.testDb)
	s.NoError(err)
	s.FileExists(s.testDb)
	s.NoFileExists(s.testDb + "~")
	d := make(data.M)
	s.NoError(json.NewDecoder(bytes.NewReader(contents)).Decode(&d))
	s.Len(d, 2)
	s.Equal("world", d["hello"])
	s.Regexp(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`, d["_id"])
}

// persistCachedDatabase should update the contents of the datafile and leave a clean state even if there is a temp datafile
func (s *PersistenceTestSuite) TestCleanDatafileIfTempFileExists() {
	ctx := context.Background()
	_id := uuid.New().String()
	s.NoError(p.PersistNewState(ctx, data.M{"_id": _id, "hello": "world"}))

	fileExists, err := s.storage.Exists(s.testDb)
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(s.testDb))
	}
	s.NoError(os.WriteFile(s.testDb+"~", []byte("blabla"), 0666))
	s.NoFileExists(s.testDb)
	s.FileExists(s.testDb + "~")

	s.NoError(p.PersistCachedDatabase(ctx, []domain.Document{data.M{"_id": _id, "hello": "world"}}, nil))
	contents, err := os.ReadFile(s.testDb)
	s.NoError(err)
	s.FileExists(s.testDb)
	s.NoFileExists(s.testDb + "~")
	d := make(data.M)
	s.NoError(json.NewDecoder(bytes.NewReader(contents)).Decode(&d))
	s.Len(d, 2)
	s.Equal("world", d["hello"])
	s.Regexp(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`, d["_id"])
}

// persistCachedDatabase should update the contents of the datafile and leave a clean state even if there is a temp datafile
func (s *PersistenceTestSuite) TestCleanDatafileIfEmptyTempFileExists() {
	dbFile := filepath.Join(s.testDbDir, "test2.db")

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

	p, err := NewPersistence(WithFilename(dbFile))
	s.NoError(err)

	ctx := context.Background()
	_, _, err = p.LoadDatabase(ctx)
	s.NoError(err)
	contents, err := os.ReadFile(dbFile)
	s.NoError(err)
	s.FileExists(dbFile)
	s.NoFileExists(dbFile + "~")
	s.Len(contents, 0)
}

// Persistence works as expected when everything goes fine
func (s *PersistenceTestSuite) TestWorkAsExpected() {
	dbFile := filepath.Join(s.testDbDir, "test2.db")

	s.NoError(s.storage.EnsureFileDoesntExist(dbFile))
	s.NoError(s.storage.EnsureFileDoesntExist(dbFile + "~"))

	p, err := NewPersistence(WithFilename(dbFile))
	s.NoError(err)

	ctx := context.Background()
	docs, _, err := p.LoadDatabase(ctx)
	s.NoError(err)
	s.Len(docs, 0)

	doc1 := data.M{"_id": uuid.New().String(), "a": "hello"}
	doc2 := data.M{"_id": uuid.New().String(), "a": "world"}

	s.NoError(p.PersistNewState(ctx, doc1, doc2))

	docs, _, err = p.LoadDatabase(ctx)
	s.NoError(err)
	s.Len(docs, 2)
	s.Equal("hello", docs[slices.IndexFunc(docs, func(a domain.Document) bool { return a.ID() == doc1.ID() })].Get("a"))
	s.Equal("world", docs[slices.IndexFunc(docs, func(a domain.Document) bool { return a.ID() == doc2.ID() })].Get("a"))

	s.FileExists(dbFile)
	s.NoFileExists(dbFile + "~")

	p2, err := NewPersistence(WithFilename(dbFile))
	s.NoError(err)
	docs, _, err = p2.LoadDatabase(ctx)
	s.NoError(err)
	s.Len(docs, 2)
	s.Equal("hello", docs[slices.IndexFunc(docs, func(a domain.Document) bool { return a.ID() == doc1.ID() })].Get("a"))
	s.Equal("world", docs[slices.IndexFunc(docs, func(a domain.Document) bool { return a.ID() == doc2.ID() })].Get("a"))

	s.FileExists(dbFile)
	s.NoFileExists(dbFile + "~")
}

func (s *PersistenceTestSuite) TestKeepOldVersionOnCrash() {
	const N = 500
	toWrite := new(bytes.Buffer)
	i := 0
	// let docI

	// Ensuring the state is clean
	fileExists, err := s.storage.Exists(filepath.Join(s.testDbDir, "lac.db"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "lac.db")))
	}
	fileExists, err = s.storage.Exists(filepath.Join(s.testDbDir, "lac.db~"))
	s.NoError(err)
	if fileExists {
		s.NoError(os.Remove(filepath.Join(s.testDbDir, "lac.db~")))
	}

	// Creating a db file with 150k records (a bit long to load)
	encoder := json.NewEncoder(toWrite)
	for i = range N {
		s.NoError(encoder.Encode(data.M{"_id": fmt.Sprintf("anid_%d", i), "hello": "world"}))
	}
	s.NoError(os.WriteFile(filepath.Join(s.testDbDir, "lac.db"), toWrite.Bytes(), 0666))

	datafile, err := os.ReadFile(filepath.Join(s.testDbDir, "lac.db"))
	s.NoError(err)
	datafileLength := len(datafile)

	s.Greater(datafileLength, 5000)

	// Loading it in a separate process that we will crash before finishing the loadDatabase
	s.Run("loadAndCrash", func() {
		// don't really like this approach, but testing by
		// running a main package
		cmd := exec.Command("go", "run", "../../../test_lac/loadandcrash.go", s.testDbDir)
		err := cmd.Run()
		e := &exec.ExitError{}
		s.ErrorAs(err, &e)
		status := e.Sys().(syscall.WaitStatus).ExitStatus()
		s.Equal(1, status)
		s.FileExists(filepath.Join(s.testDbDir, "lac.db"))
		s.FileExists(filepath.Join(s.testDbDir, "lac.db~"))
		f, err := os.ReadFile(filepath.Join(s.testDbDir, "lac.db"))
		s.NoError(err)
		s.Len(f, datafileLength)
		f, err = os.ReadFile(filepath.Join(s.testDbDir, "lac.db~"))
		s.NoError(err)
		s.Len(f, 5000)

		per, err := NewPersistence(WithFilename(filepath.Join(s.testDbDir, "lac.db")))
		s.NoError(err)

		ctx := context.Background()
		docs, _, err := per.LoadDatabase(ctx)
		s.NoError(err)
		s.FileExists(filepath.Join(s.testDbDir, "lac.db"))
		s.NoFileExists(filepath.Join(s.testDbDir, "lac.db~"))

		f, err = os.ReadFile(filepath.Join(s.testDbDir, "lac.db"))
		s.NoError(err)
		s.Len(f, datafileLength)

		slices.SortFunc(docs, func(a, b domain.Document) int {
			idA, idB := a.ID().(string), b.ID().(string)
			if len(idA) != len(idB) {
				return cmp.Compare(len(idA), len(idB))
			}
			return cmp.Compare(idA, idB)
		})

		s.Len(docs, N)
		for i, doc := range docs {
			s.Equal(data.M{"_id": fmt.Sprintf("anid_%d", i), "hello": "world"}, doc)
		}
	})
}

// Cannot cause EMFILE errors by opening too many file descriptors
//
// Not run on Windows as there is no clean way to set maximum file
// descriptors. Not an issue as the code itself is tested.
func (s *PersistenceTestSuite) TestCannotCauseEMFILEErrorsByOpeningTooManyFileDescriptors() {
	ctx, cancel := context.WithTimeout(s.T().Context(), 5000*time.Millisecond)
	defer cancel()

	if runtime.GOOS == "windows" {
		return
	}

	// not creating another file
	s.Run("openFdsLaunch", func() {
		N := 64

		var originalRLimit syscall.Rlimit
		s.NoError(syscall.Getrlimit(syscall.RLIMIT_NOFILE, &originalRLimit))

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
			filehandle, err = os.OpenFile("../../../test_lac/openFdsTestFile", os.O_RDONLY|os.O_CREATE, 0666)
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
			filehandle, err = os.OpenFile("../../../test_lac/openFdsTestFile2", os.O_RDONLY|os.O_CREATE, 0666)
			if err != nil {
				break
			}
			filehandles = append(filehandles, filehandle)
		}
		s.NoError(err)
		for _, fh := range filehandles {
			fh.Close()
		}
		p, err := NewPersistence(WithFilename(filepath.Join(s.testDbDir, "openfds.db")))
		s.NoError(err)
		docs, _, err := p.LoadDatabase(ctx)
		s.NoError(err)

		removed := make([]domain.Document, len(docs))
		for n, doc := range docs {
			removed[n] = data.M{"_id": doc.ID()}
		}
		s.NoError(p.PersistNewState(ctx, removed...))
		s.NoError(p.PersistNewState(ctx, data.M{"_id": uuid.New().String(), "hello": "world"}))
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
}

// NOTE: Most part of the this original test suite are Datastore related, not
// persistence. Im just adding a few tests so this is not empty.
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
// Check that the buffer is reset (added).
func (s *PersistenceTestSuite) TestDropDatabase() {
	s.Run("RemovesFile", func() {
		s.FileExists(s.testDb)
		ctx := context.Background()
		s.NoError(p.DropDatabase(ctx))
		s.NoFileExists(s.testDb)
	})
	s.SetupTest() // datafile is needed for the next test
	s.Run("ReloadAfterwards", func() {
		s.FileExists(s.testDb)
		ctx := context.Background()
		s.NoError(p.PersistNewState(ctx, data.M{"_id": uuid.New().String(), "hello": "world"}))
		s.FileExists(s.testDb)
		b, err := os.ReadFile(s.testDb)
		s.NoError(err)
		var lines [][]byte
		for line := range bytes.SplitSeq(b, []byte("\n")) {
			if len(line) > 0 {
				lines = append(lines, line)
			}
		}
		s.Len(lines, 1)
		s.NoError(p.DropDatabase(ctx))
		s.NoFileExists(s.testDb)
		s.NoError(p.PersistNewState(ctx, data.M{"_id": uuid.New().String(), "hello": "world"}))
		s.FileExists(s.testDb)
		b, err = os.ReadFile(s.testDb)
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
		s.FileExists(s.testDb)
		ctx := context.Background()
		s.NoError(p.DropDatabase(ctx))
		s.NoFileExists(s.testDb)
		s.NoError(p.DropDatabase(ctx))
		s.NoFileExists(s.testDb)
	})
	s.SetupTest()
	s.Run("ResetBuffer", func() {
		ctx := context.Background()
		s.NoError(p.DropDatabase(ctx))
		docs := []domain.Document{
			data.M{"_id": uuid.New().String(), "hello": "world"},
			data.M{"_id": uuid.New().String(), "hello": "world"},
			data.M{"_id": uuid.New().String(), "hello": "world"},
		}
		s.NoError(p.PersistCachedDatabase(ctx, docs, nil))
		s.NoError(p.DropDatabase(ctx))
		s.NoError(p.PersistNewState(ctx, data.M{"_id": uuid.New().String(), "hi": "world"}))
		docs, _, err := p.LoadDatabase(ctx)
		s.NoError(err)
		s.Len(docs, 1)
	})
}

// Will return error if calling methods with a cancelled context.
func (s *PersistenceTestSuite) TestPersistCancelledContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.PersistNewState(ctx)
	s.ErrorIs(err, context.Canceled)

	docs, indexes, err := p.TreatRawStream(ctx, nil)
	s.ErrorIs(err, context.Canceled)
	s.Nil(docs)
	s.Nil(indexes)

	docs, indexes, err = p.LoadDatabase(ctx)
	s.ErrorIs(err, context.Canceled)
	s.Nil(docs)
	s.Nil(indexes)

	err = p.DropDatabase(ctx)
	s.ErrorIs(err, context.Canceled)

	err = p.EnsureParentDirectoryExists(ctx, "", 0000)
	s.ErrorIs(err, context.Canceled)

	err = p.PersistCachedDatabase(ctx, nil, nil)
	s.ErrorIs(err, context.Canceled)
}

// Won't do anything if file related methods are called in a memory-only
// persistence instance.
func (s *PersistenceTestSuite) TestPersistNewStateInMemoryOnly() {
	p, err := NewPersistence(WithInMemoryOnly(true))
	s.NoError(err)

	err = p.PersistNewState(context.Background())
	s.NoError(err)

	docs, indexes, err := p.LoadDatabase(context.Background())
	s.NoError(err)
	s.Nil(docs)
	s.Nil(indexes)

	err = p.DropDatabase(context.Background())
	s.NoError(err)

	err = p.PersistCachedDatabase(context.Background(), nil, nil)
	s.NoError(err)
}

// Should not be able to drop the database without confirming the file exists.
func (s *PersistenceTestSuite) TestDropDatabaseFailCheckFileExists() {
	st := new(storageMock)

	var err error
	per, err := NewPersistence(
		WithFilename(s.testDb),
		WithStorage(st),
		WithCorruptAlertThreshold(0),
	)
	s.NoError(err)
	p = per.(*Persistence)

	errExists := fmt.Errorf("exists error")
	st.On("Exists", s.testDb).
		Return(false, errExists).
		Once()

	err = p.DropDatabase(context.Background())
	s.ErrorIs(err, errExists)
}

// Should not be able to persist data if serialization fails.
func (s *PersistenceTestSuite) TestPersistSerializeError() {

	errSerialize := fmt.Errorf("serialize error")

	original := p.serializer
	ser := serializeFunc(func(ctx context.Context, a any) ([]byte, error) {
		shouldFail := false
		switch t := a.(type) {
		case domain.Document:
			shouldFail = t.Get("shouldFail").(bool)
		case domain.IndexDTO:
			shouldFail = t.IndexCreated.FieldName == "shouldFail"
		}
		if shouldFail {
			return nil, errSerialize
		}
		return original.Serialize(ctx, a)
	})

	var err error
	per, err := NewPersistence(
		WithFilename(s.testDb),
		WithSerializer(ser),
		WithCorruptAlertThreshold(0),
	)
	s.NoError(err)
	p = per.(*Persistence)

	docsFile := `
{"_id": 1, "shouldFail": false}
{"_id": 2, "shouldFail": false}
{"_id": 3, "shouldFail": true}
{"_id": 4, "shouldFail": false}
`
	s.NoError(os.WriteFile(s.testDb, []byte(docsFile), p.fileMode))

	docs, indexes, err := p.LoadDatabase(context.Background())
	s.ErrorIs(err, errSerialize)
	s.Nil(docs)
	s.Nil(indexes)

	indexesFile := `
{"$$indexCreated": {"fieldName": "fieldA"}}
{"$$indexCreated": {"fieldName": "fieldB"}}
{"$$indexCreated": {"fieldName": "fieldC"}}
{"$$indexCreated": {"fieldName": "shouldFail"}}
{"$$indexCreated": {"fieldName": "fieldE"}}
`
	s.NoError(os.WriteFile(s.testDb, []byte(indexesFile), p.fileMode))

	docs, indexes, err = p.LoadDatabase(context.Background())
	s.ErrorIs(err, errSerialize)
	s.Nil(docs)
	s.Nil(indexes)

}

// To persist cached database, CrashSafeWriteFileLines must not fail.
func (s *PersistenceTestSuite) TestPersistCachedDatabaseFailWriting() {
	st := new(storageMock)

	var err error
	per, err := NewPersistence(
		WithFilename(s.testDb),
		WithStorage(st),
		WithCorruptAlertThreshold(0),
	)
	s.NoError(err)
	p = per.(*Persistence)
	errCrashSafeWrite := fmt.Errorf("crash safe write error")
	st.On("CrashSafeWriteFileLines", s.testDb, [][]byte(nil), p.dirMode, p.fileMode).
		Return(errCrashSafeWrite).
		Once()

	err = p.PersistCachedDatabase(context.Background(), nil, nil)
	s.ErrorIs(err, errCrashSafeWrite)

}

func (s *PersistenceTestSuite) compareThings(a any, b any) int {
	comp, _ := s.comparer.Compare(a, b)
	return comp
}

func TestPersistenceTestSuite(t *testing.T) {
	suite.Run(t, new(PersistenceTestSuite))
}
