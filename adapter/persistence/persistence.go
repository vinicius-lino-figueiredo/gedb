// Package persistence contains the default [domain.Persistence] implementation.
package persistence

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/dolmen-go/contextio"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/deserializer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/hasher"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/serializer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/storage"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/uncomparable"
)

const (
	DefaultDirMode  os.FileMode = 0o755
	DefaultFileMode os.FileMode = 0o644
)

type docMap = *uncomparable.Map[domain.Document]

// Persistence implements domain.Persistence.
type Persistence struct {
	inMemoryOnly          bool
	filename              string
	corruptAlertThreshold float64
	fileMode              os.FileMode
	dirMode               os.FileMode
	serializer            domain.Serializer
	deserializer          domain.Deserializer
	broadcaster           *ctxsync.Cond
	storage               domain.Storage
	decoder               domain.Decoder
	comparer              domain.Comparer
	documentFactory       func(any) (domain.Document, error)
	hasher                domain.Hasher
}

// NewPersistence returns a new implementation of domain.Persistence.
func NewPersistence(options ...Option) (domain.Persistence, error) {

	p := Persistence{
		filename:              "",
		comparer:              comparer.NewComparer(),
		inMemoryOnly:          false,
		corruptAlertThreshold: 0.1,
		fileMode:              DefaultFileMode,
		dirMode:               DefaultDirMode,
		storage:               storage.NewStorage(),
		decoder:               decoder.NewDecoder(),
		documentFactory:       data.NewDocument,
		hasher:                hasher.NewHasher(),
		broadcaster:           ctxsync.NewCond(),
	}
	for _, option := range options {
		option(&p)
	}
	if p.deserializer == nil {
		p.deserializer = deserializer.NewDeserializer(p.decoder)
	}
	if p.serializer == nil {
		p.serializer = serializer.NewSerializer(
			p.comparer,
			p.documentFactory,
		)
	}

	if !p.inMemoryOnly && p.filename != "" && strings.HasSuffix(p.filename, "~") {
		return nil, domain.ErrDatafileName{Name: p.filename, Reason: "cannot end with '~', reserved for backup files"}
	}

	return &p, nil
}

// SetCorruptAlertThreshold implements domain.Persistence.
func (p *Persistence) SetCorruptAlertThreshold(v float64) {
	p.corruptAlertThreshold = v
}

// PersistNewState implements domain.Persistence.
func (p *Persistence) PersistNewState(ctx context.Context, newDocs ...domain.Document) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// In-memory only datastore
	if p.inMemoryOnly {
		return nil
	}

	toPersist := new(bytes.Buffer)
	wr := contextio.NewWriter(ctx, toPersist)
	var b []byte
	var err error
	for _, doc := range newDocs {
		b, err = p.serializer.Serialize(ctx, doc)
		if err != nil {
			return err
		}
		_, err = wr.Write(append(b, byte('\n')))
		if err != nil {
			return err
		}
	}
	if toPersist.Len() == 0 {
		return nil
	}

	_, err = p.storage.AppendFile(p.filename, p.fileMode, toPersist.Bytes())

	return err
}

// TreatRawStream implements domain.Persistence.
func (p *Persistence) TreatRawStream(ctx context.Context, rawStream io.Reader) (docs []domain.Document, indexes map[string]domain.IndexDTO, err error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	dataByID := uncomparable.New[domain.Document](p.hasher, p.comparer)

	indexes = make(map[string]domain.IndexDTO)

	corruptItems := 0

	lineStream := bufio.NewScanner(rawStream)
	dataLength := 0

	for lineStream.Scan() {
		line := lineStream.Bytes()
		if len(line) == 0 {
			continue
		}
		m := make(map[string]any)
		err := p.deserializer.Deserialize(ctx, line, &m)
		if err != nil {
			corruptItems++
			dataLength++
			continue
		}
		doc, err := p.documentFactory(m)
		if err != nil {
			corruptItems++
			continue
		}
		if doc.Has("_id") {
			err := p.addOrDeleteDoc(doc, dataByID)
			if err != nil {
				corruptItems++
				continue
			}
		} else {
			if err := p.addOrDeleteIndex(doc, indexes); err != nil {
				corruptItems++
				continue
			}
		}
		dataLength++
	}
	if err := lineStream.Err(); err != nil {
		return nil, nil, err
	}
	if dataLength > 0 {
		corruptionRate := float64(corruptItems) / float64(dataLength)
		if corruptionRate > p.corruptAlertThreshold {
			return nil, nil, domain.ErrCorruptFiles{
				CorruptionRate:        corruptionRate,
				CorruptItems:          corruptItems,
				DataLength:            dataLength,
				CorruptAlertThreshold: p.corruptAlertThreshold,
			}
		}
	}
	docs = slices.Collect(dataByID.Values())
	return docs, indexes, nil
}

// if doc is a valid Index record, add or remove from the map; if not, ignore
func (p *Persistence) addOrDeleteIndex(doc domain.Document, m map[string]domain.IndexDTO) error {
	if d := doc.D("$$indexCreated"); d != nil && d.Get("fieldName") != nil {
		var ni domain.IndexDTO
		err := p.decoder.Decode(doc, &ni)
		if err != nil {
			return err
		}
		m[ni.IndexCreated.FieldName] = ni
		return nil

	}
	if doc.Get("$$indexRemoved") != nil {
		if s, ok := doc.Get("$$indexRemoved").(string); ok {
			delete(m, s)
		}
	}
	return nil
}

// if doc is a valid document declaration, add to the map; if not, ignore
func (p *Persistence) addOrDeleteDoc(doc domain.Document, m docMap) error {
	comp, err := p.comparer.Compare(doc.Get("$$deleted"), true)
	if err != nil {
		return err
	}
	if comp == 0 {
		return m.Delete(doc.ID())
	}
	return m.Set(doc.ID(), doc)
}

// LoadDatabase implements domain.Persistence.
func (p *Persistence) LoadDatabase(ctx context.Context) (docs []domain.Document, indexes map[string]domain.IndexDTO, err error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	// NOTE: Not Resetting DB indexes here. This should be done in the
	// datastore implementation

	// In-memory only datastore
	if p.inMemoryOnly {
		return nil, nil, nil
	}

	err = p.EnsureParentDirectoryExists(ctx, p.filename, p.dirMode)
	if err != nil {
		return nil, nil, err
	}
	err = p.storage.EnsureDatafileIntegrity(p.filename, p.fileMode)
	if err != nil {
		return nil, nil, err
	}

	// This would be server side, but there is no browser option in go and
	// TreatRawData (bytes version) was removed
	fileStream, err := p.storage.ReadFileStream(p.filename, p.fileMode)
	if err != nil {
		return nil, nil, err
	}
	defer fileStream.Close()

	docs, indexes, err = p.TreatRawStream(ctx, fileStream)
	if err != nil {
		return nil, nil, err
	}

	// NOTE: The original function (shown below) modifies some data in the
	// datastore instance. This is intentionally avoided to prevent coupling
	// between datastore types in this package
	//
	// // Recreate all indexes in the datafile
	// Object.keys(treatedData.indexes).forEach(key => {
	//   this.db.indexes[key] = new Index(treatedData.indexes[key])
	// })
	//
	// // Fill cached database (i.e. all indexes) with data
	// try {
	//   this.db._resetIndexes(treatedData.data)
	// } catch (e) {
	//   this.db._resetIndexes() // Rollback any index which didn't fail
	//   throw e
	// }
	//
	// await this.db.persistence.persistCachedDatabaseAsync()
	// this.db.executor.processBuffer()

	if err = p.PersistCachedDatabase(ctx, docs, indexes); err != nil {
		return nil, nil, err
	}

	return docs, indexes, nil
}

// DropDatabase implements domain.Persistence.
func (p *Persistence) DropDatabase(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if !p.inMemoryOnly {
		exists, err := p.storage.Exists(p.filename)
		if err != nil {
			return err
		}
		if exists {
			return p.storage.Remove(p.filename)
		}
	}
	return nil
}

// PersistCachedDatabase implements domain.Persistence.
func (p *Persistence) PersistCachedDatabase(ctx context.Context, allData []domain.Document, indexes map[string]domain.IndexDTO) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.inMemoryOnly {
		return nil
	}

	var lines [][]byte

	for _, doc := range allData {
		b, err := p.serializer.Serialize(ctx, doc)
		if err != nil {
			return err
		}
		lines = append(lines, b)
	}

	for fieldName, idx := range indexes {
		if fieldName != "_id" {
			b, err := p.serializer.Serialize(ctx, idx)
			if err != nil {
				return err
			}
			lines = append(lines, b)
		}
	}

	if err := p.storage.CrashSafeWriteFileLines(p.filename, lines, p.dirMode, p.fileMode); err != nil {
		return err
	}

	p.broadcaster.Broadcast()

	return nil
}

func (p *Persistence) EnsureParentDirectoryExists(ctx context.Context, dir string, mode os.FileMode) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return p.storage.EnsureParentDirectoryExists(dir, mode)
}

// WaitCompaction implements domain.Persistence.
func (p *Persistence) WaitCompaction(ctx context.Context) error {
	return p.broadcaster.WaitWithContext(ctx)
}
