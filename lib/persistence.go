package lib

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"slices"

	"github.com/dolmen-go/contextio"
	"github.com/vinicius-lino-figueiredo/gedb"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/errs"
)

const (
	DefaultDirMode  os.FileMode = 0o755
	DefaultFileMode os.FileMode = 0o644
)

// Persistence implements gedb.Persistence.
type Persistence struct {
	inMemoryOnly          bool
	filename              string
	corruptAlertThreshold float64
	fileMode              os.FileMode
	dirMode               os.FileMode
	serializer            gedb.Serializer
	deserializer          gedb.Deserializer
	broadcaster           *ctxsync.Cond
	storage               gedb.Storage
	decoder               gedb.Decoder
	comparer              gedb.Comparer
	documentFactory       func(any) (gedb.Document, error)
	hasher                gedb.Hasher
}

// NewPersistence returns a new implementation of gedb.Persistence.
func NewPersistence(options gedb.PersistenceOptions) (*Persistence, error) {

	inMemoryOnly := options.InMemoryOnly
	filename := options.Filename
	corruptAlertThreshold := options.CorruptAlertThreshold

	// This should be for null values, but for that to work I would either
	// have to accept an interface or a pointer. They both are terrible to
	// deal with when you have a constant as a value, so this implementation
	// uses a default value when receiving negative numbers.
	if options.CorruptAlertThreshold < 0 {
		corruptAlertThreshold = 0.1
	}

	fileMode := options.FileMode
	if options.FileMode == 0 {
		fileMode = DefaultFileMode
	}
	dirMode := options.DirMode
	if options.DirMode == 0 {
		dirMode = DefaultDirMode
	}
	if !inMemoryOnly && filename != "" && filename[len(filename)-1] == byte('~') {
		return nil, errors.New("the datafile name can't end with a ~, which is reserved for crash safe backup files")
	}

	comparer := options.Comparer
	if comparer == nil {
		comparer = NewComparer()
	}

	documentFactory := options.DocumentFactory
	if documentFactory == nil {
		documentFactory = NewDocument
	}

	serializer := options.Serializer
	if serializer == nil {
		serializer = NewSerializer(options.Comparer, documentFactory)
	}

	decoder := options.Decoder
	if decoder == nil {
		decoder = NewDecoder()
	}

	deserializer := options.Deserializer
	if deserializer == nil {
		deserializer = NewDeserializer(decoder)
	}

	storage := options.Storage
	if storage == nil {
		storage = NewStorage()
	}

	if options.Hasher == nil {
		options.Hasher = NewHasher()
	}

	return &Persistence{
		inMemoryOnly:          inMemoryOnly || filename == "",
		filename:              filename,
		corruptAlertThreshold: corruptAlertThreshold,
		fileMode:              fileMode,
		dirMode:               dirMode,
		serializer:            serializer,
		deserializer:          deserializer,
		broadcaster:           ctxsync.NewCond(),
		storage:               storage,
		decoder:               decoder,
		comparer:              comparer,
		documentFactory:       documentFactory,
		hasher:                options.Hasher,
	}, nil
}

// SetCorruptAlertThreshold implements gedb.Persistence.
func (p *Persistence) SetCorruptAlertThreshold(v float64) {
	p.corruptAlertThreshold = v
}

// PersistNewState implements gedb.Persistence.
func (p *Persistence) PersistNewState(ctx context.Context, newDocs ...gedb.Document) error {
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

// TreadRawStream implements gedb.Persistence.
func (p *Persistence) TreadRawStream(ctx context.Context, rawStream io.Reader) ([]gedb.Document, map[string]gedb.IndexDTO, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	dataByID := newNonComparableMap[gedb.Document](p.hasher, p.comparer)

	indexes := make(map[string]gedb.IndexDTO)

	corruptItems := 0

	lineStream := bufio.NewScanner(rawStream)
	dataLength := 0

	for lineStream.Scan() {
		if err := lineStream.Err(); err != nil {
			return nil, nil, err
		}
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
			comp, err := p.comparer.Compare(doc.Get("$$deleted"), true)
			if err != nil {
				corruptItems++
				continue
			}
			if comp == 0 {
				dataByID.Delete(doc.ID())
			} else {
				dataByID.Set(doc.ID(), doc)
			}
		} else {
			if d := doc.D("$$indexCreated"); d != nil && d.Get("fieldName") != nil {
				ni := new(gedb.IndexDTO)
				err := p.decoder.Decode(doc, ni)
				if err != nil {
					corruptItems++
					continue
				}
				indexes[ni.IndexCreated.FieldName] = *ni

			} else if doc.Get("$$indexRemoved") != nil {
				if s, ok := doc.Get("$$indexRemoved").(string); ok {
					delete(indexes, s)
				}
			}
		}
		dataLength++
	}
	if dataLength > 0 {
		corruptionRate := float64(corruptItems) / float64(dataLength)
		if corruptionRate > p.corruptAlertThreshold {
			return nil, nil, errs.ErrCorruptFiles{
				CorruptionRate:        corruptionRate,
				CorruptItems:          corruptItems,
				DataLength:            dataLength,
				CorruptAlertThreshold: p.corruptAlertThreshold,
			}
		}
	}
	data := slices.Collect(dataByID.Values())
	return data, indexes, nil
}

// LoadDatabase implements gedb.Persistence.
func (p *Persistence) LoadDatabase(ctx context.Context) ([]gedb.Document, map[string]gedb.IndexDTO, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	// In-memory only datastore
	if p.inMemoryOnly {
		return nil, nil, nil
	}
	// NOTE: Not Reseting DB indexes here. This should be done in the
	// datastore implementation

	// In-memoery only datastore
	if p.inMemoryOnly {
		return nil, nil, nil
	}

	err := p.ensureParentDirectoryExists(ctx, p.filename, p.dirMode)
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

	newDocs, newIdxs, err := p.TreadRawStream(ctx, fileStream)
	if err != nil {
		return nil, nil, err
	}

	// NOTE: The original function modifies some data in the datastore
	// instance. This is intentionally avoided to prevent coupling between
	// datastore types in this package
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

	if err = p.PersistCachedDatabase(ctx, newDocs, newIdxs); err != nil {
		return nil, nil, err
	}

	return newDocs, newIdxs, nil
}

// DropDatabase implements gedb.Persistence.
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

// PersistCachedDatabase implements gedb.Persistence.
func (p *Persistence) PersistCachedDatabase(ctx context.Context, allData []gedb.Document, indexes map[string]gedb.IndexDTO) error {
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

func (p *Persistence) ensureParentDirectoryExists(ctx context.Context, dir string, mode os.FileMode) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return p.storage.EnsureParentDirectoryExists(dir, mode)
}

// WaitCompaction implements gedb.Persistence.
func (p *Persistence) WaitCompaction(ctx context.Context) error {
	return p.broadcaster.WaitWithContext(ctx)
}
