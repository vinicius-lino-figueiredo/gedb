package lib

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"slices"

	"github.com/dolmen-go/contextio"
	"github.com/mitchellh/mapstructure"
	"github.com/vinicius-lino-figueiredo/gedb"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/errs"
)

const (
	DefaultDirMode  os.FileMode = 0o755
	DefaultFileMode os.FileMode = 0o644
)

type persistence struct {
	inMemoryOnly          bool
	filename              string
	corruptAlertThreshold float64
	fileMode              os.FileMode
	dirMode               os.FileMode
	serializer            gedb.Serializer
	deserializer          gedb.Deserializer
	broadcaster           *ctxsync.Cond
	storage               gedb.Storage
}

func NewPersistence(options gedb.PersistenceOptions) (*persistence, error) {

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

	serializer := options.Serializer
	if serializer == nil {
		serializer = defaultSerializer{}
	}

	deserializer := options.Deserializer
	if deserializer == nil {
		deserializer = defaultDeserializer{}
	}

	storage := options.Storage
	if storage == nil {
		storage = DefaultStorage{}
	}

	return &persistence{
		inMemoryOnly:          inMemoryOnly,
		filename:              filename,
		corruptAlertThreshold: corruptAlertThreshold,
		fileMode:              fileMode,
		dirMode:               dirMode,
		serializer:            serializer,
		deserializer:          deserializer,
		broadcaster:           ctxsync.NewCond(),
		storage:               storage,
	}, nil
}

func (p *persistence) SetCorruptAlertThreshold(v float64) {
	p.corruptAlertThreshold = v
}

func (p *persistence) PersistNewState(ctx context.Context, newDocs ...gedb.Document) error {
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

func (p *persistence) TreadRawStream(ctx context.Context, rawStream io.Reader) ([]gedb.Document, map[string]gedb.IndexDTO, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	dataByID := make(map[string]gedb.Document)

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
		doc := make(Document)
		err := p.deserializer.Deserialize(ctx, line, &doc)
		if err != nil {
			corruptItems++
			dataLength++
			continue
		}
		if doc.Contains("_id") {
			if doc.CompareKey("$$deleted", true) == 0 {
				delete(dataByID, doc.ID())
			} else {
				dataByID[doc.ID()] = doc
			}
		} else if doc.Contains("$$indexCreated") && doc.Get("$$indexCreated", "fieldName") != nil {
			ni := new(gedb.IndexDTO)
			if err := mapstructure.Decode(doc, ni); err != nil {
				corruptItems++
			} else {
				indexes[ni.IndexCreated.FieldName] = *ni
			}

		} else if doc["$$indexRemoved"] != nil {
			if s, ok := doc["$$indexRemoved"].(string); ok {
				delete(indexes, s)
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
	data := slices.Collect(maps.Values(dataByID))
	return data, indexes, nil
}

func (p *persistence) LoadDatabase(ctx context.Context) ([]gedb.Document, map[string]gedb.IndexDTO, error) {
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

	err := p.ensureParentDirectoryExistsAsync(ctx, p.filename, p.dirMode)
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

func (p *persistence) DropDatabase(ctx context.Context) error {
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

func (p *persistence) PersistCachedDatabase(ctx context.Context, allData []gedb.Document, indexes map[string]gedb.IndexDTO) error {
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

func (p *persistence) ensureParentDirectoryExistsAsync(ctx context.Context, dir string, mode os.FileMode) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return p.storage.EnsureParentDirectoryExists(dir, mode)
}

func (p *persistence) WaitCompaction(ctx context.Context) error {
	return p.broadcaster.WaitWithContext(ctx)
}
