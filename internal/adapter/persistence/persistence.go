package persistence

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/dolmen-go/contextio"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/deserializer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldgetter"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/hasher"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/serializer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/storage"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/errs"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/uncomparablemap"
)

const (
	DefaultDirMode  os.FileMode = 0o755
	DefaultFileMode os.FileMode = 0o644
)

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
func NewPersistence(options ...domain.PersistenceOption) (domain.Persistence, error) {

	comp := comparer.NewComparer()
	docFac := data.NewDocument
	dec := decoder.NewDecoder()
	se := serializer.NewSerializer(comp, docFac)
	de := deserializer.NewDeserializer(dec)
	opts := domain.PersistenceOptions{
		Filename:              "",
		Comparer:              comp,
		InMemoryOnly:          false,
		CorruptAlertThreshold: 0.1,
		FileMode:              DefaultFileMode,
		DirMode:               DefaultDirMode,
		Serializer:            se,
		Deserializer:          de,
		Storage:               storage.NewStorage(),
		Decoder:               dec,
		DocumentFactory:       docFac,
		Hasher:                hasher.NewHasher(),
		FieldGetter:           fieldgetter.NewFieldGetter(),
	}
	for _, option := range options {
		option(&opts)
	}

	if !opts.InMemoryOnly && opts.Filename != "" && strings.HasSuffix(opts.Filename, "~") {
		return nil, errors.New("the datafile name can't end with a ~, which is reserved for crash safe backup files")
	}

	return &Persistence{
		inMemoryOnly:          opts.InMemoryOnly || opts.Filename == "",
		filename:              opts.Filename,
		corruptAlertThreshold: opts.CorruptAlertThreshold,
		fileMode:              opts.FileMode,
		dirMode:               opts.DirMode,
		serializer:            opts.Serializer,
		deserializer:          opts.Deserializer,
		broadcaster:           ctxsync.NewCond(),
		storage:               opts.Storage,
		decoder:               opts.Decoder,
		comparer:              opts.Comparer,
		documentFactory:       opts.DocumentFactory,
		hasher:                opts.Hasher,
	}, nil
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

// TreadRawStream implements domain.Persistence.
func (p *Persistence) TreadRawStream(ctx context.Context, rawStream io.Reader) ([]domain.Document, map[string]domain.IndexDTO, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	dataByID := uncomparablemap.New[domain.Document](p.hasher, p.comparer)

	indexes := make(map[string]domain.IndexDTO)

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
				ni := new(domain.IndexDTO)
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

// LoadDatabase implements domain.Persistence.
func (p *Persistence) LoadDatabase(ctx context.Context) ([]domain.Document, map[string]domain.IndexDTO, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	// NOTE: Not Reseting DB indexes here. This should be done in the
	// datastore implementation

	// In-memoery only datastore
	if p.inMemoryOnly {
		return nil, nil, nil
	}

	err := p.EnsureParentDirectoryExists(ctx, p.filename, p.dirMode)
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
