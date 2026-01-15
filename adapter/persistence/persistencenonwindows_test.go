//go:build !windows

package persistence

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Cannot cause EMFILE errors by opening too many file descriptors
//
// Not run on Windows as there is no clean way to set maximum file
// descriptors. Not an issue as the code itself is tested.
func (s *PersistenceTestSuite) TestCannotCauseEMFILEErrorsByOpeningTooManyFileDescriptors() {
	ctx, cancel := context.WithTimeout(s.T().Context(), 5000*time.Millisecond)
	defer cancel()

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
			filehandle, err = os.OpenFile("../../test_lac/openFdsTestFile", os.O_RDONLY|os.O_CREATE, 0666)
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
			filehandle, err = os.OpenFile("../../test_lac/openFdsTestFile2", os.O_RDONLY|os.O_CREATE, 0666)
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
