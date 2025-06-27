// Package main represents a executable file that crashes during persistence
package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/vinicius-lino-figueiredo/nedb"
	"github.com/vinicius-lino-figueiredo/nedb/lib"
	"github.com/vinicius-lino-figueiredo/nedb/pkg/errs"
)

func main() {
	per, err := lib.NewPersistence(nedb.PersistenceOptions{Filename: "../workspace/lac.db", Storage: fakeStorage{}})
	if err != nil {
		log.Fatal(err)
	}
	_, _, err = per.LoadDatabase(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

type fakeWriteStream struct {
	filename string
	content  *bytes.Buffer
	storage  fakeStorage
	fileMode os.FileMode
}

func (f fakeWriteStream) Write(chunk []byte) (int, error) {
	return f.content.Write(chunk)
}

func (f fakeWriteStream) Close() error {
	return f.storage.WriteFile(f.filename, f.content.Bytes(), f.fileMode)
}

type fakeStorage struct {
	lib.DefaultStorage
}

func (fs fakeStorage) AppendFile(filename string, mode os.FileMode, data []byte) (int, error) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, mode)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.Write(data)
}

func (fs fakeStorage) CrashSafeWriteFileLines(filename string, lines [][]byte, dirMode os.FileMode, fileMode os.FileMode) error {
	tempFilename := filename + "~"

	if err := fs.flushToStorage(filepath.Dir(filename), true, dirMode); err != nil {
		return err
	}

	exists, err := fs.Exists(filename)
	if err != nil {
		return err
	}

	if exists {
		if err := fs.flushToStorage(filename, false, fileMode); err != nil {
			return err
		}
	}

	if err := fs.writeFileLines(tempFilename, lines, fileMode); err != nil {
		return err
	}

	if err := fs.flushToStorage(tempFilename, false, fileMode); err != nil {
		return err
	}

	if err := fs.rename(tempFilename, filename); err != nil {
		return err
	}

	if err := fs.flushToStorage(filename, true, dirMode); err != nil {
		return err
	}
	return nil
}

func (fs fakeStorage) EnsureDatafileIntegrity(filename string, mode os.FileMode) error {
	tempFilename := filename + "~"

	filenameExists, err := fs.Exists(filename)
	if err != nil {
		return err
	}
	// Write was successful
	if filenameExists {
		return nil
	}

	oldFilenameExists, err := fs.Exists(tempFilename)
	if err != nil {
		return err
	}
	// New database
	if !oldFilenameExists {
		return fs.WriteFile(filename, nil, mode)
	}
	return os.Rename(tempFilename, filename)
}

func (fs fakeStorage) EnsureParentDirectoryExists(filename string, mode os.FileMode) error {
	dir := filepath.Dir(filename)
	parsedDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	root := filepath.VolumeName(parsedDir) + string(os.PathSeparator)
	if runtime.GOOS != "windows" || parsedDir != root || filepath.Base(parsedDir) != "" {
		return os.MkdirAll(parsedDir, mode)
	}
	return nil
}

func (fs fakeStorage) Exists(filename string) (bool, error) {
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (fs fakeStorage) flushToStorage(filename string, isDir bool, mode os.FileMode) error {
	flags := os.O_RDWR
	if isDir {
		flags = os.O_RDONLY
	}

	fileHandle, err := os.OpenFile(filename, flags, mode)
	if err != nil {
		return errs.ErrFlushToStorage{ErrorOnFsync: err}
	}

	if err := fileHandle.Sync(); err != nil {
		return errs.ErrFlushToStorage{ErrorOnFsync: err}
	}

	if err := fileHandle.Close(); err != nil {
		return errs.ErrFlushToStorage{ErrorOnClose: err}
	}

	return nil
}

func (fs fakeStorage) ReadFileStream(filename string, mode os.FileMode) (io.ReadCloser, error) {
	return os.OpenFile(filename, os.O_RDONLY, mode)
}

func (fs fakeStorage) rename(oldPath string, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func (fs fakeStorage) writeFileLines(filename string, lines [][]byte, mode os.FileMode) error {
	stream, err := fs.writeFileStream(filename, mode)
	if err != nil {
		return err
	}
	defer stream.Close()
	for _, line := range lines {
		if _, err = stream.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func (fs fakeStorage) writeFileStream(filename string, mode os.FileMode) (io.WriteCloser, error) {
	return fakeWriteStream{
		filename: filename,
		storage:  fs,
		content:  new(bytes.Buffer),
		fileMode: mode,
	}, nil
}

func (fs fakeStorage) WriteFile(filename string, data []byte, mode os.FileMode) error {
	onePassDone := false

	filehandle, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer filehandle.Close()

	buffer := bytes.NewBuffer(data)
	length := buffer.Len()
	offset := 0

	for length > 0 {
		if onePassDone { // Crash on purpose before rewrite done
			os.Exit(1)
		}
		bytesWritten, err := filehandle.Write(buffer.Next(5000))
		if err != nil {
			return err
		}
		onePassDone = true
		offset += bytesWritten
		length -= bytesWritten
	}
	return nil
}
