package storage

import (
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Storage implements domain.Storage.
type Storage struct{}

// NewStorage returns a new implementation of domain.Storage.
func NewStorage() domain.Storage {
	return &Storage{}
}

// AppendFile implements domain.Storage.
func (d *Storage) AppendFile(filename string, mode os.FileMode, data []byte) (int, error) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, mode)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.Write(data)
}

// CrashSafeWriteFileLines implements domain.Storage.
func (d *Storage) CrashSafeWriteFileLines(filename string, lines [][]byte, dirMode os.FileMode, fileMode os.FileMode) error {
	tempFilename := filename + "~"

	if err := d.flushToStorage(filepath.Dir(filename), true, dirMode); err != nil {
		return err
	}

	exists, err := d.Exists(filename)
	if err != nil {
		return err
	}

	if exists {
		if err := d.flushToStorage(filename, false, fileMode); err != nil {
			return err
		}
	}

	if err := d.writeFileLines(tempFilename, lines, fileMode); err != nil {
		return err
	}

	if err := d.flushToStorage(tempFilename, false, fileMode); err != nil {
		return err
	}

	if err := d.rename(tempFilename, filename); err != nil {
		return err
	}

	if err := d.flushToStorage(filename, true, dirMode); err != nil {
		return err
	}
	return nil
}

// EnsureDatafileIntegrity implements domain.Storage.
func (d *Storage) EnsureDatafileIntegrity(filename string, mode os.FileMode) error {
	tempFilename := filename + "~"

	filenameExists, err := d.Exists(filename)
	if err != nil {
		return err
	}
	// Write was successful
	if filenameExists {
		return nil
	}

	oldFilenameExists, err := d.Exists(tempFilename)
	if err != nil {
		return err
	}
	// New database
	if !oldFilenameExists {
		return os.WriteFile(filename, nil, mode)
	}
	return os.Rename(tempFilename, filename)
}

// EnsureParentDirectoryExists implements domain.Storage.
func (d *Storage) EnsureParentDirectoryExists(filename string, mode os.FileMode) error {
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

// Exists implements domain.Storage.
func (d *Storage) Exists(filename string) (bool, error) {
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (d *Storage) flushToStorage(filename string, isDir bool, mode os.FileMode) error {
	flags := os.O_RDWR
	if isDir {
		flags = os.O_RDONLY
	}

	fileHandle, err := os.OpenFile(filename, flags, mode)
	if err != nil {
		return domain.ErrFlushToStorage{ErrorOnFsync: err}
	}

	if err := fileHandle.Sync(); err != nil {
		return domain.ErrFlushToStorage{ErrorOnFsync: err}
	}

	if err := fileHandle.Close(); err != nil {
		return domain.ErrFlushToStorage{ErrorOnClose: err}
	}

	return nil
}

// ReadFileStream implements domain.Storage.
func (d *Storage) ReadFileStream(filename string, mode os.FileMode) (io.ReadCloser, error) {
	return os.OpenFile(filename, os.O_RDONLY, mode)
}

func (d *Storage) rename(oldPath string, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func (d *Storage) writeFileLines(filename string, lines [][]byte, mode os.FileMode) error {
	stream, err := d.writeFileStream(filename, mode)
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

func (d *Storage) writeFileStream(filename string, mode os.FileMode) (io.WriteCloser, error) {
	return os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
}

// Remove implements domain.Storage.
func (d *Storage) Remove(filename string) error {
	return os.Remove(filename)
}
