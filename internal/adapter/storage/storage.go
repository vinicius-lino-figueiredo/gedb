package storage

import (
	"io"
	"os"
	"path/filepath"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var osSpecificEnsureDir = func(o osOps, dir string, mode os.FileMode) error {
	// default behavior for ensuring a dir is to always create the dir. On
	// windows, we do not ensure if it is root.
	return o.MkdirAll(dir, mode)
}

var osSpecificSync = func(f *os.File, _ bool) error {
	return f.Sync()
}

// Storage implements domain.Storage.
type Storage struct {
	osOpts osOps
}

// NewStorage returns a new implementation of domain.Storage.
func NewStorage() domain.Storage {
	return &Storage{
		osOpts: &osImpl{},
	}
}

// AppendFile implements domain.Storage.
func (d *Storage) AppendFile(filename string, mode os.FileMode, data []byte) (int, error) {
	f, err := d.osOpts.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, mode)
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
		return d.osOpts.WriteFile(filename, nil, mode)
	}
	return d.osOpts.Rename(tempFilename, filename)
}

// EnsureParentDirectoryExists implements domain.Storage.
func (d *Storage) EnsureParentDirectoryExists(filename string, mode os.FileMode) error {
	dir := filepath.Dir(filename)
	parsedDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	return osSpecificEnsureDir(d.osOpts, parsedDir, mode)
}

// Exists implements domain.Storage.
func (d *Storage) Exists(filename string) (bool, error) {
	_, err := d.osOpts.Stat(filename)
	if err != nil {
		if d.osOpts.IsNotExist(err) {
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

	fileHandle, err := d.osOpts.OpenFile(filename, flags, mode)
	if err != nil {
		return domain.ErrFlushToStorage{ErrorOnFsync: err}
	}

	if err := osSpecificSync(fileHandle, isDir); err != nil {
		return domain.ErrFlushToStorage{ErrorOnFsync: err}
	}

	if err := fileHandle.Close(); err != nil {
		return domain.ErrFlushToStorage{ErrorOnClose: err}
	}

	return nil
}

// ReadFileStream implements domain.Storage.
func (d *Storage) ReadFileStream(filename string, mode os.FileMode) (io.ReadCloser, error) {
	return d.osOpts.OpenFile(filename, os.O_RDONLY, mode)
}

func (d *Storage) rename(oldPath string, newPath string) error {
	return d.osOpts.Rename(oldPath, newPath)
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
	return d.osOpts.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
}

// Remove implements domain.Storage.
func (d *Storage) Remove(filename string) error {
	return d.osOpts.Remove(filename)
}
