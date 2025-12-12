package storage

import "os"

type osOps interface {
	IsNotExist(err error) bool
	MkdirAll(path string, perm os.FileMode) error
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	Rename(oldpath string, newpath string) error
	Remove(name string) error
	Stat(name string) (os.FileInfo, error)
	WriteFile(name string, data []byte, perm os.FileMode) error
}

type osImpl struct{}

// IsNotExist implements [osOpts].
func (o *osImpl) IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

// MkdirAll implements [osOpts].
func (o *osImpl) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// OpenFile implements [osOpts].
func (o *osImpl) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

// Remove implements [osOpts].
func (o *osImpl) Remove(name string) error {
	return os.Remove(name)
}

// Rename implements [osOpts].
func (o *osImpl) Rename(oldpath string, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// Stat implements [osOpts].
func (o *osImpl) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// WriteFile implements [osOpts].
func (o *osImpl) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}
