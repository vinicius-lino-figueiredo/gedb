//go:build windows

package storage

import (
	"os"
	"path/filepath"
)

func init() {
	osSpecificEnsureDir = func(o osOps, dir string, mode os.FileMode) error {
		root := filepath.VolumeName(dir) + string(os.PathSeparator)
		if dir != root || filepath.Base(dir) != "" {
			return o.MkdirAll(dir, mode)
		}
		return nil
	}

	osSpecificSync = func(f *os.File, isDir bool) error {
		if isDir {
			return nil
		}
		return f.Sync()
	}
}
