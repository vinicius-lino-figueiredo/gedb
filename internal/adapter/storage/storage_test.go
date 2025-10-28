package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	testLine = "some data"
)

type osOptsMock struct {
	mock.Mock
	mockCount int
}

// IsNotExist implements osOps.
func (o *osOptsMock) IsNotExist(err error) bool {
	if o.mockCount > 0 {
		o.mockCount--
		return os.IsNotExist(err)
	}
	return o.Called(err).Bool(0)
}

// MkdirAll implements osOps.
func (o *osOptsMock) MkdirAll(path string, perm os.FileMode) error {
	if o.mockCount > 0 {
		o.mockCount--
		return os.MkdirAll(path, perm)
	}
	return o.Called(path, perm).Error(0)
}

// OpenFile implements osOps.
func (o *osOptsMock) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if o.mockCount > 0 {
		o.mockCount--
		return os.OpenFile(name, flag, perm)
	}
	call := o.Called(name, flag, perm)
	return call.Get(0).(*os.File), call.Error(1)
}

// Remove implements osOps.
func (o *osOptsMock) Remove(name string) error {
	if o.mockCount > 0 {
		o.mockCount--
		return os.Remove(name)
	}
	return o.Called(name).Error(0)
}

// Rename implements osOps.
func (o *osOptsMock) Rename(oldpath string, newpath string) error {
	if o.mockCount > 0 {
		o.mockCount--
		return os.Rename(oldpath, newpath)
	}
	return o.Called(oldpath, newpath).Error(0)
}

// Stat implements osOps.
func (o *osOptsMock) Stat(name string) (os.FileInfo, error) {
	if o.mockCount > 0 {
		o.mockCount--
		return os.Stat(name)
	}
	call := o.Called(name)
	if call.Get(0) == nil {
		return nil, call.Error(1)
	}
	return call.Get(0).(os.FileInfo), call.Error(1)
}

// WriteFile implements osOps.
func (o *osOptsMock) WriteFile(name string, data []byte, perm os.FileMode) error {
	if o.mockCount > 0 {
		o.mockCount--
		return os.WriteFile(name, data, perm)
	}
	return o.Called(name, data, perm).Error(0)
}

type StorageTestSuite struct {
	suite.Suite
	store *Storage
}

func (s *StorageTestSuite) SetupTest() {
	s.store = NewStorage().(*Storage)
}

// Will append to existent file.
func (s *StorageTestSuite) TestAppendExistentFile() {

	file := s.ExistentFile(s.T())

	i, err := s.store.AppendFile(file, 0666, []byte(testLine))
	s.NoError(err)
	s.Equal(len(testLine), i)
	s.FileExists(file)
	bytes, err := os.ReadFile(file)
	s.NoError(err)
	s.Equal([]byte(testLine), bytes)
}

// Will append to non-empty file.
func (s *StorageTestSuite) TestAppendNonEmptyFile() {

	file := s.NonEmptyFile(s.T())

	i, err := s.store.AppendFile(file, 0666, []byte(testLine))
	s.NoError(err)
	s.Equal(len(testLine), i)
	s.FileExists(file)
	bytes, err := os.ReadFile(file)
	s.NoError(err)
	s.Equal("123\n"+testLine, string(bytes))
}

// Will create file if it does not exist.
func (s *StorageTestSuite) TestAppendNonExistentFile() {
	file := s.NonexistentFile(s.T())

	i, err := s.store.AppendFile(file, 0666, []byte(testLine))
	s.NoError(err)
	s.Equal(len(testLine), i)
	s.FileExists(file)
	bytes, err := os.ReadFile(file)
	s.NoError(err)
	s.Equal([]byte(testLine), bytes)
}

// Will fail to append to read-only file.
func (s *StorageTestSuite) TestAppendReadOnlyFile() {

	file := s.ReadOnlyFile(s.T())

	i, err := s.store.AppendFile(file, 0666, []byte(testLine))
	s.Error(err)
	s.Zero(i)

}

func (s *StorageTestSuite) TestCrashSafeWriteNoError() {
	lines := [][]byte{
		[]byte("abc123"),
		[]byte("abc234"),
		[]byte("abc345"),
		[]byte("abc456"),
		[]byte("abc567"),
	}
	expected := append(bytes.Join(lines, []byte("\n")), '\n')

	file := s.ExistentFile(s.T())

	err := s.store.CrashSafeWriteFileLines(file, lines, 0666, 0666)
	s.NoError(err)

	s.FileExists(file)

	b, err := os.ReadFile(file)
	s.NoError(err)

	s.Equal(expected, b)

}

func (s *StorageTestSuite) TestCrashSafeWriteInvalidDir() {
	invalidDir := filepath.Join(s.T().TempDir(), "invalid", "dir")
	s.NoError(os.MkdirAll(invalidDir, 0777))
	s.NoError(os.Chmod(filepath.Dir(invalidDir), 0000))
	defer s.NoError(os.Chmod(filepath.Dir(invalidDir), 0777))

	lines := [][]byte{[]byte("abc123")}
	err := s.store.CrashSafeWriteFileLines(invalidDir, lines, 0666, 0666)
	s.Error(err)
}

func (s *StorageTestSuite) TestCrashSafeWriteReadOnlyFile() {

	file := s.ReadOnlyFile(s.T())

	lines := [][]byte{[]byte("abc123")}
	err := s.store.CrashSafeWriteFileLines(file, lines, 0666, 0666)
	s.Error(err)
}

// Will not overwrite data if program crashes during crash safe write.
func (s *StorageTestSuite) TestCrashSafeWrite() {

	dir := s.T().TempDir()
	file := filepath.Join(dir, "crashable.txt")

	first := s.MakeCrashableTest("first", file, "somedata_first", 0)
	s.Run("first_write", first)

	second := s.MakeCrashableTest("second", file, "somedata_second", 0)
	s.Run("rewrite", second)

	third := s.MakeCrashableTest("third", file, "somedata_second", time.Millisecond)
	s.Run("rewrite_and_crash", third)

}

func (s *StorageTestSuite) MakeCrashableTest(name, file, lineVal string, crash time.Duration) func() {
	return func() {
		cmd := exec.Command(
			"go",
			"run",
			"../../../test_lac/storage/crash.go",
			name,
			file,
		)
		s.NoError(cmd.Start())
		if crash > 0 {
			time.Sleep(crash)
			s.NoError(cmd.Process.Kill())
			s.Error(cmd.Wait())
			s.False(cmd.ProcessState.Exited())
		} else {
			s.NoError(cmd.Wait())
		}

		s.FileExists(file)
		b, err := os.ReadFile(file)
		s.NoError(err)
		lines := bytes.Split(b, []byte("\n"))

		s.Len(lines, 50001)
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			if !s.Equal(lineVal, string(line)) {
				break
			}
		}
	}
}

// Will return error if parent exists, but is a file and not a directory.
func (s *StorageTestSuite) TestCrashSafeWriteFileLinesInaccessibleFile() {

	dir := filepath.Join(s.T().TempDir(), "notadir.txt")
	file := filepath.Join(dir, "file.txt")

	filemode := os.FileMode(0666)
	s.NoError(os.WriteFile(dir, nil, filemode))

	lines := [][]byte{[]byte("abc123")}
	err := s.store.CrashSafeWriteFileLines(file, lines, filemode, filemode)
	s.Error(err)
}

func (s *StorageTestSuite) TestCrashSafeWriteFileLinesInaccessibleTempFile() {

	dir := s.T().TempDir()
	file := filepath.Join(dir, "noaccesstemp.txt")
	s.NoError(os.WriteFile(file+"~", nil, 0000))

	lines := [][]byte{[]byte("abc123")}
	err := s.store.CrashSafeWriteFileLines(file, lines, 0666, 0666)
	s.Error(err)
}

func (s *StorageTestSuite) TestCrashSafeWriteFileLinesFailWriteTempFileLines() {
	lines := [][]byte{[]byte("abc123")}

	om := &osOptsMock{mockCount: 3}
	s.store.osOpts = om

	file := filepath.Join(s.T().TempDir(), "file")
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	om.On("OpenFile", file+"~", flags, os.FileMode(0666)).
		Return((*os.File)(nil), fmt.Errorf("error")).
		Once()
	err := s.store.CrashSafeWriteFileLines(file, lines, 0666, 0666)
	s.Error(err)
	om.AssertExpectations(s.T())
}

func (s *StorageTestSuite) TestCrashSafeWriteFileLinesFailFlushingTempFile() {
	lines := [][]byte{[]byte("abc123")}

	om := &osOptsMock{mockCount: 4}
	s.store.osOpts = om

	file := filepath.Join(s.T().TempDir(), "file")
	om.On("OpenFile", file+"~", os.O_RDWR, os.FileMode(0666)).
		Return((*os.File)(nil), fmt.Errorf("error")).
		Once()

	err := s.store.CrashSafeWriteFileLines(file, lines, 0666, 0666)
	s.Error(err)
	om.AssertExpectations(s.T())
}

func (s *StorageTestSuite) TestCrashSafeWriteFileLinesFailRenaming() {
	lines := [][]byte{[]byte("abc123")}

	om := &osOptsMock{mockCount: 5}
	s.store.osOpts = om

	file := filepath.Join(s.T().TempDir(), "file")
	om.On("Rename", file+"~", file).
		Return(fmt.Errorf("error")).
		Once()

	err := s.store.CrashSafeWriteFileLines(file, lines, 0666, 0666)
	s.Error(err)
	om.AssertExpectations(s.T())
}

func (s *StorageTestSuite) TestCrashSafeWriteFileLinesFailFlushingRenamed() {
	lines := [][]byte{[]byte("abc123")}

	om := &osOptsMock{mockCount: 6}
	s.store.osOpts = om

	file := filepath.Join(s.T().TempDir(), "file")
	om.On("OpenFile", file, os.O_RDONLY, os.FileMode(0666)).
		Return((*os.File)(nil), fmt.Errorf("error")).
		Once()

	err := s.store.CrashSafeWriteFileLines(file, lines, 0666, 0666)
	s.Error(err)
	om.AssertExpectations(s.T())
}

func (s *StorageTestSuite) TestCrashSafeWriteFileLiensForbiddenDir() {
	dir := s.T().TempDir()
	dir = filepath.Join(dir, "forbidden")
	s.NoError(os.Mkdir(dir, 0000))
	defer os.Remove(dir)

	file := filepath.Join(dir, "file.txt")

	err := s.store.CrashSafeWriteFileLines(file, nil, 0666, 0666)
	s.Error(err)
}

// Will not get any error when ensuring integrity of existing file.
func (s *StorageTestSuite) TestEnsureDatafileIntegrityExistingFile() {
	file := s.ExistentFile(s.T())
	s.NoError(s.store.EnsureDatafileIntegrity(file, 0000))
}

// Will not get any error when ensuring integrity of existing file.
func (s *StorageTestSuite) TestEnsureDatafileIntegrityNonExistingFile() {
	file := s.NonexistentFile(s.T())

	s.NoFileExists(file)
	s.NoError(s.store.EnsureDatafileIntegrity(file, 0000))
	s.FileExists(file)
}

func (s *StorageTestSuite) TestEnsureDatafileIntegrityNonExistingFileExistingTemp() {

	dir := s.T().TempDir()
	file := filepath.Join(dir, "primbutnomain.txt")
	s.NoError(os.WriteFile(file+"~", nil, 0666))

	s.NoFileExists(file)
	s.FileExists(file + "~")

	s.NoError(s.store.EnsureDatafileIntegrity(file, 0000))

	s.FileExists(file)
	s.NoFileExists(file + "~")
}

func (s *StorageTestSuite) TestEnsureDatafileIntegrityFailCheckingPrimFile() {
	file := s.ExistentFile(s.T())

	om := new(osOptsMock)
	s.store.osOpts = om

	om.On("Stat", mock.Anything).Return(nil, fmt.Errorf("error")).Once()
	om.On("IsNotExist", mock.Anything).Return(false).Once()
	s.Error(s.store.EnsureDatafileIntegrity(file, 0000))
}

func (s *StorageTestSuite) TestEnsureDatafileIntegrityFailCheckingTempFile() {
	file := s.NonexistentFile(s.T())

	om := &osOptsMock{mockCount: 2}
	s.store.osOpts = om

	om.On("Stat", mock.Anything).Return(nil, fmt.Errorf("error")).Once()
	om.On("IsNotExist", mock.Anything).Return(false).Once()
	s.Error(s.store.EnsureDatafileIntegrity(file, 0000))
}

func (s *StorageTestSuite) TestEnsureParentDirectoryExistsExistingDir() {
	file := s.ExistentFile(s.T())
	s.NoError(s.store.EnsureParentDirectoryExists(file, 0000))
}

func (s *StorageTestSuite) TestEnsureParentDirectoryExistsFailAbs() {
	om := new(osOptsMock)
	s.store.osOpts = om

	back, err := os.Getwd()
	if err != nil {
		s.FailNow("cannot continue test without pwd")
	}

	dir := filepath.Join(s.T().TempDir(), "willbedeleted")
	abs, err := filepath.Abs(dir)
	s.NoError(err)

	s.NoError(os.Mkdir(dir, 0777))
	s.NoError(os.Chmod(dir, 0777))

	s.NoError(os.Chdir(dir))
	defer func() { s.NoError(os.Chdir(back)) }()
	s.NoError(os.Remove(abs))

	s.Error(s.store.EnsureParentDirectoryExists("subdir", 0000))
	om.AssertExpectations(s.T())

}

func (s *StorageTestSuite) TestflushToStorageFailFileSync() {
	file := s.ExistentFile(s.T())

	om := new(osOptsMock)
	s.store.osOpts = om

	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		s.FailNow("need a open file to continue the test")
	}

	s.NoError(f.Close())

	om.On("OpenFile", file, os.O_RDWR, os.FileMode(0666)).
		Return(f, nil)

	s.Error(s.store.flushToStorage(file, false, 0666))

}

func (s *StorageTestSuite) TestReadFileStream() {
	existentFile := s.ExistentFile(s.T())
	nonEmptyFile := s.NonEmptyFile(s.T())
	nonExistentFile := s.NonexistentFile(s.T())

	nonEmpty, err := s.store.ReadFileStream(nonEmptyFile, 0666)
	s.NoError(err)
	defer nonEmpty.Close()
	b, err := io.ReadAll(nonEmpty)
	s.NoError(err)
	s.Equal([]byte("123\n"), b)

	empty, err := s.store.ReadFileStream(existentFile, 0666)
	s.NoError(err)
	defer empty.Close()
	b, err = io.ReadAll(empty)
	s.NoError(err)
	s.Equal([]byte(""), b)

	nonexistent, err := s.store.ReadFileStream(nonExistentFile, 0666)
	s.Error(err)
	s.Nil(nonexistent)
}

func (s *StorageTestSuite) TestWriteFileLinesErrorWriting() {
	file := s.ExistentFile(s.T())

	om := new(osOptsMock)
	s.store.osOpts = om

	flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC

	f, err := os.OpenFile(file, flag, 0666)
	if err != nil {
		s.FailNow("need a open file to continue the test")
	}
	s.NoError(f.Close())

	om.On("OpenFile", file, flag, os.FileMode(0666)).
		Return(f, nil).
		Once()

	lines := [][]byte{
		[]byte("hello world"),
	}
	s.Error(s.store.writeFileLines(file, lines, 0666))
	om.AssertExpectations(s.T())
}

func (s *StorageTestSuite) TestRemove() {
	existent := s.ExistentFile(s.T())
	nonexistent := s.NonexistentFile(s.T())

	s.NoError(s.store.Remove(existent))
	s.Error(s.store.Remove(nonexistent))
}

func (s *StorageTestSuite) ExistentFile(t *testing.T) string {
	return s.CreateFile(t, nil, 0666)
}

func (s *StorageTestSuite) NonEmptyFile(t *testing.T) string {
	return s.CreateFile(t, []byte("123\n"), 0666)
}

func (s *StorageTestSuite) NonexistentFile(t *testing.T) string {
	dir := t.TempDir()
	return filepath.Join(dir, "nonexistent.txt")
}

func (s *StorageTestSuite) ReadOnlyFile(t *testing.T) string {
	return s.CreateFile(t, nil, 0444)
}

func (s *StorageTestSuite) CreateFile(t *testing.T, content []byte, mode os.FileMode) string {
	dir := t.TempDir()
	file := filepath.Join(dir, "existent.txt")
	if !s.NoError(os.WriteFile(file, content, mode)) {
		s.FailNow("could not create file")
	}
	return file
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}
