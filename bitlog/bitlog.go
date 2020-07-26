package bitlog

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Panda-Home/bitcask/utils"
)

type Logger struct {
	Dirpath string
	MaxSize int // megabytes

	filepath    string
	fileHandler *os.File
	curFilePos  int64

	mu sync.Mutex
}

const megabyte = 1024 * 1024

func NewLogger(dirpath string, maxSize int) (*Logger, error) {
	if len(dirpath) == 0 {
		return nil, errors.New("Filename cannot be empty")
	}
	if maxSize <= 0 {
		return nil, errors.New("File max size must be positive integer")
	}

	l := &Logger{
		Dirpath: dirpath,
		MaxSize: maxSize,
	}
	availableFile, err := l.findLatestAvailableFile()
	if err != nil {
		l.filepath = l.newFilepath()
	} else {
		l.filepath = availableFile
	}

	if err := l.openFile(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Logger) Write(b []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	byteLen := int64(len(b))
	if byteLen+l.curFilePos >= l.maxSize() {
		if err := l.rotate(); err != nil {
			return 0, err
		}
	}

	n, err = l.fileHandler.Write(b)
	if err == nil {
		l.curFilePos += byteLen
	}
	return n, err
}

func (l *Logger) Close() {
	l.fileHandler.Close()
}

// SeekLog moves file handler to given pos relative to
// the origin of the file
func (l *Logger) SeekLog(pos int64) error {
	_, err := l.fileHandler.Seek(pos, 0)
	return err
}

func (l *Logger) ActiveFilepath() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.filepath
}

func (l *Logger) ActiveFilePos() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.curFilePos
}

func (l *Logger) newFilepath() string {
	ts := utils.MakeTimestampInMS()
	tsStr := strconv.FormatUint(ts, 10)
	return filepath.Join(l.Dirpath, "data.bit."+tsStr)
}

func (l *Logger) openFile() error {
	if err := os.MkdirAll(l.Dirpath, 0755); err != nil {
		return fmt.Errorf("Can't create directory: %v", l.Dirpath)
	}

	info, err := os.Stat(l.filepath)
	if os.IsNotExist(err) {
		f, err := os.OpenFile(l.filepath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("Can't open new logfile: %s", err)
		}
		l.fileHandler = f
		l.curFilePos = 0
		return nil
	} else {
		f, err := os.OpenFile(l.filepath, os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("Can't open logfile: %s", err)
		}
		l.fileHandler = f
		l.curFilePos = info.Size()
		return nil
	}
}

func (l *Logger) rotate() error {
	err := l.fileHandler.Close()
	if err != nil {
		return fmt.Errorf("Failed to close logfile: %s", err)
	}

	newFilepath := l.newFilepath()
	f, err := os.OpenFile(newFilepath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("Can't open new logfile: %s", err)
	}
	l.filepath = newFilepath
	l.fileHandler = f
	l.curFilePos = 0
	return nil
}

// returns the file max size in bytes
func (l *Logger) maxSize() int64 {
	return int64(l.MaxSize) * megabyte
}

func (l *Logger) findLatestAvailableFile() (string, error) {
	files, err := ioutil.ReadDir(l.Dirpath)
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", errors.New("Data directory is empty")
	}

	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		_, filename := filepath.Split(f.Name())
		if strings.HasPrefix(filename, "data.bit.") {
			if f.Size() >= l.maxSize() {
				return "", nil
			}
			return filepath.Join(l.Dirpath, f.Name()), nil
		}
	}
	return "", errors.New("No data file is found")
}
