// Package bitlog implements data structure and functions
// to manage Bitcask log files. By log files it means the
// files contains data represent both running log and data,
// which is how Bitcask is designed.
package bitlog

// The following code was sourced and modified from the
// https://github.com/Panda-Home/go-bitcask package
// governed by the following license:
//
// Copyright (c) 2020 Panda-Home
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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

// Logger ...
type Logger struct {
	Dirpath string
	MaxSize int // megabytes

	filepath    string
	fileHandler *os.File
	curFilePos  int64
	isMerge     bool // to tell if this is to build merged files

	mu sync.Mutex
}

const megabyte = 1024 * 1024

// NewLogger ...
func NewLogger(dirpath string, maxSize int, isMerge bool) (*Logger, error) {
	if len(dirpath) == 0 {
		return nil, errors.New("Filename cannot be empty")
	}
	if maxSize <= 0 {
		return nil, errors.New("File max size must be positive integer")
	}

	l := &Logger{
		Dirpath: dirpath,
		MaxSize: maxSize,
		isMerge: isMerge,
	}
	l.filepath = l.newFilepath()
	if !l.isMerge {
		availableFile, _ := l.findLatestAvailableFile()
		if availableFile != "" {
			l.filepath = availableFile
		}
	}

	if err := l.openFile(); err != nil {
		return nil, err
	}

	return l, nil
}

// Write outputs given byte array to active file.
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

// Close ...
func (l *Logger) Close() {
	l.fileHandler.Close()
}

// SeekLog moves file handler to given pos relative to
// the origin of the file.
func (l *Logger) SeekLog(pos int64) error {
	_, err := l.fileHandler.Seek(pos, 0)
	return err
}

// ActiveFilepath returns the path to active log file.
func (l *Logger) ActiveFilepath() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.filepath
}

// ActiveFilePos returns the current position of
// active file.
func (l *Logger) ActiveFilePos() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.curFilePos
}

// GetFileTS returns the timestamp suffix of given log file
func (l *Logger) GetFileTS(filePath string) (uint64, error) {
	components := strings.Split(filePath, ".")
	tsPart := components[len(components)-1]
	i, err := strconv.Atoi(tsPart)
	if err != nil {
		return 0, fmt.Errorf("Not a valid integer: %s", err)
	}
	return uint64(i), nil
}

func (l *Logger) newFilepath() string {
	ts := utils.MakeTimestampInMS()
	tsStr := strconv.FormatUint(ts, 10)
	if !l.isMerge {
		return filepath.Join(l.Dirpath, "data.bit."+tsStr)
	}
	return filepath.Join(l.Dirpath, "data.bit.merged."+tsStr)
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
	}
	f, err := os.OpenFile(l.filepath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("Can't open logfile: %s", err)
	}
	l.fileHandler = f
	l.curFilePos = info.Size()
	return nil
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

// maxSize returns the file's max allowed size in bytes.
func (l *Logger) maxSize() int64 {
	return int64(l.MaxSize) * megabyte
}

// Find the unmerged log file with latest timestamp, and
// check if it reaches the max size. The file will be used
// as active file if it doesn't.
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
		if strings.HasPrefix(filename, "data.bit.") &&
			!strings.HasPrefix(filename, "data.bit.merged.") {
			if f.Size() >= l.maxSize() {
				return "", nil
			}
			return filepath.Join(l.Dirpath, f.Name()), nil
		}
	}
	return "", errors.New("No data file is found")
}
