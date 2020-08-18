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
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var fakeDir = "/tmp/fake_dir"

func Test_NewLogger(t *testing.T) {
	defer cleanup()
	_, err := NewLogger("", 1, false)
	assert.Error(t, err, "Expected an error when not given log directory")

	_, err = NewLogger(fakeDir, -1, false)
	assert.Error(t, err, "Expected an error on negative file size")

	logger, err := NewLogger(fakeDir, 1, false)
	defer logger.Close()
	assert.Nil(t, err, "Expected no error on log file creation")
	assert.Equal(t, fakeDir, logger.Dirpath, fmt.Sprintf("Expected log directory path: %s, got: %s", fakeDir, logger.Dirpath))
	assert.Equal(t, 1, logger.MaxSize, fmt.Sprintf("Expected file size limit: %d MB, got: %d MB", 1, logger.MaxSize))
	assert.Equal(t, false, logger.isMerge, "Expected not in merge mode")
	assert.Equal(t, int64(1048576), logger.maxSize(), fmt.Sprintf("Expected file max size in bytes: %d, got: %d", int64(1048576), logger.maxSize()))
	assert.Equal(t, int64(0), logger.curFilePos, fmt.Sprintf("Expected pointing at the file head, but pointing at %d", logger.curFilePos))
}

func Test_Write(t *testing.T) {
	defer cleanup()

	logger, err := NewLogger(fakeDir, 1, false)
	defer logger.Close()
	assert.Nil(t, err, "Expected no error on log file creation")
	n, err := logger.Write([]byte("Hello world!"))
	assert.Nil(t, err, "Expected no error on file writing")
	assert.Equal(t, 12, n, fmt.Sprintf("Expected returned value: %d, got: %d", 12, n))
	assert.Equal(t, int64(12), logger.curFilePos, fmt.Sprintf("Expected pointing at %d, but pointing at %d", int64(12), logger.curFilePos))

	// Write 1MB to trigger log rotate
	for i := 0; i < 1024*32; i++ {
		_, err := logger.Write([]byte("ca73fb4f5c5bbe26f9de9d97c1f3d0d4")) // byte array with len 32
		assert.Nil(t, err, "Expected no error on file writing")
	}
	files, _ := ioutil.ReadDir(fakeDir)
	assert.Equal(t, 2, len(files), fmt.Sprintf("Expected there're %d files in test directory, got: %d", 2, len(files)))
}

func Test_SeekLog(t *testing.T) {
	defer cleanup()

	logger, err := NewLogger(fakeDir, 1, false)
	defer logger.Close()
	assert.Nil(t, err, "Expected no error on log file creation")
	_, err = logger.Write([]byte("Hello world!"))
	assert.Nil(t, err, "Expected no error on file writing")
	err = logger.SeekLog(int64(10))
	assert.Nil(t, err, "Expected no error on seeking to a head of EOF")
}

func cleanup() {
	os.RemoveAll(fakeDir)
}
