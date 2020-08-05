package config

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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testConfigFile       = "/tmp/test_config.json"
	testBrokenConfigFile = "/tmp/test_broken_config.json"
)

func Test_NewBitcaskConfig(t *testing.T) {
	prepareConfigFile()
	defer cleanupFile()

	_, err := NewBitcaskConfig("/tmp/non-exist-file")
	assert.Error(t, err, "Expected an error when opening non-exist file")

	_, err = NewBitcaskConfig(testBrokenConfigFile)
	assert.Error(t, err, "Expected an error on broken json config")

	c, err := NewBitcaskConfig(testConfigFile)
	assert.Nil(t, err, "Expected no error on normal config file")
	assert.Equal(t, "localhost", c.Host, fmt.Sprintf("Expected host: %s, got: %s", "localhost", c.Host))
	assert.Equal(t, 9876, c.Port, fmt.Sprintf("Expected port: %d, got: %d", 9876, c.Port))
	assert.Equal(t, "/usr/local/var/run/bitcask.pid", c.PidFile, fmt.Sprintf("Expected pidfile: %s, got: %s", "/usr/local/var/run/bitcask.pid", c.PidFile))
	assert.Equal(t, "/usr/local/var/bitcask", c.DataDir, fmt.Sprintf("Expected data directory: %s, got: %s", "/usr/local/var/bitcask", c.DataDir))
	assert.Equal(t, 1, c.DataSize, fmt.Sprintf("Expected data size (MB): %d, got: %d", 1, c.DataSize))
	assert.Equal(t, 10, c.MergeFreq, fmt.Sprintf("Expected merger frequency (second): %d, got: %d", 10, c.MergeFreq))
}

func prepareConfigFile() {
	f, _ := os.OpenFile(testConfigFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	defer f.Close()

	f.Write([]byte(`{
	"host": "localhost",
	"port": 9876,
	"pidfile": "/usr/local/var/run/bitcask.pid",
	"data_directory": "/usr/local/var/bitcask",
	"data_filesize_in_mb": 1,
	"merge_frequency_in_seconds": 10
}`))

	brokenF, _ := os.OpenFile(testBrokenConfigFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	defer brokenF.Close()

	brokenF.Write([]byte(`{
	"host": "localhost",
	"port": 9876,
	"pidfile": "/usr/local/var/run/bitcask.pid"
	"data_directory": "/usr/local/var/bitcask",
	"data_filesize_in_mb": 1,
	"merge_frequency_in_seconds": 10`))
}

func cleanupFile() {
	os.Remove(testConfigFile)
	os.Remove(testBrokenConfigFile)
}
