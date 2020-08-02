package data

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

	"github.com/Panda-Home/bitcask/utils"
	"github.com/stretchr/testify/assert"
)

var (
	emptyKey      = make([]byte, 0)
	emptyValue    = make([]byte, 0)
	fakeKey       = []byte("foo")
	fakeValue     = []byte("bar")
	entryFilePath = "/tmp/bitcask_entry_test.bit"
)

func Test_NewEntry(t *testing.T) {
	_, err := NewEntry(emptyKey, emptyValue)
	assert.EqualErrorf(t, err, "Key cannot be empty", "Expected an error when given empty key")

	entry, err := NewEntry(fakeKey, fakeValue)
	assert.NoError(t, err, "Expected no error on normal entry creation")
	assert.Equal(t, fakeKey, entry.Key, fmt.Sprintf("Expected key: %s, got: %s", fakeKey, entry.Key))
	assert.Equal(t, fakeValue, entry.Value, fmt.Sprintf("Expected value: %s, got: %s", fakeValue, entry.Value))
	assert.Equal(t, uint32(3), entry.KeySize, "Expected key size: %d, got: %d", 3, entry.KeySize)
	assert.Equal(t, uint32(3), entry.ValueSize, "Expected value size: %d, got: %d", 3, entry.ValueSize)

	curTimestamp := utils.MakeTimestampInMS()
	assert.LessOrEqual(t, entry.Timestamp, curTimestamp, fmt.Sprintf("Entry's timestamp expected to be early than %d", curTimestamp))
}

func Test_Dump(t *testing.T) {
	entry, _ := NewEntry(fakeKey, fakeValue)
	bytes, err := entry.Dump()
	assert.Nil(t, err, "Expected no error")
	assert.Equal(t, 166, len(bytes), "Expected dump byte array length: %d, got: %d", 166, len(bytes))
}

func Test_LoadFromBytes(t *testing.T) {
	entry, _ := NewEntry(fakeKey, fakeValue)
	bytes, _ := entry.Dump()
	brokenBytes := bytes[20:]
	_, err := LoadFromBytes(brokenBytes)
	assert.EqualErrorf(t, err, fmt.Sprintf("Invalid entry bytes: %v", brokenBytes), "Expected error on broken bytes")

	entry2, err := LoadFromBytes(bytes)
	assert.Nil(t, err, "Expected no error")
	assert.Equal(t, entry.Checksum, entry2.Checksum, fmt.Sprintf("Expected checksum: %v, got: %v", entry.Checksum, entry2.Checksum))
	assert.Equal(t, entry.Timestamp, entry2.Timestamp, fmt.Sprintf("Expected timestamp: %d, got: %d", entry.Timestamp, entry2.Timestamp))
	assert.Equal(t, entry.KeySize, entry2.KeySize, fmt.Sprintf("Expected key size: %d, got: %d", entry.KeySize, entry2.KeySize))
	assert.Equal(t, entry.ValueSize, entry2.ValueSize, fmt.Sprintf("Expected value size: %d, got: %d", entry.ValueSize, entry2.ValueSize))
	assert.Equal(t, entry.Key, entry2.Key, fmt.Sprintf("Expected key: %s, got: %s", entry.Key, entry2.Key))
	assert.Equal(t, entry.Value, entry2.Value, fmt.Sprintf("Expected key: %s, got: %s", entry.Value, entry2.Value))
}

func Test_LoadFromFile(t *testing.T) {
	prepareFile()
	defer cleanupFile()

	f, _ := os.OpenFile(entryFilePath, os.O_RDONLY, 0644)
	entry, err := LoadFromFile(f, int64(0))
	assert.Nil(t, err, "Expected no error")
	assert.Equal(t, fakeKey, entry.Key, fmt.Sprintf("Expected key: %s, got: %s", fakeKey, entry.Key))
	assert.Equal(t, fakeValue, entry.Value, fmt.Sprintf("Expected value: %s, got: %s", fakeValue, entry.Value))
	assert.Equal(t, uint32(len(fakeKey)), entry.KeySize, fmt.Sprintf("Expected key size: %d, got: %d", len(fakeKey), entry.KeySize))
	assert.Equal(t, uint32(len(fakeValue)), entry.ValueSize, fmt.Sprintf("Expected value size: %d, got: %d", len(fakeValue), entry.ValueSize))

	_, err = LoadFromFile(f, int64(1024))
	assert.Error(t, err, "Expected error when reading beyond EOF")
}

func Test_ValidateEntry(t *testing.T) {
	entry, _ := NewEntry(fakeKey, fakeValue)
	bytes, _ := entry.Dump()
	res := ValidateEntry(bytes)
	assert.True(t, res, "Expected true on correct entry bytes")

	brokenBytes := bytes[:]
	brokenBytes[0] = 123
	res = ValidateEntry(brokenBytes)
	assert.False(t, res, "Expected false on broken bytes")

	emptyBytes := make([]byte, 0)
	res = ValidateEntry(emptyBytes)
	assert.False(t, res, "Expected false on empty bytes")
}

func prepareFile() {
	entry, _ := NewEntry(fakeKey, fakeValue)
	bytes, _ := entry.Dump()

	f, _ := os.OpenFile(entryFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	defer f.Close()
	f.Write(bytes)
}

func cleanupFile() {
	os.Remove(entryFilePath)
}
