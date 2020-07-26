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
)

// KeyDir ...
type KeyDir struct {
	dataMap map[string]*KeyDirEntry
}

// KeyDirEntry ...
type KeyDirEntry struct {
	FileID    string
	ValueSize uint32
	ValuePos  int64
	Timestamp uint64
}

// NewKeyDir ...
func NewKeyDir() *KeyDir {
	return &KeyDir{
		dataMap: make(map[string]*KeyDirEntry),
	}
}

// SetEntryFromByteArray set KeyDir entry from entry's byte array
func (dir *KeyDir) SetEntryFromByteArray(fileID string, valuePos int64, entryBytes []byte) error {
	entry, err := LoadFromBytes(entryBytes)
	if err != nil {
		return fmt.Errorf("Failed to create entry from byte array: %s", err)
	}
	dir.dataMap[string(entry.Key)] = &KeyDirEntry{
		FileID:    fileID,
		ValueSize: entry.ValueSize,
		ValuePos:  valuePos,
		Timestamp: entry.Timestamp,
	}
	return nil
}

// SetEntryFromKeyValue sets KeyDir entry given all fields
func (dir *KeyDir) SetEntryFromKeyValue(key []byte, fileID string, valuePos int64, valueSize uint32, ts uint64) error {
	dir.dataMap[string(key)] = &KeyDirEntry{
		FileID:    fileID,
		ValueSize: valueSize,
		ValuePos:  valuePos,
		Timestamp: ts,
	}
	return nil
}

// GetValue ...
func (dir *KeyDir) GetValue(key []byte) (*KeyDirEntry, error) {
	if entry, ok := dir.dataMap[string(key)]; ok {
		return entry, nil
	}
	return nil, fmt.Errorf("Key not found: %s", key)
}

// DelKeydirEntry ...
func (dir *KeyDir) DelKeydirEntry(key []byte) error {
	if _, ok := dir.dataMap[string(key)]; ok {
		delete(dir.dataMap, string(key))
		return nil
	}
	return fmt.Errorf("Key not found: %s", key)
}
