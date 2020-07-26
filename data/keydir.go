package data

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
