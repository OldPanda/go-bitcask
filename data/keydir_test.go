package data

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var keyDir *KeyDir
var (
	keydirFilePath = "/tmp/bitcask_keydir_test.bit"
)

func Test_NewKeyDir(t *testing.T) {
	keyDir = NewKeyDir()
	assert.NotNil(t, keyDir, "Expected not nil")
}

func Test_SetEntryFromByteArray(t *testing.T) {
	entry, _ := NewEntry(fakeKey, fakeValue)
	bytes, _ := entry.Dump()
	err := keyDir.SetEntryFromByteArray("fakeFileID", int64(0), bytes)
	assert.Nil(t, err, "Expected no error")
	assert.Equal(t, 1, len(keyDir.dataMap), "Expected keydir has one entry")

	keyDirEntry, _ := keyDir.GetValue(fakeKey)
	assert.Equal(t, "fakeFileID", keyDirEntry.FileID, fmt.Sprintf("Expected file ID: %s, got: %s", "fakeFileID", keyDirEntry.FileID))
	assert.Equal(t, int64(0), keyDirEntry.ValuePos, fmt.Sprintf("Expected value position: %d, got: %d", int64(0), keyDirEntry.ValuePos))
	assert.Equal(t, uint32(3), keyDirEntry.ValueSize, fmt.Sprintf("Expected value size: %d, got: %d", uint32(3), keyDirEntry.ValueSize))
}

func Test_SetEntryFromKeyValue(t *testing.T) {
	entry, _ := NewEntry(fakeKey, fakeValue)
	err := keyDir.SetEntryFromKeyValue(entry.Key, "fakeFileID", int64(0), entry.ValueSize, entry.Timestamp)
	assert.Nil(t, err, "Expected no error")
	assert.Equal(t, 1, len(keyDir.dataMap), "Expected keydir has one entry")

	keyDirEntry, _ := keyDir.GetValue(fakeKey)
	assert.Equal(t, "fakeFileID", keyDirEntry.FileID, fmt.Sprintf("Expected file ID: %s, got: %s", "fakeFileID", keyDirEntry.FileID))
	assert.Equal(t, int64(0), keyDirEntry.ValuePos, fmt.Sprintf("Expected value position: %d, got: %d", int64(0), keyDirEntry.ValuePos))
	assert.Equal(t, uint32(3), keyDirEntry.ValueSize, fmt.Sprintf("Expected value size: %d, got: %d", uint32(3), keyDirEntry.ValueSize))
	assert.Equal(t, entry.Timestamp, keyDirEntry.Timestamp, fmt.Sprintf("Expected timestamp: %d, got: %d", entry.Timestamp, keyDirEntry.Timestamp))
}

func Test_GetValue(t *testing.T) {
	entry, _ := NewEntry(fakeKey, fakeValue)
	keyDir.SetEntryFromKeyValue(entry.Key, "fakeFileID", int64(0), entry.ValueSize, entry.Timestamp)
	keyDirEntry, err := keyDir.GetValue(fakeKey)
	assert.Nil(t, err, "Expected no error")
	assert.Equal(t, "fakeFileID", keyDirEntry.FileID, fmt.Sprintf("Expected file ID: %s, got: %s", "fakeFileID", keyDirEntry.FileID))
	assert.Equal(t, int64(0), keyDirEntry.ValuePos, fmt.Sprintf("Expected value position: %d, got: %d", int64(0), keyDirEntry.ValuePos))
	assert.Equal(t, uint32(3), keyDirEntry.ValueSize, fmt.Sprintf("Expected value size: %d, got: %d", uint32(3), keyDirEntry.ValueSize))
	assert.Equal(t, entry.Timestamp, keyDirEntry.Timestamp, fmt.Sprintf("Expected timestamp: %d, got: %d", entry.Timestamp, keyDirEntry.Timestamp))

	nonExistKey := []byte("no-exist")
	_, err = keyDir.GetValue(nonExistKey)
	assert.EqualErrorf(t, err, fmt.Sprintf("Key not found: %s", nonExistKey), "Expected an error on non-exist key")
}

func Test_DelKeydirEntry(t *testing.T) {
	entry, _ := NewEntry(fakeKey, fakeValue)
	keyDir.SetEntryFromKeyValue(entry.Key, "fakeFileID", int64(0), entry.ValueSize, entry.Timestamp)
	err := keyDir.DelKeydirEntry(fakeKey)
	assert.Nil(t, err, "Expected no error")

	nonExistKey := []byte("no-exist")
	err = keyDir.DelKeydirEntry(nonExistKey)
	assert.EqualErrorf(t, err, fmt.Sprintf("Key not found: %s", nonExistKey), "Expected an error on non-exist key")
}

func Test_HasKey(t *testing.T) {
	entry, _ := NewEntry(fakeKey, fakeValue)
	keyDir.SetEntryFromKeyValue(entry.Key, "fakeFileID", int64(0), entry.ValueSize, entry.Timestamp)
	res := keyDir.HasKey(fakeKey)
	assert.True(t, res, "Expected true on existing key")

	nonExistKey := []byte("no-exist")
	res = keyDir.HasKey(nonExistKey)
	assert.False(t, res, "Expected false on non-exist key")
}
