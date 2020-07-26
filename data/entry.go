package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/Panda-Home/bitcask/utils"
)

// Entry ...
type Entry struct {
	// header
	Checksum  uint32
	Timestamp uint64
	KeySize   uint32
	ValueSize uint32
	// body
	Key   []byte
	Value []byte
}

// NewEntry ...
func NewEntry(key, value []byte) (*Entry, error) {
	if len(key) == 0 {
		return nil, errors.New("Key cannot be empty")
	}
	return &Entry{
		Timestamp: utils.MakeTimestampInMS(),
		KeySize:   uint32(len(key)),
		Key:       key,
		ValueSize: uint32(len(value)),
		Value:     value,
	}, nil
}

// Dump serializes Entry struct to byte array
func (entry *Entry) Dump() ([]byte, error) {
	entryBytes := make([]byte, 32+64+32+32+entry.KeySize+entry.ValueSize)
	binary.BigEndian.PutUint64(entryBytes[32:], entry.Timestamp)
	binary.BigEndian.PutUint32(entryBytes[96:], entry.KeySize)
	binary.BigEndian.PutUint32(entryBytes[128:], entry.ValueSize)
	copy(entryBytes[160:], entry.Key)
	copy(entryBytes[160+entry.KeySize:], entry.Value)
	entry.Checksum = crc32.ChecksumIEEE(entryBytes[32:])
	binary.BigEndian.PutUint32(entryBytes, entry.Checksum)
	return entryBytes, nil
}

// LoadFromBytes converts byte array to Entry struct
func LoadFromBytes(entryBytes []byte) (*Entry, error) {
	if !ValidateEntry(entryBytes) {
		return nil, fmt.Errorf("Invalid entry bytes: %v", entryBytes)
	}

	keySize := binary.BigEndian.Uint32(entryBytes[96:128])
	valueSize := binary.BigEndian.Uint32(entryBytes[128:160])
	return &Entry{
		Checksum:  binary.BigEndian.Uint32(entryBytes[:32]),
		Timestamp: binary.BigEndian.Uint64(entryBytes[32:96]),
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       entryBytes[160 : 160+keySize],
		Value:     entryBytes[160+keySize:],
	}, nil
}

// LoadFromFile reads one entry from given file at given position
func LoadFromFile(f *os.File, pos int64) (*Entry, error) {
	_, err := f.Seek(pos, 0)
	if err != nil {
		return nil, err
	}

	crc, err := readBytesFromFile(f, 32)
	if err != nil {
		return nil, err
	}
	ts, err := readBytesFromFile(f, 64)
	if err != nil {
		return nil, err
	}
	keySize, err := readBytesFromFile(f, 32)
	if err != nil {
		return nil, err
	}
	valueSize, err := readBytesFromFile(f, 32)
	if err != nil {
		return nil, err
	}
	ksInt := binary.BigEndian.Uint32(keySize)
	vsInt := binary.BigEndian.Uint32(valueSize)
	key, err := readBytesFromFile(f, int(ksInt))
	if err != nil {
		return nil, err
	}
	value, err := readBytesFromFile(f, int(vsInt))
	if err != nil {
		return nil, err
	}
	if !ValidateEntry(bytes.Join([][]byte{crc, ts, keySize, valueSize, key, value}, []byte{})) {
		// broken file
		return nil, errors.New("Broken entry")
	}
	return &Entry{
		Checksum:  binary.BigEndian.Uint32(crc),
		Timestamp: binary.BigEndian.Uint64(ts),
		KeySize:   ksInt,
		ValueSize: vsInt,
		Key:       key,
		Value:     value,
	}, nil
}

// ValidateEntry validates if an entry byte array is correct
func ValidateEntry(entryBytes []byte) bool {
	// byte array should be longer than smallest struct size(160)
	// plus minimal key size(1)
	if len(entryBytes) < 160+1 {
		return false
	}

	checksum := binary.BigEndian.Uint32(entryBytes[:32])
	if checksum != crc32.ChecksumIEEE(entryBytes[32:]) {
		return false
	}

	keySize := binary.BigEndian.Uint32(entryBytes[96:128])
	valueSize := binary.BigEndian.Uint32(entryBytes[128:160])
	if keySize+valueSize != uint32(len(entryBytes[160:])) {
		return false
	}

	return true
}

func readBytesFromFile(f *os.File, length int) ([]byte, error) {
	bytes := make([]byte, length)
	n, err := f.Read(bytes)
	if err != nil {
		return nil, err
	}
	if n != length {
		return nil, errors.New("Read wrong length of bytes")
	}
	return bytes, nil
}
