package server

import (
	"fmt"
	"os"

	"github.com/Panda-Home/bitcask/data"
	"github.com/Panda-Home/bitcask/utils"
)

func (s *Server) Set(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.setKeyValue(key, value)
}

func (s *Server) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.keyDir.GetValue(key)
	if err != nil {
		return nil, err
	}

	value, err := readValueFromFile(entry.FileID, entry.ValuePos, entry.ValueSize)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *Server) Del(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Set nil value in file
	err := s.setKeyValue(key, []byte(nil))
	if err != nil {
		return err
	}
	// Then delete key from in-memory structure
	err = s.keyDir.DelKeydirEntry(key)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) setKeyValue(key, value []byte) error {
	entry, err := data.NewEntry(key, value)
	if err != nil {
		return err
	}

	entryBytes, err := entry.Dump()
	if err != nil {
		return err
	}

	curPos := s.logFile.ActiveFilePos()
	s.keyDir.SetEntryFromByteArray(s.logFile.ActiveFilepath(), curPos, entryBytes)
	_, err = s.logFile.Write(entryBytes)
	if err != nil {
		s.keyDir.DelKeydirEntry(key) // remove the new added key
		s.logFile.SeekLog(curPos)    // move the file handler back to original pos
		return fmt.Errorf("Failed set key value pair: %s", err)
	}

	return nil
}

func readValueFromFile(filepath string, pos int64, vs uint32) ([]byte, error) {
	fileSize, err := utils.GetFileSize(filepath)
	if err != nil {
		return nil, fmt.Errorf("Can't get file size: %s", err)
	}
	if fileSize < pos {
		return nil, fmt.Errorf("Value pos is out of file size: %s", filepath)
	}

	f, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file: %s", err)
	}
	defer f.Close()

	entry, err := data.LoadFromFile(f, pos)
	if err != nil {
		return nil, fmt.Errorf("Failed to load data from file: %s", err)
	}

	return entry.Value, nil
}
