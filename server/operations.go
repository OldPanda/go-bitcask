package server

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

	"github.com/Panda-Home/bitcask/data"
	"github.com/Panda-Home/bitcask/utils"
)

// Set sets a new key value pair to the in-memory structure
// as well as persists them into log file
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
