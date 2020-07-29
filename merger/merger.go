package merger

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Panda-Home/bitcask/bitlog"
	"github.com/Panda-Home/bitcask/config"
	"github.com/Panda-Home/bitcask/data"
	"github.com/Panda-Home/bitcask/server"
	"github.com/Panda-Home/bitcask/utils"
)

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

type Merger struct {
	dirPath   string
	fileSize  int
	logFile   *bitlog.Logger
	quit      chan interface{}
	frequency int

	server *server.Server

	wg sync.WaitGroup
}

func NewMerger(c *config.BitcaskConfig, s *server.Server) (*Merger, error) {
	info, err := os.Stat(c.DataDir)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("Data directory doesn't exist: %s", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("Not a directory: %s", c.DataDir)
	}

	m := &Merger{
		dirPath:   c.DataDir,
		fileSize:  c.DataSize,
		quit:      make(chan interface{}),
		server:    s,
		frequency: c.MergeFreq,
	}
	m.wg.Add(1)
	go m.merge()
	return m, nil
}

func (m *Merger) Stop() {
	close(m.quit)
	m.wg.Wait()
}

func (m *Merger) merge() {
	defer m.wg.Done()

	ticker := time.NewTicker(time.Duration(m.frequency) * time.Second)
	for {
		select {
		case <-m.quit:
			return
		case <-ticker.C:
			m.mergeOldFiles()
		}
	}
}

func (m *Merger) mergeOldFiles() error {
	files, err := ioutil.ReadDir(m.dirPath)
	if err != nil {
		return fmt.Errorf("Failed to list directory: %s", err)
	}

	utils.SortLogFiles(files)

	logFiles := make([]os.FileInfo, 0) // data files to be merged
	oldDataFilesCount := 0
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if strings.HasPrefix(f.Name(), "data.bit.") &&
			filepath.Join(m.dirPath, f.Name()) != m.server.GetActiveFile() {
			logFiles = append(logFiles, f)
			if !strings.HasPrefix(f.Name(), "data.bit.merged") {
				oldDataFilesCount++
			}
		}
	}

	if oldDataFilesCount == 0 {
		return nil
	}

	logFile, err := bitlog.NewLogger(m.dirPath, m.fileSize, true)
	if err != nil {
		log.Fatal(err)
	}
	m.logFile = logFile

	activeMergedFileTS, err := m.logFile.GetFileTS(m.logFile.ActiveFilepath())
	if err != nil {
		return fmt.Errorf("Failed to get timestamp from active merged file: %s", err)
	}

	// Read all data file's content
	entries := make(map[string]*data.Entry)
	for _, f := range logFiles {
		ts, _ := m.logFile.GetFileTS(f.Name())
		if ts > activeMergedFileTS {
			continue
		}

		filePath := filepath.Join(m.dirPath, f.Name())
		fileHandler, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
		if err != nil {
			log.Printf("Failed to open file: %s", filePath)
			continue
		}

		// Read each record from file to build KeyDir
		var curPos int64 = 0
		for {
			entry, err := data.LoadFromFile(fileHandler, curPos)
			if err != nil {
				break
			}
			valueSize := entry.ValueSize
			key := entry.Key
			if valueSize == 0 {
				delete(entries, string(key))
			} else {
				entries[string(key)] = entry // new value overrides old value automatically
			}
			curPos += int64(160 + entry.KeySize + valueSize)
		}
		fileHandler.Close()
	}

	// Write merged file
	for _, v := range entries {
		byteArray, err := v.Dump()
		if err != nil {
			// Skip broken entry
			continue
		}
		m.logFile.Write(byteArray)
		fileID := m.logFile.ActiveFilepath()
		pos := m.logFile.ActiveFilePos() - int64(len(byteArray))
		m.server.UpdateKeyDir(v.Key, fileID, pos, v.ValueSize, v.Timestamp)
	}

	// Delete obsolete files
	for _, f := range logFiles {
		os.Remove(filepath.Join(m.dirPath, f.Name()))
	}
	return nil
}
