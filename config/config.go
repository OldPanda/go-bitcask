// Package config holds functions to talk with configuration files.
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// BitcaskConfig is the configuration file used by Bitcask
// in json format.
type BitcaskConfig struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	PidFile   string `json:"pidfile"`
	DataDir   string `json:"data_directory"`
	DataSize  int    `json:"data_filesize_in_mb"`        // data file rotate size in MB
	MergeFreq int    `json:"merge_frequency_in_seconds"` // in seconds
}

// NewBitcaskConfig reads the config file and converts its content
// to a BitcaskConfig structure.
func NewBitcaskConfig(configFile string) (*BitcaskConfig, error) {
	f, err := os.Open(configFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to open config file: %s", err)
	}
	defer f.Close()

	content, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("Error on reading config: %s", err)
	}

	var c BitcaskConfig
	if err = json.Unmarshal(content, &c); err != nil {
		return nil, fmt.Errorf("Not valid json: %s", err)
	}

	if c.MergeFreq == 0 {
		c.MergeFreq = 3600 // by default run merge process every hour
	}
	return &c, nil
}
