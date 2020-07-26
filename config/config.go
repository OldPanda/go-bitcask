package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// BitcaskConfig is the configuration file used by Bitcask
// in json format.
type BitcaskConfig struct {
	Host     string `json:"hostname"`
	Port     int    `json:"port"`
	PidFile  string `json:"pidfile"`
	DataDir  string `json:"data_directory"`
	DataSize int    `json:"data_filesize"` // data file rotate size in MB
}

// NewBitcaskConfig reads the config file and converts its content
// to a BitcaskConfig struct.
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
	return &c, nil
}
