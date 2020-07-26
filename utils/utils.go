package utils

import (
	"fmt"
	"os"
	"time"
)

// GetFileSize returns the last pos of given file
func GetFileSize(filename string) (int64, error) {
	fileinfo, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return 0, fmt.Errorf("File doesn't exist: %s", filename)
	}
	if err != nil {
		return 0, err
	}
	return fileinfo.Size(), nil
}

// MakeTimestampInMS returns timestamp in milliseconds
func MakeTimestampInMS() uint64 {
	return uint64(time.Now().UnixNano() / 1e6)
}
