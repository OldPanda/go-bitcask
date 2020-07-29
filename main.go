package main

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
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Panda-Home/bitcask/config"
	"github.com/Panda-Home/bitcask/merger"
	"github.com/Panda-Home/bitcask/server"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "c", "", "Path to config file")
}

func main() {
	flag.Parse()
	if configPath == "" {
		log.Fatal("Config file must be provided")
	}

	c, err := config.NewBitcaskConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	if err := writePid(c.PidFile); err != nil {
		log.Fatalf("Failed to write pidfile: %s", err)
	}

	s, err := server.NewServer(c)
	if err != nil {
		log.Fatalf("Failed to create server: %s", err)
	}

	merger, err := merger.NewMerger(c, s)
	if err != nil {
		log.Fatalf("Failed to create merger: %s", err)
	}

	done := make(chan interface{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		s.Stop()
		merger.Stop()
		cleanup(c)
		close(done)
	}()
	<-done
	log.Println("Exit")
}

func writePid(pidFile string) error {
	if pidFile == "" {
		log.Println("Pidfile is not configured. Use 'bitcask.pid' by default")
		pidFile = "bitcask.pid"
	}

	info, err := os.Stat(pidFile)
	if os.IsExist(err) {
		if info.IsDir() {
			return errors.New("Pidfile can't be a directory")
		}
		return errors.New("Bitcask is running")
	}

	if err := os.MkdirAll(filepath.Dir(pidFile), 0755); err != nil {
		return fmt.Errorf("Failed to create pidfile directory: %s", err)
	}

	f, err := os.OpenFile(pidFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Failed to open pidfile: %s", err)
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%d", os.Getpid())
	if err != nil {
		return fmt.Errorf("Failed to write pid to file")
	}
	return nil
}

func cleanup(c *config.BitcaskConfig) {
	os.Remove(c.PidFile)
}
