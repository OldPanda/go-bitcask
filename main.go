package main

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

	done := make(chan interface{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		s.Stop()
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
