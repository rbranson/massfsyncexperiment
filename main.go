package main

import (
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Syncer func([]*os.File)

func syncWithGoroutines(files []*os.File) {
	wg := sync.WaitGroup{}
	for _, file := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			file.Sync()
		}()
	}
	wg.Wait()
}

func syncWithGoroutinePool(files []*os.File, cnt int) {
	wg := sync.WaitGroup{}
	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for x := i; x < len(files); x += cnt {
				files[x].Sync()
			}
		}()
	}
	wg.Wait()
}

func syncWithGoroutinePool32(files []*os.File) {
	syncWithGoroutinePool(files, 32)
}

func syncWithGoroutinePool64(files []*os.File) {
	syncWithGoroutinePool(files, 64)
}

func syncWithGoroutinePool128(files []*os.File) {
	syncWithGoroutinePool(files, 128)
}

func syncWithGoroutinePool256(files []*os.File) {
	syncWithGoroutinePool(files, 256)
}

func syncNaive(files []*os.File) {
	for _, file := range files {
		file.Sync()
	}
}

func writeAndSync(files []*os.File, name string, syncer Syncer) {
	log.Println("Writing random character to file")
	for _, file := range files {
		file.Write([]byte{byte(rand.Int63() % math.MaxInt8)})
	}

	log.Println("Syncing files using", name)
	start := time.Now()
	syncer(files)
	dur := time.Since(start)
	log.Println("Sync took", dur.Seconds(), "seconds")
}

func runExperiment(fileCount int) {
	log.Println("--- BEGIN", fileCount, "FILES ---")
	dir, err := ioutil.TempDir("", "massfsyncexperiment")
	if err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(dir)
	files := make([]*os.File, fileCount)

	log.Println("Creating", fileCount, "files")

	for i := 0; i < fileCount; i++ {
		path := filepath.Join(dir, strconv.Itoa(i))
		file, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
		}
		files[i] = file
	}

	syncNaive(files)

	writeAndSync(files, "naive", syncNaive)
	writeAndSync(files, "goroutine-per-sync", syncWithGoroutines)
	writeAndSync(files, "goroutine-pool-32", syncWithGoroutinePool32)
	writeAndSync(files, "goroutine-pool-64", syncWithGoroutinePool64)
	writeAndSync(files, "goroutine-pool-128", syncWithGoroutinePool128)
	writeAndSync(files, "goroutine-pool-256", syncWithGoroutinePool256)

	log.Println("")
}

func main() {
	runExperiment(50)
	runExperiment(500)
	runExperiment(5000)
	runExperiment(50000)
}
