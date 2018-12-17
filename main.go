package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type logentry struct {
	Lines []string
}

type query struct {
	Time         time.Time
	User         string
	Client       string
	ID           int
	Schema       string
	LastErrno    int
	Killed       int
	QueryTime    float64
	LockTime     float64
	RowsSent     int
	RowsExamined int
	RowsAffected int
	BytesSent    int
}

func main() {
	log.SetLevel(log.DebugLevel)
	// closed by filereader
	logentries := make(chan []string, 1000)
	// closed by us
	queries := make(chan query, 1000)
	done := make(chan bool)

	var wg sync.WaitGroup

	wg.Add(1)
	go fileReader(&wg, os.Args[1], logentries)

	// We do not Add this one
	// We do not wait for it in the wg
	// but using <-done
	// This is required so we can properly close the channel
	go aggregator(queries, done)

	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go worker(&wg, logentries, queries)
	}

	wg.Wait()

	// We close queries ourselves
	close(queries)

	// Wait for aggregator to finish
	<-done
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func fileReader(wg *sync.WaitGroup, f string, lines chan<- []string) {
	defer wg.Done()
	file, err := os.Open(f)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	count, err := lineCounter(file)
	file.Seek(0, 0)

	if err != nil {
		panic(err)
	}

	log.Infof("file has %d lines\n", count)

	scanner := bufio.NewScanner(file)

	// Read version
	scanner.Scan()
	version := scanner.Text()

	// Read listeners
	scanner.Scan()
	listeners := scanner.Text()

	// Skip header line
	scanner.Scan()

	fmt.Printf("Got version: %s\n", version)
	fmt.Printf("Got listeners: %s\n", listeners)

	// The entry we'll fill
	curentry := make([]string, 0, 6)

	// Fetch first "# Time" line
	// We should loop until found here to be more robust
	scanner.Scan()
	line := scanner.Text()
	curentry = append(curentry, line)

	read := 0
	ticks := count / 10
	for scanner.Scan() {
		line = scanner.Text()
		read++
		if read%ticks == 0 {
			log.Infof("r %d/%d (%d %%)\n", read, count, read*100/count)
		}

		// If we have `# Time`, send current entry and wipe clean
		if strings.HasPrefix(line, "# Time") {
			lines <- curentry
			curentry = curentry[:0]
		}
		curentry = append(curentry, line)

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	close(lines)
}

func worker(wg *sync.WaitGroup, lines <-chan []string, entries chan<- query) {

	defer wg.Done()

	qry := query{}

	for lineblock := range lines {
		for _, line := range lineblock {
			// fmt.Printf("%d => %s\n", line[0]+line[2], line)
			switch line[0] + line[2] {
			case 119: // "#T"
				qry.Time = time.Now()
			case 120: // "#U"
				qry.User = "foobar"
			case 118: // "#S"
				qry.Schema = "fizzbuz"
			case 116: // "#Q"
				qry.QueryTime = 0
			case 101: // "#B"
				qry.BytesSent = 12
			}
		}
		entries <- qry
	}
	log.Debug("worker exiting")
}

// # Time: 2018-12-17T15:18:58.744913Z
// # User@Host: agency[agency] @  [192.168.0.102]  Id: 3502988
// # Schema: taskl-production  Last_errno: 0  Killed: 0
// # Query_time: 0.000030  Lock_time: 0.000000  Rows_sent: 0  Rows_examined: 0  Rows_affected: 0
// # Bytes_sent: 561

func aggregator(queries <-chan query, done chan<- bool) {
	log.Info("aggregator started")

	cumbytes := 0
	countqry := 0

	for qry := range queries {
		countqry++
		cumbytes += qry.BytesSent
	}

	fmt.Printf("saw %d bytes over %d queries\n", cumbytes, countqry)
	log.Info("aggregator exiting")

	done <- true

}
