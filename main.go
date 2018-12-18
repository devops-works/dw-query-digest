package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
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
	AltUser      string
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
	FullQuery    string
	FingerPrint  string
}

func main() {
	// defer profile.Start().Stop()

	// trace.Start(os.Stderr)
	// defer trace.Stop()

	log.SetLevel(log.ErrorLevel)
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
			curentry = make([]string, 0, 6) //curentry[:0]
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
	// var err error

	for lineblock := range lines {
		for _, line := range lineblock {
			// fmt.Println(strings.ToUpper(line[0:4]))
			switch strings.ToUpper(line[0:4]) {
			case "# TI":
				// # Time: 2018-12-17T15:18:58.744913Z
				qry.Time, _ = time.Parse(time.RFC3339, strings.Split(line, " ")[2])
			case "# US":
				// # User@Host: agency[agency] @  [192.168.0.102]  Id: 3502988
				s := strings.Replace(line, "[", " ", -1)
				s = strings.Replace(s, "]", " ", -1)
				fmt.Sscanf(s, "# User@Host: %s %s  @   %s   Id: %d", &qry.AltUser, &qry.User, &qry.Client, &qry.ID)
			case "# SC": // "#S"
				//# Schema: taskl-production  Last_errno: 0  Killed: 0
				fmt.Sscanf(line, "# Schema: %s  Last_errno: %d  Killed: %d", &qry.Schema, &qry.LastErrno, &qry.Killed)
			case "# QU": // "#Q"
				// # Query_time: 0.000030  Lock_time: 0.000000  Rows_sent: 0  Rows_examined: 0  Rows_affected: 0
				fmt.Sscanf(line, "# Query_time: %f  Lock_time: %f  Rows_sent: %d  Rows_examined: %d  Rows_affected: %d",
					&qry.QueryTime, &qry.LockTime, &qry.RowsSent, &qry.RowsExamined, &qry.RowsAffected)
			case "# BY":
				// # Bytes_sent: 561
				fmt.Sscanf(line, "# Bytes_sent: %d", &qry.BytesSent)

				// qry.BytesSent, err = strconv.Atoi(strings.Split(line, " ")[2])
				// if err != nil {
				// 	log.Errorf("Error converting bytes: %v", err)
				// 	qry.BytesSent = 0
				// }
			case "SET ":
			case "USE ":
			case "# AD":
				continue
			default:
				qry.FullQuery = line
				fingerprint(&qry)
				// fmt.Println(line)
			}
		}
		entries <- qry
	}
	log.Debug("worker exiting")
}

func fingerprint(qry *query) {
	//
	// From pt-query-digest man page
	//
	// 1·   Group all SELECT queries from mysqldump together, even if they are against different tables.
	//      The same applies to all queries from pt-table-checksum.
	// 2·   Shorten multi-value INSERT statements to a single VALUES() list.
	// 3·   Strip comments.
	// 4·   Abstract the databases in USE statements, so all USE statements are grouped together.
	// 5·   Replace all literals, such as quoted strings.  For efficiency, the code that replaces literal numbers is
	//      somewhat non-selective, and might replace some things as numbers when they really are not.
	//      Hexadecimal literals are also replaced.  NULL is treated as a literal.  Numbers embedded in identifiers are
	//	    also replaced, so tables named similarly will be fingerprinted to the same values
	//      (e.g. users_2009 and users_2010 will fingerprint identically).
	// 6·   Collapse all whitespace into a single space.
	// 7·   Lowercase the entire query.
	// 8·   Replace all literals inside of IN() and VALUES() lists with a single placeholder, regardless of cardinality.
	// 9·   Collapse multiple identical UNION queries into a single one.

	// 1. skipped
	// 2. shorten inserts

	re := regexp.MustCompile(`(?i)(insert .*) values .*`)
	match := re.FindStringSubmatch(qry.FullQuery)
	if len(match) == 2 {
		qry.FingerPrint = match[1]
		// fmt.Printf("%q\n", re.FindStringSubmatch(query))
	}

	// 3. strip comments
	// C-style naive approach
	re = regexp.MustCompile(`(.*)/\*.*\*/(.*)`)
	match = re.FindStringSubmatch(qry.FullQuery)
	if len(match) == 2 {
		qry.FingerPrint = match[1]
		// fmt.Printf("%q\n", re.FindStringSubmatch(query))
	}

	// SQL style
	re = regexp.MustCompile(`(.*) --`)
	match = re.FindStringSubmatch(qry.FullQuery)
	if len(match) == 2 {
		qry.FingerPrint = match[1]
		// fmt.Printf("%q\n", re.FindStringSubmatch(query))
	}

}

func aggregator(queries <-chan query, done chan<- bool) {
	log.Info("aggregator started")

	cumbytes := 0
	countqry := 0
	start := time.Now()
	end := time.Unix(0, 0)

	for qry := range queries {
		countqry++
		cumbytes += qry.BytesSent
		if start.After(qry.Time) {
			start = qry.Time
		}
		if end.Before(qry.Time) {
			end = qry.Time
		}
		//fmt.Printf("user:  %s / %s, time: %s\n", qry.User, qry.AltUser, qry.Time)

	}

	fmt.Printf("Total queries : %.1fM (%d)\n", (float64)(countqry/1e6), countqry)
	fmt.Printf("Total bytes   : %.1fM (%d)\n", (float64)(cumbytes/1e6), cumbytes)
	fmt.Printf("Capture start : %s\n", start)
	fmt.Printf("Capture end   : %s\n", end)
	fmt.Printf("Duration      : %s (%d s)\n", end.Sub(start), end.Sub(start)/time.Second)
	fmt.Printf("QPS           : %d\n", countqry/int((end.Sub(start)/time.Second)))

	log.Info("aggregator exiting")

	done <- true

}
