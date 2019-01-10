package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gonum.org/v1/gonum/stat"
	"gopkg.in/cheggaaa/pb.v1"

	log "github.com/sirupsen/logrus"
)

// logentry holds a complete query entry from log file
type logentry struct {
	lines [9]string
	pos   int
}

// query holds a single query with metrics
type query struct {
	Time         time.Time
	User         string
	AltUser      string
	Client       string
	ConnectionID int
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
	Hash         [32]byte
}

type querystatsSlice []*querystats

// questystats type & methods
type querystats struct {
	Hash            [32]byte
	Count           int
	FingerPrint     string
	CumQueryTime    float64
	CumBytesSent    int
	CumLockTime     float64
	CumRowsSent     int
	CumRowsExamined int
	CumRowsAffected int
	CumKilled       int
	CumErrored      int
	Concurrency     float64
	QueryTime       []float64
	BytesSent       []float64
	LockTime        []float64
	RowsSent        []float64
	RowsExamined    []float64
	RowsAffected    []float64
}

// // Len is part of sort.Interface.
// func (d querystatsSlice) Len() int {
// 	return len(d)
// }

// // Swap is part of sort.Interface.
// func (d querystatsSlice) Swap(i, j int) {
// 	d[i], d[j] = d[j], d[i]
// }

// // Less is part of sort.Interface. We use count as the value to sort by
// func (d querystatsSlice) Less(i, j int) bool {
// 	return d[i].CumQueryTime < d[j].CumQueryTime
// }

// serverinfo holds server information gathered from first 2 log lines
type serverinfo struct {
	Binary             string
	VersionShort       string
	Version            string
	VersionDescription string
	TCPPort            int
	UnixSocket         string
}

// replacements holds list of regexps we'll apply to queries for normalization
type replacements struct {
	Rexp *regexp.Regexp
	Repl string
}

// options holds options we got in arguments
type options struct {
	ShowProgress bool
	Debug        bool
	Quiet        bool
	Top          int
	SortKey      string
	SortReverse  bool
}

// actual global variables
var regexeps []replacements
var servermeta serverinfo

// Config holds global
var Config options

// Version is set via linker
var Version string

// BuildDate is set via linker
var BuildDate string

func init() {
	// Regexps initialization
	// Create regexps entries for query normalization
	//
	// From pt-query-digest man page (package QueryRewriter section)
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
	regexeps = []replacements{
		// 1·   Group all SELECT queries from mysqldump together
		// ... not implemented ...
		// 2·   Shorten multi-value INSERT statements to a single VALUES() list.
		{regexp.MustCompile(`(insert .*) values .*`), "$1 values (?)"},
		// 3·   Strip comments.
		{regexp.MustCompile(`(.*)/\*.*\*/(.*)`), "$1$2"},
		{regexp.MustCompile(`(.*) --.*`), "$1"},
		// 4·   Abstract the databases in USE statements
		// ... not implemented ... since I don't really get it
		// 5·   Sort of...
		{regexp.MustCompile(`\s*([!><=]{1,2})\s*'[^']+'`), " $1 ?"},
		{regexp.MustCompile(`\s*([!><=]{1,2})\s*\x60[^\x60]+\x60`), " $1 ?"},
		{regexp.MustCompile(`\s*([!><=]{1,2})\s*[\.a-zA-Z0-9_-]+`), " $1 ?"},
		{regexp.MustCompile(`\s*(not)?\s+like\s+'[^']+'`), " not like ?"},
		// {regexp.MustCompile(`\s*(not)?\s+like\s+\x60[^\x60]+\x60`), " not like ?"}, // Not sure this one (LIKE `somestuff`) is necessary
		// 6·   Collapse all whitespace into a single space.
		{regexp.MustCompile(`[\s]{2,}`), " "},
		{regexp.MustCompile(`\s$`), ""}, // trim space at end
		// 7·   Lowercase the entire query.
		// ... implemented elsewhere ...
		// 8·   Replace all literals inside of IN() and VALUES() lists with a single placeholder
		// IN (...), VALUES, OFFSET
		{regexp.MustCompile(`in\s+\([^\)]+\)`), "in (?)"},
		// {regexp.MustCompile(`values\s+\([^\)]+\)`), "values (?)"},
		{regexp.MustCompile(`offset\s+\d+`), "offset ?"},
		// 9·   Collapse multiple identical UNION queries into a single one.
		// ... not implemented ...

	}
}
func main() {

	// var debug = flag.BoolVar(&config."-d", false, "debug mode (very verbose !)")
	// var quiet = flag.Bool("-q", false, "quiet mode (only reporting)")
	flag.BoolVar(&Config.ShowProgress, "progress", false, "Display progress bar")
	flag.BoolVar(&Config.Debug, "debug", false, "Show debugging information (verbose !)")
	flag.BoolVar(&Config.Quiet, "quiet", false, "Display only the report")
	flag.IntVar(&Config.Top, "top", 20, "Top queries to display")
	flag.StringVar(&Config.SortKey, "sort", "time", "Sort key (time (default), count, bytes, lock[time], [rows]sent, [rows]examined, [rows]affected")
	flag.BoolVar(&Config.SortReverse, "reverse", false, "Reverse sort (lowest first)")
	var showversion = flag.Bool("version", false, "Show version & exit")

	flag.Parse()

	if *showversion {
		fmt.Printf("dw-query-digest version %s, built %s\n", Version, BuildDate)
		os.Exit(0)
	}

	log.SetLevel(log.InfoLevel)

	if Config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	if Config.Quiet {
		log.SetLevel(log.ErrorLevel)
	}

	// TODO: bind to -q option
	// log.SetOutput(ioutil.Discard)
	// trace.Start(os.Stderr)
	// defer trace.Stop()
	// defer profile.Start().Stop()

	// TODO: bug with UPDATE `workers` SET `latest_availability_updated_at` = NOW() WHERE `id`=689597;
	// NOW() is replaced by ?()

	// Create channels
	// closed by filereader
	logentries := make(chan logentry, 1000)

	// closed by us
	queries := make(chan query, 1000)
	done := make(chan bool)
	defer close(done)

	var wg sync.WaitGroup
	// numWorkers := 1
	numWorkers := runtime.NumCPU()

	wg.Add(1)
	go fileReader(&wg, flag.Arg(0), logentries)

	// We do not Add this one
	// We do not wait for it in the wg
	// but using <-done
	// This is required so we can properly close the channel
	go aggregator(queries, done)

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker(&wg, logentries, queries)
	}

	wg.Wait()

	// We close queries ourselves
	close(queries)

	// Wait for aggregator to finish
	<-done
}

// lineCouter counts number of lines in file
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

// fileReader reads slow log and adds queries in channel for workers
func fileReader(wg *sync.WaitGroup, f string, lines chan<- logentry) {
	defer wg.Done()
	defer close(lines)

	file, err := os.Open(f)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	count, err := lineCounter(file)

	if err != nil {
		panic(err)
	}

	_, err = file.Seek(0, 0)
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

	// Parse server infomation
	versionre := regexp.MustCompile(`^([^,]+),\s+Version:\s+([0-9\.]+)([a-z0-9-]+)\s+\((.*)\)\. started`)
	matches := versionre.FindStringSubmatch(version)

	if len(matches) != 5 {
		log.Warnf("unable to parse server information; beginning of log might be missing")
		servermeta.Binary = "unable to parse line"
		servermeta.VersionShort = "unable to parse line"
		servermeta.Version = "unable to parse line"
		servermeta.VersionDescription = "unable to parse line"
		servermeta.TCPPort = 0
		servermeta.UnixSocket = "unable to parse line"
	} else {
		servermeta.Binary = matches[1]
		servermeta.VersionShort = matches[2]
		servermeta.Version = servermeta.VersionShort + matches[3]
		servermeta.VersionDescription = matches[4]
		servermeta.TCPPort, _ = strconv.Atoi(strings.Split(listeners, " ")[2])
		servermeta.UnixSocket = strings.TrimLeft(strings.Split(listeners, ":")[2], " ")
	}

	// The entry we'll fill
	curentry := logentry{}

	// Fetch first "# Time" line
	scanner.Scan()
	line := scanner.Text()
	for !strings.HasPrefix(line, "# Time") {
		if !scanner.Scan() {
			log.Errorf("unable to find initial '# Time' entry")
			os.Exit(1)
		}
		line = scanner.Text()
	}
	curentry.lines[0] = line

	var bar *pb.ProgressBar

	// Create progressbar
	if Config.ShowProgress {
		bar = pb.New(count)
		bar.ShowSpeed = true
		bar.Start()
	}

	read := 0
	curline := 1

	for scanner.Scan() {
		line = scanner.Text()
		read++
		// log.Debugf("reading line %d: %s", read, line)

		if Config.ShowProgress {
			bar.Increment()
		}

		// If we have `# Time`, send current entry and wipe clean
		if strings.HasPrefix(line, "# Time") {
			curentry.pos = read
			lines <- curentry
			curline = 0
			for i := range curentry.lines {
				curentry.lines[i] = ""
			}
		}

		curentry.lines[curline] = line
		curline++
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

// worker reads queries entries from a channel, parses them to create a clean
// query{} structure, so aggregator can directly extract stats
func worker(wg *sync.WaitGroup, lines <-chan logentry, entries chan<- query) {
	defer wg.Done()

	// var err error

	for lineblock := range lines {
		qry := query{}
		// fmt.Printf("HERE: %v", lineblock.lines)
		for _, line := range lineblock.lines {
			if line == "" {
				break
			}
			// fmt.Printf("LINE: %s\n", line)
			switch strings.ToUpper(line[0:4]) {

			case "# TI":
				// # Time: 2018-12-17T15:18:58.744913Z
				qry.Time, _ = time.Parse(time.RFC3339, strings.Split(line, " ")[2])

			case "# US":
				// # User@Host: agency[agency] @  [192.168.0.102]  Id: 3502988
				s := strings.Replace(line, "[", " ", -1)
				s = strings.Replace(s, "]", " ", -1)
				fmt.Sscanf(s, "# User@Host: %s %s  @   %s   Id: %d", &qry.AltUser, &qry.User, &qry.Client, &qry.ConnectionID)

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

			case "SET ":
			case "USE ":
			case "# AD":
				continue
			default:
				qry.FullQuery = line
				if qry.FullQuery == "" {
					log.Errorf("worker: got empty query at line %d", lineblock.pos)
				}

				// fmt.Printf("# call   : %s - %s\n", qry.FingerPrint, line)
				fingerprint(&qry)
				if qry.FingerPrint == "" {
					log.Errorf("worker: got empty fingerprint after fingerprinting at line %d", lineblock.pos)
				}
			}
		}

		// We had no queries so we skip this logentries set
		if qry.FingerPrint == "" {
			continue
		}
		qry.Hash = sha256.Sum256([]byte(qry.FingerPrint))
		entries <- qry
	}
	log.Debug("worker exiting")
}

// fingeprint normalizes queries so they can be aggregated
// See regexps initialization above
func fingerprint(qry *query) {
	log.Debugf("fingerprint raw query: %s", qry.FullQuery)

	// Lowercase query first; this is done by pt-query-digest (step 7)
	// and speeds up pattern matching by 20% !
	// (since we do not need to be case insensitive when matching SQL keywords)
	qry.FingerPrint = strings.ToLower(qry.FullQuery)

	// Apply all regexps
	for _, r := range regexeps {
		qry.FingerPrint = r.Rexp.ReplaceAllString(qry.FingerPrint, r.Repl)
	}
	log.Debugf("fingerprint normalized query to: %s", qry.FingerPrint)
}

func aggregator(queries <-chan query, done chan<- bool) {
	log.Info("aggregator started")

	querylist := make(map[[32]byte]*querystats)

	cumbytes := 0
	countqry := 0
	start := time.Now()
	end := time.Unix(0, 0)

	for qry := range queries {
		if qry.FingerPrint == "" {
			log.Errorf("aggregator: got empty fingerprint for %v", qry)
		}

		countqry++
		cumbytes += qry.BytesSent
		if start.After(qry.Time) {
			start = qry.Time
		}
		if end.Before(qry.Time) {
			end = qry.Time
		}
		//fmt.Printf("user:  %s / %s, time: %s\n", qry.User, qry.AltUser, qry.Time)
		if _, ok := querylist[qry.Hash]; !ok {
			// fmt.Printf("FP: %s | HASH: %x\n", qry.FingerPrint, qry.Hash)
			querylist[qry.Hash] = &querystats{FingerPrint: qry.FingerPrint, Hash: qry.Hash}
		}

		if qry.LastErrno != 0 {
			querylist[qry.Hash].CumErrored++
		}

		querylist[qry.Hash].Count++
		querylist[qry.Hash].CumKilled += qry.Killed
		querylist[qry.Hash].CumQueryTime += qry.QueryTime
		querylist[qry.Hash].CumLockTime += qry.LockTime
		querylist[qry.Hash].CumRowsSent += qry.RowsSent
		querylist[qry.Hash].CumRowsExamined += qry.RowsExamined
		querylist[qry.Hash].CumRowsAffected += qry.RowsAffected
		querylist[qry.Hash].CumBytesSent += qry.BytesSent

		querylist[qry.Hash].QueryTime = append(querylist[qry.Hash].QueryTime, qry.QueryTime)
		querylist[qry.Hash].BytesSent = append(querylist[qry.Hash].BytesSent, float64(qry.BytesSent))
		querylist[qry.Hash].LockTime = append(querylist[qry.Hash].LockTime, qry.LockTime)
		querylist[qry.Hash].RowsSent = append(querylist[qry.Hash].RowsSent, float64(qry.RowsSent))
		querylist[qry.Hash].RowsExamined = append(querylist[qry.Hash].RowsExamined, float64(qry.RowsExamined))
		querylist[qry.Hash].RowsAffected = append(querylist[qry.Hash].RowsAffected, float64(qry.RowsAffected))

	}

	fmt.Printf("\n# Server Info\n\n")
	fmt.Printf("  Binary             : %s\n", servermeta.Binary)
	fmt.Printf("  VersionShort       : %s\n", servermeta.VersionShort)
	fmt.Printf("  Version            : %s\n", servermeta.Version)
	fmt.Printf("  VersionDescription : %s\n", servermeta.VersionDescription)
	fmt.Printf("  TCPPort            : %d\n", servermeta.TCPPort)
	fmt.Printf("  UnixSocket         : %s\n", servermeta.UnixSocket)

	fmt.Printf("\n# Global Statistics\n\n")
	fmt.Printf("  Total queries      : %.3fM (%d)\n", float64(countqry)/1000000.0, countqry)
	fmt.Printf("  Total bytes        : %.3fM (%d)\n", float64(cumbytes)/1000000.0, cumbytes)
	fmt.Printf("  Total fingerprints : %d\n", len(querylist))
	fmt.Printf("  Capture start      : %s\n", start)
	fmt.Printf("  Capture end        : %s\n", end)
	fmt.Printf("  Duration           : %s (%d s)\n", end.Sub(start), end.Sub(start)/time.Second)
	fmt.Printf("  QPS                : %.0f\n", float64(time.Second)*(float64(countqry)/float64(end.Sub(start))))

	fmt.Printf("\n# Queries\n")

	s := make(querystatsSlice, 0, len(querylist))
	for _, d := range querylist {
		s = append(s, d)
	}

	sort.Slice(s, func(i, j int) bool {
		var a, b float64

		switch strings.ToUpper(Config.SortKey) {

		case "COUNT":
			a = float64(s[i].Count)
			b = float64(s[j].Count)
		case "BYTES":
			a = float64(s[i].CumBytesSent)
			b = float64(s[j].CumBytesSent)
		case "LOCK":
		case "LOCKTIME":
			a = float64(s[i].CumLockTime)
			b = float64(s[j].CumLockTime)
		case "ROWSSENT":
		case "SENT":
			a = float64(s[i].CumRowsSent)
			b = float64(s[j].CumRowsSent)
		case "ROWSEXAMINED":
		case "EXAMINED":
			a = float64(s[i].CumRowsExamined)
			b = float64(s[j].CumRowsExamined)
		case "ROWSAFFECTED":
		case "AFFECTED":
			a = float64(s[i].CumRowsAffected)
			b = float64(s[j].CumRowsAffected)
		// case "TIME":
		default:
			a = s[i].CumQueryTime
			b = s[j].CumQueryTime
		}

		if Config.SortReverse {
			return a < b
		}
		return a > b
	})

	// Keep top queries
	if len(s) > Config.Top {
		s = s[:Config.Top]
	}

	ffactor := 100.0 * float64(time.Second) / float64(end.Sub(start))
	for idx, val := range s {
		val.Concurrency = val.CumQueryTime * ffactor
		sort.Float64s(val.QueryTime)
		fmt.Printf("\n# Query #%d: %x\n\n", idx+1, val.Hash[0:5])
		fmt.Printf("  Fingerprint     : %s\n", val.FingerPrint)
		fmt.Printf("  Calls           : %d\n", val.Count)
		fmt.Printf("  CumErrored      : %d\n", val.CumErrored)
		fmt.Printf("  CumKilled       : %d\n", val.CumKilled)
		fmt.Printf("  CumQueryTime    : %s\n", fsecsToDuration(val.CumQueryTime))
		fmt.Printf("  CumLockTime     : %s\n", fsecsToDuration(val.CumLockTime))
		fmt.Printf("  CumRowsSent     : %d\n", val.CumRowsSent)
		fmt.Printf("  CumRowsExamined : %d\n", val.CumRowsExamined)
		fmt.Printf("  CumRowsAffected : %d\n", val.CumRowsAffected)
		fmt.Printf("  CumBytesSent    : %d\n", val.CumBytesSent)
		fmt.Printf("  Concurrency     : %2.2f%%\n", val.Concurrency)
		fmt.Printf("  min / max time  : %s / %s\n", fsecsToDuration(val.QueryTime[0]), fsecsToDuration(val.QueryTime[len(val.QueryTime)-1]))
		fmt.Printf("  mean time       : %s\n", fsecsToDuration(stat.Mean(val.QueryTime, nil)))
		fmt.Printf("  p50 time        : %s\n", fsecsToDuration(stat.Quantile(0.5, 1, val.QueryTime, nil)))
		fmt.Printf("  p95 time        : %s\n", fsecsToDuration(stat.Quantile(0.95, 1, val.QueryTime, nil)))
		fmt.Printf("  stddev time     : %s\n", fsecsToDuration(stat.StdDev(val.QueryTime, nil)))
		// fmt.Printf("\tmax time        : %.2f\n", stat.Max(0.95, 1, val.QueryTime, nil))

	}

	log.Info("aggregator exiting")

	done <- true
}

// fsecsToDuration converts float seconds to time.Duration
// Since we have float64 seconds durations
// We first convert to µs (* 1e6) then to duration
func fsecsToDuration(d float64) time.Duration {
	return time.Duration(d*1e6) * time.Microsecond
}
