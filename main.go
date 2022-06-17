package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nxadm/tail"
	log "github.com/sirupsen/logrus"
	"gitlab.com/devopsworks/tools/dw-query-digest/outputs"
	_ "gitlab.com/devopsworks/tools/dw-query-digest/outputs/all"
	"gopkg.in/cheggaaa/pb.v1"
)

// logentry holds a complete query entry from log file
type logentry struct {
	lines [9]string
	pos   int
}

// query holds a single query with metrics
// FIXME: Do some stats with QCHit
type query struct {
	Time         time.Time
	User         string
	AltUser      string
	Client       string
	ConnectionID int
	Schema       string
	LastErrno    int
	QCHit        bool
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
	Output       string
	ListOutputs  bool
	DisableCache bool
	FileName     string
	Follow       bool
	Refresh      int
}

// actual global variables
var regexeps []replacements
var servermeta outputs.ServerInfo

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
		// 3·   Strip comments.
		{regexp.MustCompile(`(.*)/\*.*\*/(.*)`), "$1$2"},
		{regexp.MustCompile(`(.*) --.*`), "$1"},
		// 2·   Shorten multi-value INSERT statements to a single VALUES() list.
		{regexp.MustCompile(`^(insert .*) values.*`), "$1 values (?)"},
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
	flag.IntVar(&Config.Refresh, "refresh", 0, "How often to refresh display (ms)")
	flag.StringVar(&Config.SortKey, "sort", "time", "Sort key (time (default), count, bytes, lock[time], [rows]sent, [rows]examined, [rows]affected")
	flag.BoolVar(&Config.SortReverse, "reverse", false, "Reverse sort (lowest first)")
	flag.StringVar(&Config.Output, "output", "terminal", "Report output (see `--list-outputs` for a list of possible outputs")
	flag.BoolVar(&Config.ListOutputs, "list-outputs", false, "List possible outputs")
	flag.BoolVar(&Config.DisableCache, "nocache", false, "Disable cache usage (reading from and writing to)")
	flag.BoolVar(&Config.Follow, "follow", false, "Follow file as it grows (tail -F style)")

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

	if _, ok := outputs.Outputs[Config.Output]; !ok {
		log.Errorf("unknown output %s; see `--list-outputs`", Config.Output)
		os.Exit(1)
	}

	if Config.ListOutputs {
		fmt.Println("Compiled outputs:")

		for k := range outputs.Outputs {
			fmt.Printf("\t%s\n", k)
		}
		os.Exit(0)
	}

	// File selection
	var (
		file  *os.File
		piper *io.PipeReader
		pipew *io.PipeWriter
		err   error
	)
	Config.FileName = flag.Arg(0)

	if Config.FileName == "" || Config.FileName == "-" {
		log.Info(`reading from STDIN`)
		Config.FileName = ""
		Config.DisableCache = true
		Config.Follow = true
		Config.ShowProgress = false
		file = os.Stdin
	} else if Config.Follow {
		log.Info(`follow enabled`)
		Config.ShowProgress = false
		Config.DisableCache = true

		piper, pipew = io.Pipe()

		go func(*io.PipeWriter) {
			// file, err =
			defer pipew.Close()
			t, err := tail.TailFile(Config.FileName,
				tail.Config{Follow: true, ReOpen: true,
					Logger:   log.StandardLogger(),
					Location: &tail.SeekInfo{Offset: 0, Whence: 0}})
			if err != nil {
				log.Fatalf(`error setting up tail goroutine: %v`, err)
			}
			for line := range t.Lines {
				_, err := io.Copy(pipew, strings.NewReader(line.Text+"\n"))
				if err != nil {
					log.Fatalf(`error copying from tailfile to pipe: %v`, err)
				}
			}
		}(pipew)
	} else {
		log.Infof(`using "%s" as input file`, Config.FileName)
		file, err = os.Open(Config.FileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
	}

	log.Infof(`using "%s" output`, Config.Output)

	// If cache is not disabled and we're are not tailing input
	// Try to display from cache
	// If it succeeds, we've done our job
	if !Config.DisableCache && runFromCache(flag.Arg(0)) {
		log.Info(`results retrieved from cache`)
		os.Exit(0)
	}

	// log.SetOutput(ioutil.Discard)
	// trace.Start(os.Stderr)
	// defer trace.Stop()
	// defer profile.Start().Stop()

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

	if Config.FileName == "" {
		go fileReader(&wg, file, logentries, 0)
	} else if Config.Follow {
		go fileReader(&wg, piper, logentries, 0)
	} else {
		count, err := lineCounter(file)
		if err != nil {
			panic(err)
		}

		_, err = file.Seek(0, 0)
		if err != nil {
			panic(err)
		}

		servermeta.CumLines = count
		log.Infof("file has %d lines\n", count)

		go fileReader(&wg, file, logentries, count)
	}

	servermeta.AnalysisStart = time.Now()

	// We do not Add this one
	// We do not wait for it in the wg
	// but using <-done
	// This is required so we can properly close the channel
	go aggregator(queries, done, time.Duration(Config.Refresh)*time.Millisecond)

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

func parseHeader(scanner *bufio.Scanner, meta *outputs.ServerInfo) error {
	// Read version
	scanner.Scan()
	version := scanner.Text()

	// Read listeners
	scanner.Scan()
	listeners := scanner.Text()

	// Skip header line
	scanner.Scan()

	// Parse server information
	versionre := regexp.MustCompile(`^([^,]+),\s+Version:\s+([0-9\.]+)([A-Za-z0-9-.]+)\s+\((.*)\)\. started`)
	matches := versionre.FindStringSubmatch(version)

	if len(matches) != 5 {
		meta.Binary = "unable to parse line"
		meta.VersionShort = meta.Binary
		meta.Version = meta.Binary
		meta.VersionDescription = meta.Binary
		meta.TCPPort = 0
		meta.UnixSocket = meta.Binary
		return fmt.Errorf("unable to parse server information; beginning of log might be missing")
	}

	meta.Binary = matches[1]
	meta.VersionShort = matches[2]
	meta.Version = meta.VersionShort + matches[3]
	meta.VersionDescription = matches[4]
	meta.TCPPort, _ = strconv.Atoi(strings.Split(listeners, " ")[2])
	meta.UnixSocket = strings.TrimLeft(strings.Split(listeners, ":")[2], " ")

	return nil
}

// fileReader reads slow log and adds queries in channel for workers
func fileReader(wg *sync.WaitGroup, r io.Reader, lines chan<- logentry, count int) {
	defer wg.Done()
	defer close(lines)

	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	err := parseHeader(scanner, &servermeta)
	if err != nil {
		log.Errorf("error reading log header: %v", err)
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
	curline := 0
	foldnext := false
	hasQuery := false

	for scanner.Scan() {
		line = scanner.Text()
		read++
		if Config.ShowProgress {
			bar.Increment()
		}

		// If we have `# ...`, send current entry and wipe clean and go on
		if hasQuery && strings.HasPrefix(line, "# ") {
			lines <- curentry
			curline = -1
			for i := range curentry.lines {
				curentry.lines[i] = ""
			}
			hasQuery = false
			foldnext = false
		}

		// Skip duplicated header
		// FIXME: this does not match "/usr/libexec/mysqld" (cf https://gitlab.com/devopsworks/tools/dw-query-digest/issues/3)
		firstword := strings.Split(line, " ")[0]
		if firstword == "mysqld," || firstword == "Tcp" || firstword == "Time" {
			continue
		}

		// We check that line number is below capacity minus one
		// Why minus one ? because we increment curline and use it as an index
		// inside this if
		if curline < cap(curentry.lines)-1 {
			// Now if line does not end with a ';', this is a multiline query
			// So we append to previous entry in slice
			if foldnext {
				curentry.lines[curline] = strings.Join([]string{curentry.lines[curline], line}, " ")
			} else {
				curline++
				log.Debugf("curline is %d, len is %d, cap is %d)\n", curline, len(curentry.lines), cap(curentry.lines))
				curentry.lines[curline] = line
			}

			foldnext = false
			firstchar := curentry.lines[curline][:1]
			lastchar := curentry.lines[curline][len(curentry.lines[curline])-1:]

			if firstchar != "#" {
				hasQuery = true
				if lastchar != ";" {
					log.Debugf("line (%d) will fold after %s\n", read+1, firstword)
					foldnext = true
				}
			}
		} else {
			log.Warningf(`request to add element %d for line "%s" exceeds capacity`, curline, line)
		}
	}

	// Ship the last curentry
	lines <- curentry

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

// worker reads queries entries from a channel, parses them to create a clean
// query{} structure, so aggregator can directly extract stats
func worker(wg *sync.WaitGroup, lines <-chan logentry, entries chan<- query) {
	defer wg.Done()

	var err error
	var smallbuf string

	for lineblock := range lines {
		qry := query{}
		for _, line := range lineblock.lines {
			if line == "" {
				break
			}

			if len(line) < 5 {
				log.Warnf("unable to parse line preamble for '%s'; skipping", line)
				continue
			}

			switch strings.ToUpper(line[0:4]) {
			case "# TI":
				// # Time: 2018-12-17T15:18:58.744913Z
				// or
				// # Time: 190603 23:14:02 // in mariadb
				qry.Time, err = time.Parse(time.RFC3339, strings.Split(line, " ")[2])
				if err != nil {
					splitted := strings.Split(line, " ")
					if len(splitted) > 2 {
						datetime := strings.Join(splitted[2:], " ")
						qry.Time, err = time.Parse("060102 15:04:05", datetime)
						if err != nil {
							log.Errorf("worker: error parsing time '%s': %v", datetime, err)
						}
					} else {
						log.Errorf("worker: error parsing time in line '%s'", line)
					}
				}

			case "# US":
				// # User@Host: agency[agency] @  [192.168.0.102]  Id: 3502988
				s := strings.Replace(line, "[", " ", -1)
				s = strings.Replace(s, "]", " ", -1)
				fmt.Sscanf(s, "# User@Host: %s %s  @   %s   Id: %d", &qry.AltUser, &qry.User, &qry.Client, &qry.ConnectionID)

			case "# SC": // "#S"
				//# Schema: taskl-production  Last_errno: 0  Killed: 0
				fmt.Sscanf(line, "# Schema: %s  Last_errno: %d  Killed: %d", &qry.Schema, &qry.LastErrno, &qry.Killed)

			case "# TH": // "#S"
				//# Thread_id: 3  Schema: thedb  QC_hit: No
				fmt.Sscanf(line, "# Thread_id: %d Schema: %s QC_hit: %s", &qry.Schema, &qry.LastErrno, &smallbuf)
				if smallbuf != "No" {
					qry.QCHit = true
				}

			case "# QU": // "#Q"
				// # Query_time: 0.000030  Lock_time: 0.000000  Rows_sent: 0  Rows_examined: 0  Rows_affected: 0
				_, err := fmt.Sscanf(line, "# Query_time: %f  Lock_time: %f  Rows_sent: %d  Rows_examined: %d  Rows_affected: %d", &qry.QueryTime, &qry.LockTime, &qry.RowsSent, &qry.RowsExamined, &qry.RowsAffected)
				if err != nil {
					// mariadb
					// # Query_time: 0.002718  Lock_time: 0.000139  Rows_sent: 25  Rows_examined: 35
					fmt.Sscanf(line, "# Query_time: %f  Lock_time: %f  Rows_sent: %d  Rows_examined: %d", &qry.QueryTime, &qry.LockTime, &qry.RowsSent, &qry.RowsExamined)
				}

			case "# BY":
				// # Bytes_sent: 561
				fmt.Sscanf(line, "# Bytes_sent: %d", &qry.BytesSent)

			case "# RO":
				// mariadb
				// # Rows_affected: 0  Bytes_sent: 252
				fmt.Sscanf(line, "# Rows_affected: %d  Bytes_sent: %d", &qry.RowsAffected, &qry.BytesSent)

			case "SET ":
			case "USE ":
			case "# AD":
				continue

			default:
				qry.FullQuery = line
				if qry.FullQuery == "" {
					log.Warnf("worker: got empty query at line %d", lineblock.pos)
				}

				// fmt.Printf("# call   : %s - %s\n", qry.FingerPrint, line)
				fingerprint(&qry)
				if qry.FingerPrint == "" {
					log.Warnf("worker: got empty fingerprint after fingerprinting at line %d", lineblock.pos)
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

func aggregator(queries <-chan query, done chan<- bool, tickerdelay time.Duration) {
	log.Info("aggregator started")

	querylist := make(map[[32]byte]*outputs.QueryStats)

	servermeta.CumBytes = 0
	servermeta.QueryCount = 0
	servermeta.Start = time.Now()
	servermeta.End = time.Unix(0, 0)

	var ticker *time.Ticker

	// The ticker is here for periodic redisplay
	// Since ticker channel is required in select
	// it is required event if it is not used.
	// To handle this, we have to create a sync.Once function
	// for closing so we do not close twice when refresh is not required
	var tickerstoponce sync.Once
	tickerStop := func() {
		ticker.Stop()
	}

	if tickerdelay > 0 {
		ticker = time.NewTicker(tickerdelay)
	} else {
		ticker = time.NewTicker(10000 * time.Millisecond)
		tickerstoponce.Do(tickerStop)
		// fmt.Printf("ticker channel: %v", ticker.C)
	}

	for {
		select {

		case <-ticker.C:
			displayReport(querylist, nil, false)

		case qry, ok := <-queries:
			if !ok {
				tickerstoponce.Do(tickerStop)
				displayReport(querylist, nil, true)
				log.Info("aggregator exiting")
				done <- true
				return
			}

			servermeta.QueryCount++
			servermeta.CumBytes += qry.BytesSent
			if servermeta.Start.After(qry.Time) {
				servermeta.Start = qry.Time
			}
			if servermeta.End.Before(qry.Time) {
				servermeta.End = qry.Time
			}

			if _, ok := querylist[qry.Hash]; !ok {
				// New entry, create
				querylist[qry.Hash] = &outputs.QueryStats{FingerPrint: qry.FingerPrint, Hash: qry.Hash}
				querylist[qry.Hash].Schema = qry.Schema
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
	}
}

// displayReport show a report given the select output
func displayReport(querylist map[[32]byte]*outputs.QueryStats, sinfo *outputs.ServerInfo, final bool) {
	if sinfo == nil {
		servermeta.UniqueQueries = len(querylist)
		servermeta.AnalysisEnd = time.Now()
		servermeta.AnalysisDuration = servermeta.AnalysisEnd.Sub(servermeta.AnalysisStart).Seconds()
		servermeta.AnalysedLinesPerSecond = float64(servermeta.CumLines) / servermeta.AnalysisDuration
		servermeta.AnalysedBytesPerSecond = float64(servermeta.CumBytes) / servermeta.AnalysisDuration
		servermeta.AnalysedQueriesPerSecond = float64(servermeta.QueryCount) / servermeta.AnalysisDuration
	} else {
		servermeta = *sinfo
	}

	s := make(outputs.QueryStatsSlice, 0, len(querylist))
	for _, d := range querylist {
		// TODO: make all the crappy calculations here so we do not have to repeat them in every output
		s = append(s, d)
	}

	// fmt.Printf("sortkey is %s\n", Config.SortKey)

	sort.Slice(s, func(i, j int) bool {
		var a, b float64

		switch strings.ToUpper(Config.SortKey) {

		case "COUNT":
			a = float64(s[i].Count)
			b = float64(s[j].Count)
		case "BYTES":
			a = float64(s[i].CumBytesSent)
			b = float64(s[j].CumBytesSent)
		case "LOCK", "LOCKTIME":
			a = float64(s[i].CumLockTime)
			b = float64(s[j].CumLockTime)
		case "ROWSSENT", "SENT":
			a = float64(s[i].CumRowsSent)
			b = float64(s[j].CumRowsSent)
		case "ROWSEXAMINED", "EXAMINED":
			a = float64(s[i].CumRowsExamined)
			b = float64(s[j].CumRowsExamined)
		case "ROWSAFFECTED", "AFFECTED":
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

	// Save before trimming query list so we can change `top` in next runs
	// Implement json & cache here
	// If cache is not disabled, open file ".file.cache" and pass an io.Writer
	// If cache is disable, just skip cache writing
	if !Config.DisableCache && final {
		cachefile := Config.FileName + ".cache"
		w, err := os.Create(cachefile)
		if err != nil {
			log.Fatalf("unable to write to cache file %s: %v", cachefile, err)
		}
		log.Infof("caching results in %s", cachefile)
		defer w.Close()
		//outputs.Outputs["null"](servermeta, s, w)
		outputs.Outputs["json"](servermeta, s, w)
	}

	// Keep top queries
	if len(s) > Config.Top {
		s = s[:Config.Top]
	}

	outputs.Outputs[Config.Output](servermeta, s, os.Stdout)
}

func runFromCache(file string) bool {
	cachefile := file + ".cache"

	fi, err := os.Stat(file)
	if err != nil {
		log.Errorf("unable to get file information for %s: %v", file, err)
		return false
	}

	fc, err := os.Stat(cachefile)
	if err != nil {
		log.Errorf("cachefile %s not found: %v", file, err)
		return false
	}

	// If file is more recent than cache
	// return immediately
	if fi.ModTime().After(fc.ModTime()) {
		log.Infof("skipping stale cache")
		return false
	}

	// Open our jsonFile
	cache, err := os.Open(cachefile)

	if err != nil {
		log.Errorf("unable to open cache file %s: %v", cachefile, err)
		return false
	}

	defer cache.Close()

	filecontent, _ := ioutil.ReadAll(cache)

	entries := outputs.CacheInfo{}

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'users' which we defined above
	err = json.Unmarshal(filecontent, &entries)
	if err != nil {
		log.Errorf("unable to read cache: %v", err)
		return false
	}

	if len(entries.Queries) > Config.Top {
		entries.Queries = entries.Queries[:Config.Top]
	}

	mqs := map[[32]byte]*outputs.QueryStats{}

	for _, q := range entries.Queries {
		h := q.Hash
		mqs[h] = q
	}

	displayReport(mqs, &entries.Server, false)
	// outputs.Outputs[Config.Output](entries.Server, entries.Queries, os.Stdout)
	return true
}
