package main

import (
	"bufio"
	"fmt"
	"github.com/devops-works/dw-query-digest/outputs"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

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
func TestFingerprint(t *testing.T) {
	queries := []struct {
		vanilla    string
		normalized string
	}{
		// 2·   Shorten multi-value INSERT statements to a single VALUES() list.
		{`INSERT INTO foo ('a','b','c') VALUES ('hey','dude')`, `insert into foo ('a','b','c') values (?)`},
		{`INSERT INTO foo ('a','b','c') VALUES('hey','dude')`, `insert into foo ('a','b','c') values (?)`},
		// 3·   Strip comments.
		{`SELECT * FROM table /* with comment */`, `select * from table`},
		{`SELECT * FROM /* with comment */ table`, `select * from table`},
		{`SELECT * FROM table -- with comment`, `select * from table`},
		// 4·   Abstract the databases in USE statements, so all USE statements are grouped together.
		// 5·   Replace all literals, such as quoted strings
		{"SELECT foo, bar FROM table WHERE foo=5 AND bar=`baz`", "select foo, bar from table where foo = ? and bar = ?"},
		{`SELECT foo, bar FROM table WHERE foo=5 AND bar='baz'`, `select foo, bar from table where foo = ? and bar = ?`},
		{"UPDATE `something` set `a`=4, `b`=false, `c`='1' WHERE `a`=300662 AND `c`=`2`", "update `something` set `a` = ?, `b` = ?, `c` = ? where `a` = ? and `c` = ?"},
		{`SET timestamp=1545059940;`, `set timestamp = ?;`},
		// 6·   Collapse all whitespace into a single space.
		{`SELECT  *  FROM  table  `, `select * from table`},
		// 7·   Lowercase the entire query.
		{`AAA`, `aaa`},
		// 8·   Replace all literals inside of IN() and VALUES() lists with a single placeholder, regardless of cardinality.
		{`SELECT  *  FROM  table  WHERE foo in (1,2,3,4)`, `select * from table where foo in (?)`},
	}

	for _, tt := range queries {
		qry := query{
			FullQuery: tt.vanilla,
		}
		fingerprint(&qry)
		if qry.FingerPrint != tt.normalized {
			t.Errorf("Normalization failed for `%s`; got `%s`, want: `%s`", tt.vanilla, qry.FingerPrint, tt.normalized)
		}
	}
}
func TestLineCounter(t *testing.T) {

	var lentests = []int{0, 1, 1000, 1000000}

	for _, tt := range lentests {
		t.Run(fmt.Sprintf("%d", tt), func(t *testing.T) {

			f, err := ioutil.TempFile("", "linecountbench")
			if err != nil {
				t.Fatalf("unable to create linecount file for testing")
			}

			defer os.Remove(f.Name())

			for i := 0; i < tt; i++ {
				f.WriteString("\n")
			}

			_, err = f.Seek(0, 0)

			if err != nil {
				t.Fatalf("unable to seek to start")
			}

			l, err := lineCounter(f)
			assert.Nil(t, err)
			assert.Equal(t, tt, l, "should be equal")

			f.Close()
		})
	}
}

func TestParseHeader(t *testing.T) {
	// 	s := `/usr/sbin/mysqld, Version: 5.7.19-17-57-log (Percona XtraDB Cluster (GPL), Release rel17, Revision 35cdc81, WSREP version 29.22, wsrep_29.22). started with:
	// Tcp port: 3306  Unix socket: /var/run/mysqld/mysqld.sock
	// Time                 Id Command    Argument`

	var headertests = []struct {
		label  string
		hasErr bool
		header string
		out    outputs.ServerInfo
	}{
		{
			label:  "good header",
			hasErr: false,
			header: `/usr/sbin/mysqld, Version: 5.7.19-17-57-log (Percona XtraDB Cluster (GPL), Release rel17, Revision 35cdc81, WSREP version 29.22, wsrep_29.22). started with:
Tcp port: 3306  Unix socket: /var/run/mysqld/mysqld.sock
Time                 Id Command    Argument`,
			out: outputs.ServerInfo{
				Binary:             "/usr/sbin/mysqld",
				VersionShort:       "5.7.19",
				Version:            "5.7.19-17-57-log",
				VersionDescription: "Percona XtraDB Cluster (GPL), Release rel17, Revision 35cdc81, WSREP version 29.22, wsrep_29.22",
				TCPPort:            3306,
				UnixSocket:         "/var/run/mysqld/mysqld.sock",
			},
		},
		{
			label:  "bad header",
			hasErr: true,

			header: `crap`,
			out: outputs.ServerInfo{
				Binary:             "unable to parse line",
				VersionShort:       "unable to parse line",
				Version:            "unable to parse line",
				VersionDescription: "unable to parse line",
				TCPPort:            0,
				UnixSocket:         "unable to parse line",
			},
		},
	}

	for _, tt := range headertests {
		t.Run(tt.label, func(t *testing.T) {
			r := strings.NewReader(tt.header)
			scanner := bufio.NewScanner(r)
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1024*1024)

			servermeta := outputs.ServerInfo{}

			err := parseHeader(scanner, &servermeta)

			if tt.hasErr {
				assert.NotNil(t, err)
			} else {

				assert.Nil(t, err)
			}

			assert.Equal(t, tt.out.Binary, servermeta.Binary, "should be equal")
			assert.Equal(t, tt.out.VersionShort, servermeta.VersionShort, "should be equal")
			assert.Equal(t, tt.out.Version, servermeta.Version, "should be equal")
			assert.Equal(t, tt.out.VersionDescription, servermeta.VersionDescription, "should be equal")
			assert.Equal(t, tt.out.TCPPort, servermeta.TCPPort, "should be equal")
			assert.Equal(t, tt.out.UnixSocket, servermeta.UnixSocket, "should be equal")
		})

	}
}

func BenchmarkLineCounter(b *testing.B) {
	f, err := ioutil.TempFile("", "linecountbench")
	if err != nil {
		b.Fatalf("unable to create linecount file for benchmark")
	}

	defer os.Remove(f.Name())

	for i := 0; i < 100000; i++ {
		f.WriteString("aaaa\n")
	}

	for n := 0; n < b.N; n++ {
		_, err = f.Seek(0, 0)

		if err != nil {
			b.Fatalf("unable to seek to start")
		}
		lineCounter(f)
	}

	f.Close()
}

func benchmarkFingerprint(statement string, b *testing.B) {
	qry := query{
		FullQuery: statement,
	}

	for n := 0; n < b.N; n++ {
		fingerprint(&qry)
	}
}

func BenchmarkFingerprintSelect(b *testing.B) {
	benchmarkFingerprint(`SELECT foo, bar FROM table WHERE foo=5 AND bar='baz'`, b)
}
func BenchmarkFingerprintInsert(b *testing.B) {
	benchmarkFingerprint(`INSERT INTO foo ('a','b','c') VALUES ('hey','dude')`, b)
}
func BenchmarkFingerprintSelectComment(b *testing.B) {
	benchmarkFingerprint(`SELECT * FROM table /* with comment */`, b)
}
func BenchmarkFingerprintUpdate(b *testing.B) {
	benchmarkFingerprint("UPDATE `something` set `a`=4, `b`=false, `c`='1' WHERE `a`=300662 AND `c`=`2`", b)
}
func BenchmarkFingerprintSet(b *testing.B) {
	benchmarkFingerprint(`SET timestamp=1545059940;`, b)
}
