package main

import "testing"

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
