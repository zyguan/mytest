package mystmt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func scanAll(it Iterator) ([]Stmt, error) {
	var ss []Stmt
	for it.Scan() {
		if it.Err() != nil {
			return ss, it.Err()
		}
		ss = append(ss, it.Stmt())
	}
	return ss, nil
}

var texts = []string{
	`CREATE TABLE t1 (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(50),
  purchased DATE, KEY(id))
PARTITION BY RANGE( YEAR(purchased) ) ( 
PARTITION p0 VALUES LESS THAN (1990),
PARTITION p1 VALUES LESS THAN (1995),
PARTITION p2 VALUES LESS THAN (2000),
PARTITION p3 VALUES LESS THAN (2005));`,
	`CREATE TABLE t ( 
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(50),
  purchased DATE, KEY(id));`,
	`CREATE TABLE t2 ( id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50), purchased DATE, KEY(id)) PARTITION BY HASH( YEAR(purchased) ) PARTITIONS 4;`,
}

func toStmts(texts ...string) []Stmt {
	stmts := make([]Stmt, len(texts))
	for i := range stmts {
		stmts[i].Text = texts[i]
	}
	return stmts
}

func TestSplitText(t *testing.T) {
	for _, tt := range []struct {
		text string
		out  []Stmt
	}{
		{"", nil},
		{";", nil},
		{";;", nil},
		{"select 1", toStmts("select 1")},
		{"select 1;", toStmts("select 1;")},
		{"select 1;\n\n# xxx\n ", toStmts("select 1;")},
		{";;select 1;;", toStmts("select 1;")},
		{"select 'foo;';", toStmts("select 'foo;';")},
		{"--select 'foo;';", nil},
		{"-- select 'foo;';", nil},
		{"#select 'foo;';", nil},
		{"# select 'foo;';", nil},
		{"/*select 'foo;';*/", nil},
		{"select /*+ INL_JOIN */ * from t, s", toStmts("select /*+ INL_JOIN */ * from t, s")},
		{"select 1; select 2;", toStmts("select 1;", "select 2;")},
		{"select 1;\nselect 2;", toStmts("select 1;", "select 2;")},
		{"# foo;\nselect 1;\nselect /* bar; */ 2\n;\n", toStmts("select 1;", "select  2\n;")},
		{strings.Join(texts, "\n"), toStmts(texts...)},
		{"--foo\nselect 1;", []Stmt{{"select 1;", []Command{{"foo", []string{}}}}}},
		{"--foo 1\nselect 1;", []Stmt{{"select 1;", []Command{{"foo", []string{"1"}}}}}},
		{"--foo --bar\nselect 1;", []Stmt{{"select 1;", []Command{{"foo", []string{"--bar"}}}}}},
		{"--foo\n--bar\nselect 1;", []Stmt{{"select 1;", []Command{{"foo", []string{}}, {"bar", []string{}}}}}},
		{"--foo\n--bar\nselect 1; select 2; --query 1 \nselect 'x'", []Stmt{
			{"select 1;", []Command{{"foo", []string{}}, {"bar", []string{}}}},
			{"select 2;", nil},
			{"select 'x'", []Command{{"query", []string{"1"}}}},
		}},
	} {
		it := SplitText(tt.text)
		out, err := scanAll(it)
		assert.NoError(t, err, "split: "+tt.text)
		assert.Equal(t, tt.out, out)
	}
}
