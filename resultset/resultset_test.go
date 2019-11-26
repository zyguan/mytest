package resultset

import (
	"database/sql"
	"flag"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

var opts struct {
	mysqlDSN string
}

var rss = []ResultSet{
	{nil, nil},
	{[]ColumnDef{}, nil},
	{[]ColumnDef{
		{Name: "foo", Type: "TEXT"},
	}, nil},
	{[]ColumnDef{
		{Name: "foo", Type: "TEXT"},
	}, [][][]byte{
		{{0x1}},
		{nil},
		{{}},
	}},
}

func init() {
	flag.StringVar(&opts.mysqlDSN, "mysql-dsn", "root:@tcp(127.0.0.1:3306)/information_schema", "mysql dsn")
}

func TestEncodeDecodeCheck(t *testing.T) {
	for i, rs := range rss {
		t.Run("EncodeDecodeCheck#"+strconv.Itoa(i), tEncodeDecodeCheck(&rs))
	}
}

func TestEncodeDecodeWithMySQLDataSource(t *testing.T) {
	db, err := sql.Open("mysql", opts.mysqlDSN)
	assert.NoError(t, err)
	if err := db.Ping(); err != nil {
		t.Skipf("failed to ping mysql: dsn=%s, err=%v", opts.mysqlDSN, err)
	}
	for _, table := range []string{
		"INFORMATION_SCHEMA.CHARACTER_SETS",
		"INFORMATION_SCHEMA.COLLATIONS",
		"INFORMATION_SCHEMA.TABLES",
		"INFORMATION_SCHEMA.COLUMNS",
	} {
		rows, err := db.Query("SELECT * FROM " + table)
		assert.NoError(t, err, "select "+table)
		rs, err := ReadFromRows(rows)
		assert.NoError(t, err, "read rows from "+table)
		t.Run("EncodeDecode[MySQL:"+table+"]", tEncodeDecodeCheck(rs))
	}
}

func tEncodeDecodeCheck(rs1 *ResultSet) func(t *testing.T) {
	return func(t *testing.T) {
		bs, err := rs1.Encode()
		assert.NoError(t, err)
		rs2 := &ResultSet{}
		assert.NoError(t, rs2.Decode(bs))
		assert.Equal(t, rs1.DataDigest(), rs2.DataDigest())

		checker := Checker{
			CheckPrecision: true,
			CheckSchema:    true,
			Assertions:     []ValueAssertion{RawBytesAssertion{}},
		}
		assert.NoError(t, checker.Diff(rs1, rs2))

		for i := 0; i < rs1.NCols(); i++ {
			assert.Equal(t, rs1.ColumnDef(i), rs2.ColumnDef(i))
		}
	}
}
