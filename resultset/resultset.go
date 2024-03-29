package resultset

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"io"
	"sort"
	"strconv"

	"github.com/olekukonko/tablewriter"
)

type ColumnDef struct {
	Name      string
	Type      string
	Length    int64
	Precision int64
	Scale     int64
	Nullable  bool

	HasNullable       bool
	HasLength         bool
	HasPrecisionScale bool
}

type ExecResult struct {
	RowsAffected int64
	LastInsertId int64

	HasRowsAffected bool
	HasLastInsertId bool
}

type ResultSet struct {
	cols []ColumnDef
	data [][][]byte
	exec ExecResult
}

func New(schema []ColumnDef) *ResultSet {
	return &ResultSet{cols: schema}
}

func NewFromResult(res sql.Result) *ResultSet {
	var err error
	rs := &ResultSet{exec: ExecResult{}}
	rs.exec.RowsAffected, err = res.RowsAffected()
	rs.exec.HasRowsAffected = err == nil
	rs.exec.LastInsertId, err = res.LastInsertId()
	rs.exec.HasLastInsertId = err == nil
	return rs
}

func ReadFromRows(rows *sql.Rows) (*ResultSet, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	cols := make([]ColumnDef, len(types))
	for i, t := range types {
		cols[i].Name = t.Name()
		cols[i].Type = t.DatabaseTypeName()
		cols[i].Nullable, cols[i].HasNullable = t.Nullable()
		cols[i].Length, cols[i].HasLength = t.Length()
		cols[i].Precision, cols[i].Scale, cols[i].HasPrecisionScale = t.DecimalSize()
	}
	rs := New(cols)
	for rows.Next() {
		if err = rows.Scan(rs.AllocateRow()...); err != nil {
			return rs, err
		}
	}
	return rs, rows.Err()
}

func (rs *ResultSet) IsExecResult() bool { return len(rs.cols) == 0 }

func (rs *ResultSet) ExecResult() ExecResult { return rs.exec }

func (rs *ResultSet) NRows() int { return len(rs.data) }

func (rs *ResultSet) NCols() int { return len(rs.cols) }

func (rs *ResultSet) ColumnDef(i int) ColumnDef {
	if i < 0 {
		i += len(rs.cols)
	}
	if i < 0 || i >= len(rs.cols) {
		return ColumnDef{}
	}
	return rs.cols[i]
}

func (rs *ResultSet) Sort(less func(i int, j int) bool) { sort.SliceStable(rs.data, less) }

func (rs *ResultSet) RawValue(i int, j int) ([]byte, bool) {
	if i < 0 {
		i += len(rs.data)
	}
	if i < 0 || i >= len(rs.data) {
		return nil, false
	}
	row := rs.data[i]
	if j < 0 {
		j += len(row)
	}
	if j < 0 || j >= len(rs.data) {
		return nil, false
	}
	return rs.data[i][j], true
}

func (rs *ResultSet) AllocateRow() []interface{} {
	if rs.IsExecResult() {
		return nil
	}
	row := make([][]byte, len(rs.cols))
	rs.data = append(rs.data, row)
	xs := make([]interface{}, len(row))
	for i := range row {
		xs[i] = &row[i]
	}
	return xs
}

func (rs *ResultSet) DataDigest(optFilters ...func(i int, j int, raw []byte) bool) string {
	if rs.IsExecResult() {
		return ""
	}
	h := sha1.New()
	for i, row := range rs.data {
	cellLoop:
		for j, v := range row {
			for _, filter := range optFilters {
				if filter != nil && !filter(i, j, v) {
					continue cellLoop
				}
			}
			buf := make([]byte, 4)

			binary.BigEndian.PutUint32(buf, uint32(i))
			h.Write(buf)
			binary.BigEndian.PutUint32(buf, uint32(j))
			h.Write(buf)

			h.Write(v)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (rs *ResultSet) PrettyPrint(out io.Writer) {
	table := tablewriter.NewWriter(out)
	if rs.IsExecResult() {
		table.SetHeader([]string{"RowsAffected", "LastInsertId"})
		row := []string{"NULL", "NULL"}
		if rs.exec.HasRowsAffected {
			row[0] = strconv.FormatInt(rs.exec.RowsAffected, 10)
		}
		if rs.exec.HasLastInsertId {
			row[1] = strconv.FormatInt(rs.exec.LastInsertId, 10)
		}
		table.Append(row)
		table.Render()
		return
	}
	hdr := make([]string, len(rs.cols))
	for i, c := range rs.cols {
		hdr[i] = c.Name
	}
	table.SetHeader(hdr)
	for _, r := range rs.data {
		row := make([]string, len(r))
		for i, s := range r {
			if s == nil {
				row[i] = "NULL"
			} else {
				row[i] = string(s)
			}
		}
		table.Append(row)
	}
	table.Render()
}

func (rs *ResultSet) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := rs.EncodeTo(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (rs *ResultSet) EncodeTo(w io.Writer) error {
	zw := gzip.NewWriter(w)
	defer zw.Close()
	enc := gob.NewEncoder(zw)
	tmp := struct {
		Cols []ColumnDef
		Data [][][]byte
		Exec ExecResult
	}{rs.cols, rs.data, rs.exec}
	return enc.Encode(tmp)
}

func (rs *ResultSet) Decode(raw []byte) error {
	return rs.DecodeFrom(bytes.NewReader(raw))
}

func (rs *ResultSet) DecodeFrom(r io.Reader) error {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(zr)
	var tmp struct {
		Cols []ColumnDef
		Data [][][]byte
		Exec ExecResult
	}
	if err := dec.Decode(&tmp); err != nil {
		return err
	}
	rs.cols, rs.data, rs.exec = tmp.Cols, tmp.Data, tmp.Exec
	return nil
}
