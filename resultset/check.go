package resultset

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
)

type ShapeMismatch struct {
	NRows1  int
	NRows2  int
	Schema1 []ColumnDef
	Schema2 []ColumnDef
	Reason  string
}

func (e ShapeMismatch) Error() string { return e.Reason }

type CellMismatch struct {
	Pos       [2]int
	Val1      []byte
	Val2      []byte
	Assertion ValueAssertion
}

func (e CellMismatch) Error() string {
	return fmt.Sprintf("[%d:%d] %v <> %v by %T", e.Pos[0], e.Pos[1], e.Val1, e.Val2, e.Assertion)
}

type DataMismatch []CellMismatch

func (e DataMismatch) Error() string {
	return strconv.Itoa(len(e)) + " cells mismatch"
}

type ValueAssertion interface {
	Available(i int, col ColumnDef) bool
	Equal(v1 []byte, v2 []byte) (bool, bool)
}

var (
	_ ValueAssertion = RawBytesAssertion{}
	_ ValueAssertion = FloatAssertion{}
)

type RawBytesAssertion struct{}

func (s RawBytesAssertion) Available(i int, col ColumnDef) bool { return true }

func (s RawBytesAssertion) Equal(v1 []byte, v2 []byte) (bool, bool) {
	return bytes.Equal(v1, v2), true
}

type FloatAssertion struct {
	Columns   []int
	TypeNames []string
	Delta     float64
}

func (m FloatAssertion) Available(i int, col ColumnDef) bool {
	for _, k := range m.Columns {
		if i == k {
			return true
		}
	}
	for _, tn := range m.TypeNames {
		if tn == col.Type {
			return true
		}
	}
	return false
}

func (m FloatAssertion) Equal(v1 []byte, v2 []byte) (bool, bool) {
	f1, err := strconv.ParseFloat(string(v1), 64)
	if err != nil {
		return false, false
	}
	f2, err := strconv.ParseFloat(string(v2), 64)
	if err != nil {
		return false, false
	}
	d := f1 - f2
	return -m.Delta < d && d < m.Delta, true
}

type Checker struct {
	CheckSchema    bool
	CheckPrecision bool
	FailFast       bool
	Assertions     []ValueAssertion
}

func (c Checker) diffCols(cols1 []ColumnDef, cols2 []ColumnDef) string {
	for i := range cols1 {
		t1, t2 := cols1[i], cols2[i]
		if t1.Name != t2.Name {
			return fmt.Sprintf("cols[%d].name: %s <> %s", i, t1.Name, t2.Name)
		}
		if t1.Type != t2.Type {
			return fmt.Sprintf("cols[%d].type: %s <> %s", i, t1.Type, t2.Type)
		}
		if t1.HasNullable != t2.HasNullable || t1.Nullable != t2.Nullable {
			return fmt.Sprintf("cols[%d].nullable: %v <> %v", i, t1.Nullable, t2.Nullable)
		}

		if t1.HasLength != t2.HasLength || t1.Length != t2.Length {
			return fmt.Sprintf("cols[%d].type: %s(%d) <> %s(%d)", i, t1.Type, t1.Length, t2.Type, t2.Length)
		}
		if c.CheckPrecision {
			if t1.HasPrecisionScale != t2.HasPrecisionScale || t1.Precision != t2.Precision || t1.Scale != t2.Scale {
				return fmt.Sprintf("cols[%d].type: %s(%d,%d) <> %s(%d,%d)", i,
					t1.Type, t1.Precision, t1.Scale, t2.Type, t2.Precision, t2.Scale)
			}
		}
	}
	return ""
}

func (c Checker) Diff(rs1 *ResultSet, rs2 *ResultSet) error {
	sm := ShapeMismatch{
		NRows1:  rs1.NRows(),
		NRows2:  rs2.NRows(),
		Schema1: rs1.cols,
		Schema2: rs2.cols,
	}
	if rs1.NRows() != rs2.NRows() {
		sm.Reason = fmt.Sprintf("len(rows): %d <> %d", rs1.NRows(), rs2.NRows())
		return sm
	}
	if rs1.NCols() != rs2.NCols() {
		sm.Reason = fmt.Sprintf("len(cols): %d <> %d", rs1.NCols(), rs2.NCols())
		return sm
	}
	if c.CheckSchema {
		sm.Reason = c.diffCols(rs1.cols, rs2.cols)
		if len(sm.Reason) > 0 {
			return sm
		}
	}
	var cms []CellMismatch
	for i := 0; i < rs1.NRows(); i++ {
		for j := 0; j < rs1.NCols(); j++ {
			v1, _ := rs1.RawValue(i, j)
			v2, _ := rs2.RawValue(i, j)
			for _, va := range c.Assertions {
				if !va.Available(j, rs1.cols[j]) {
					continue
				}
				if eq, ok := va.Equal(v1, v2); ok && !eq {
					cm := CellMismatch{Pos: [2]int{i, j}, Val1: v1, Val2: v2, Assertion: va}
					if c.FailFast {
						return cm
					}
					cms = append(cms, cm)
				}
			}
		}
	}
	if len(cms) > 0 {
		return DataMismatch(cms)
	}
	return nil
}

func (c Checker) StreamDiff(rs1 *sql.Rows, rs2 *sql.Rows) error {
	panic("TODO")
}
