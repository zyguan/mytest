package xsql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zyguan/mytest/resultset"
)

func TestLoad(t *testing.T) {
	for _, n := range []string{"decode", "no_test_stage", "unknown_assertion", "file_not_found"} {
		t.Run(n, func(t *testing.T) {
			_, err := Load("fixtures/err_" + n + ".json")
			assert.Error(t, err)
		})
	}

	t.Run("minimum", func(t *testing.T) {
		x, err := Load("fixtures/ok_minimum.json")
		assert.NoError(t, err)
		assert.Equal(t, "foo", x.Name)
		assert.Equal(t, []string{"bar"}, x.Stages.Test)
		assert.Nil(t, x.Meta)
	})

	t.Run("with_meta", func(t *testing.T) {
		x, err := Load("fixtures/ok_with_meta.json")
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"tags": ["a", "bb"]}`), x.Meta)
	})

	t.Run("with_checkers", func(t *testing.T) {
		x, err := Load("fixtures/ok_with_checkers.json")
		assert.NoError(t, err)
		assert.Equal(t, []string{}, x.Stages.Setup)
		assert.Equal(t, []string{"bar"}, x.Stages.Test)
		assert.Equal(t, []string(nil), x.Stages.Teardown)
		assert.Equal(t, resultset.Checker{
			CheckSchema:    false,
			CheckPrecision: false,
			FailFast:       true,
			Assertions:     []resultset.ValueAssertion{resultset.RawBytesAssertion{}},
		}, x.checkers["k1"])
		assert.Equal(t, x.checkers["k1"], x.checkers["k2"])
		assert.Equal(t, resultset.Checker{
			CheckSchema:    false,
			CheckPrecision: true,
			FailFast:       false,
			Assertions:     []resultset.ValueAssertion{resultset.RawBytesAssertion{}},
		}, x.checkers["k3"])
		assert.Equal(t, []resultset.ValueAssertion(nil), x.checkers["k4"].Assertions)
		assert.Equal(t, []resultset.ValueAssertion{resultset.RawBytesAssertion{}}, x.checkers["k5"].Assertions)
		assert.Equal(t, []resultset.ValueAssertion{resultset.FloatAssertion{Delta: 1.0, TypeNames: []string{"DECIMAL", "FLOAT", "DOUBLE"}}}, x.checkers["k6"].Assertions)
		assert.Equal(t, 3.14, x.checkers["k7"].Assertions[0].(resultset.FloatAssertion).Delta)
		assert.Equal(t, []int{}, x.checkers["k7"].Assertions[0].(resultset.FloatAssertion).Columns)
		assert.Equal(t, []int{0, 1, 3}, x.checkers["k8"].Assertions[0].(resultset.FloatAssertion).Columns)
	})
}
