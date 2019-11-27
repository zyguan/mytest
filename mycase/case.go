package mycase

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/zyguan/mytest/resultset"
)

type TaskInfo struct {
	Time time.Time
	ID   string
	Name string
	Meta json.RawMessage
}

type QueryResult struct {
	Time      time.Time
	Duration  float64
	Key       string
	SQL       string
	Version   string
	ResultSet *resultset.ResultSet
}

type MyCase interface {
	NewTask() TaskInfo
	Checkers() map[string]resultset.Checker
	Setup() error
	Teardown() error
	Test(rc ResultStore) error
}

type CheckErrors struct {
	Info        TaskInfo
	DiffErrs    []error
	CollectErrs []error
}

func (e *CheckErrors) Error() string {
	return fmt.Sprintf("there are %d diff errors and %d collect errors", len(e.DiffErrs), len(e.CollectErrs))
}

const (
	StateOK   = "OK"
	StateFail = "FAIL"
)

func Run(mc MyCase, rc ResultStore) error {
	info := mc.NewTask()
	if err := rc.Setup(info); err != nil {
		return fmt.Errorf("setup result store: %s", err.Error())
	}
	if err := mc.Setup(); err != nil {
		return fmt.Errorf("setup case: %s", err.Error())
	}
	defer func() {
		if err := mc.Teardown(); err != nil {
		}
	}()
	if err := mc.Test(rc); err != nil {
		return fmt.Errorf("run test: %s", err.Error())
	}
	errs := CheckErrors{Info: info}
	for key, checker := range mc.Checkers() {
		rs, err := rc.Read(key)
		if err != nil {
			errs.CollectErrs = append(errs.CollectErrs, err)
			continue
		}
		if len(rs) <= 1 {
			continue
		}
		r0 := rs[0]
		state := StateOK
		for i := 1; i < len(rs); i++ {
			if err := checker.Diff(r0.ResultSet, rs[i].ResultSet); err != nil {
				state = StateFail
				errs.DiffErrs = append(errs.DiffErrs, err)
				break
			}
		}
		if err = rc.Mark(key, state); err != nil {
			errs.CollectErrs = append(errs.CollectErrs, err)
		}
	}
	if len(errs.DiffErrs) > 0 || len(errs.CollectErrs) > 0 {
		return &errs
	}
	return nil
}
