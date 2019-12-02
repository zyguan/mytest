package mycase

import (
	"encoding/json"
	"fmt"
	"regexp"
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

type GlobalCheckMode string

const (
	GlobalCheckNone        GlobalCheckMode = "None"
	GlobalCheckIfUnchecked GlobalCheckMode = "IfUnchecked"
	GlobalCheckAlways      GlobalCheckMode = "Always"
)

type GlobalChecker interface {
	Available(key string) bool
	Checker() resultset.Checker
}

type wrappedChecker struct {
	filter   func(string) bool
	internal resultset.Checker
}

func (w *wrappedChecker) Available(key string) bool { return w.filter(key) }

func (w *wrappedChecker) Checker() resultset.Checker { return w.internal }

func CheckerMatchRegexp(pattern string, checker resultset.Checker) GlobalChecker {
	p, err := regexp.Compile(pattern)
	if err != nil {
		return &wrappedChecker{filter: func(s string) bool { return false }, internal: checker}
	}
	return &wrappedChecker{filter: p.MatchString, internal: checker}
}

type RunOptions struct {
	GlobalCheckMode GlobalCheckMode
	GlobalCheckers  []GlobalChecker
}

type RunErrors struct {
	Info      TaskInfo
	Stage     string
	ExecErr   error
	DiffKeys  []string
	DiffErrs  []error
	StoreErrs []error
}

func (e *RunErrors) Error() string {
	prefix := fmt.Sprintf("[%s:%s:%s] ", e.Info.ID, e.Info.Name, e.Stage)
	if e.ExecErr != nil {
		return prefix + e.ExecErr.Error()
	}
	return prefix + fmt.Sprintf("there are %d diff errors and %d store errors", len(e.DiffErrs), len(e.StoreErrs))
}

func (e *RunErrors) Errors() []error {
	var errs []error
	if e.ExecErr != nil {
		errs = append(errs, e.ExecErr)
	}
	for _, err := range e.DiffErrs {
		errs = append(errs, err)
	}
	for _, err := range e.StoreErrs {
		errs = append(errs, err)
	}
	return errs
}

func (e *RunErrors) NoError() bool {
	return e.ExecErr == nil && len(e.DiffErrs) == 0 && len(e.StoreErrs) == 0
}

const (
	StateOK   = "OK"
	StateFail = "FAIL"

	StageSetup    = "SETUP"
	StageTest     = "TEST"
	StageCheck    = "CHECK"
	StageTeardown = "TEARDOWN"
)

func Run(mc MyCase, rc ResultStore, opts ...RunOptions) error {
	info := mc.NewTask()
	errs := &RunErrors{Info: info, Stage: StageSetup}
	if err := rc.Setup(info); err != nil {
		errs.ExecErr = err
		return errs
	}
	if err := mc.Setup(); err != nil {
		errs.ExecErr = err
		return errs
	}
	defer mc.Teardown()

	errs.Stage = StageTest
	if err := mc.Test(rc); err != nil {
		errs.ExecErr = err
		return errs
	}

	errs.Stage = StageCheck
	checked := make(map[string]bool)

	checkKey := func(checker resultset.Checker, key string) bool {
		rs, err := rc.Read(key)
		if err != nil {
			errs.StoreErrs = append(errs.StoreErrs, err)
			return false
		}
		if len(rs) <= 1 {
			return true
		}
		r0 := rs[0]
		state := StateOK
		for i := 1; i < len(rs); i++ {
			if err := checker.Diff(r0.ResultSet, rs[i].ResultSet); err != nil {
				state = StateFail
				errs.DiffErrs = append(errs.DiffErrs, err)
				errs.DiffKeys = append(errs.DiffKeys, key)
				break
			}
		}
		if err = rc.Mark(key, state); err != nil {
			errs.StoreErrs = append(errs.StoreErrs, err)
		}
		return true
	}

	for key, checker := range mc.Checkers() {
		checked[key] = checkKey(checker, key)
	}

	if errs.NoError() && len(opts) > 0 && opts[0].GlobalCheckMode != GlobalCheckNone && len(opts[0].GlobalCheckers) > 0 {
		keys, err := rc.Keys()
		if err != nil {
			errs.StoreErrs = append(errs.StoreErrs, err)
			return errs
		}
		for _, key := range keys {
			if checked[key] && opts[0].GlobalCheckMode != GlobalCheckAlways {
				continue
			}
			for _, gc := range opts[0].GlobalCheckers {
				if gc.Available(key) {
					checked[key] = checkKey(gc.Checker(), key)
					break
				}
			}
		}
	}

	if errs.NoError() {
		return nil
	}
	return errs
}
