package mycase

import (
	"encoding/json"
	"time"

	"github.com/juju/errors"
	"github.com/zyguan/mytest/resultset"
	"github.com/zyguan/zapglog/log"
	"go.uber.org/zap"
)

type RunInfo struct {
	Time time.Time
	ID   string
	Name string
	Meta json.RawMessage
}

type ResultStore interface {
	Setup(info RunInfo)

	Write(sql string, key string, rs *resultset.ResultSet)
	Read(key string) []*resultset.ResultSet

	FindKeys(state string) []string
	Mark(key string, state string)
}

type MyCase interface {
	RunInfo() RunInfo
	Checkers() map[string]resultset.Checker
	Setup() error
	Teardown() error
	Test(rc ResultStore) error
}

const (
	StateOK   = "OK"
	StateFail = "FAIL"
)

func Run(mc MyCase, rc ResultStore) error {
	info := mc.RunInfo()
	logger := log.L().With(zap.String("id", info.Name), zap.String("name", info.Name))
	rc.Setup(info)
	if err := mc.Setup(); err != nil {
		logger.Error("setup error", zap.Error(err))
		return errors.Annotate(err, "setup case")
	}
	defer func() {
		if err := mc.Teardown(); err != nil {
			logger.Warn("teardown error", zap.Error(err))
		}
	}()
	if err := mc.Test(rc); err != nil {
		logger.Error("test error", zap.Error(err))
		return errors.Annotate(err, "run test")
	}
	for key, checker := range mc.Checkers() {
		rss := rc.Read(key)
		if len(rss) <= 1 {
			continue
		}
		rs0 := rss[0]
		state := StateOK
		for i := 1; i < len(rss); i++ {
			if err := checker.Diff(rs0, rss[i]); err != nil {
				logger.Warn("diff error", zap.String("key", key), zap.Error(err))
				state = StateFail
				break
			}
		}
		rc.Mark(key, state)
	}
	return nil
}
