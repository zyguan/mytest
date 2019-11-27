package mycase

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/zyguan/mytest/resultset"
)

type ResultStore interface {
	Setup(info TaskInfo) error

	Write(res QueryResult) error
	Read(key string) ([]QueryResult, error)

	FindKeys(state string) ([]string, error)
	Mark(key string, state string) error
}

var (
	_ ResultStore = &SQLiteResultStore{}
)

type SQLiteResultStore struct {
	CurrentTask TaskInfo

	db *sql.DB
}

func NewSQLiteResultStore(dsn string) (*SQLiteResultStore, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	s := &SQLiteResultStore{db: db}
	if err = s.bootstrap(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SQLiteResultStore) Setup(info TaskInfo) error {
	if len(info.ID) == 0 {
		return errors.New("id is required")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return errors.New("begin txn: " + err.Error())
	}
	row := tx.QueryRow("select `id`, `name`, `meta`, `time` from `task` where `id` = ?", info.ID)
	var t int64
	var m []byte
	err = row.Scan(&info.ID, &info.Name, &m, &t)
	if err == nil {
		info.Time = time.Unix(t, 0)
		info.Meta = m
		s.CurrentTask = info
	} else if err == sql.ErrNoRows {
		_, err = tx.Exec("insert into `task`(`id`, `name`, `meta`, `time`) values (?, ?, ?, ?)",
			info.ID, info.Name, string(info.Meta), info.Time.Unix())
		if err != nil {
			return errors.New("add task: " + err.Error())
		}
		s.CurrentTask = info
	}
	err = tx.Commit()
	if err != nil {
		return errors.New("commit txn: " + err.Error())
	}
	return nil
}

func (s *SQLiteResultStore) Write(res QueryResult) error {
	if len(s.CurrentTask.ID) == 0 {
		return errors.New("task has not been setup")
	}
	args := make([]interface{}, 8)
	var err error
	args[5], err = res.ResultSet.Encode()
	if err != nil {
		return errors.New("encode result set: " + err.Error())
	}
	args[0] = s.CurrentTask.ID
	args[1] = res.Key
	args[2] = res.SQL
	args[3] = res.Version
	args[4] = res.ResultSet.DataDigest()
	args[6] = res.Time.Unix()
	args[7] = res.Duration
	_, err = s.db.Exec("insert into `result`(`task_id`, `key`, `sql`, `version`, `data_digest`, `result`, `time`, `duration`) values (?, ?, ?, ?, ?, ?, ?, ?)", args...)
	return err
}

func (s *SQLiteResultStore) Read(key string) ([]QueryResult, error) {
	if len(s.CurrentTask.ID) == 0 {
		return nil, errors.New("task has not been setup")
	}
	rows, err := s.db.Query("select `sql`, `version`, `result`, `time`, `duration` from `result` where `task_id` = ? and `key` = ?", s.CurrentTask.ID, key)
	if err != nil {
		return nil, errors.New("query result: " + err.Error())
	}
	defer rows.Close()
	var qrs []QueryResult
	for rows.Next() {
		var raw []byte
		var ts int64
		qr := QueryResult{Key: key}
		err = rows.Scan(&qr.SQL, &qr.Version, &raw, &ts, &qr.Duration)
		if err != nil {
			return nil, errors.New("scan result row: " + err.Error())
		}
		qr.Time = time.Unix(ts, 0)
		qr.ResultSet = &resultset.ResultSet{}
		if err = qr.ResultSet.Decode(raw); err != nil {
			return nil, errors.New("decode result set: " + err.Error())
		}
		qrs = append(qrs, qr)
	}
	return qrs, rows.Err()
}

func (s *SQLiteResultStore) FindKeys(state string) ([]string, error) {
	if len(s.CurrentTask.ID) == 0 {
		return nil, errors.New("task has not been setup")
	}
	rows, err := s.db.Query("select distinct `key` from `group_state` where `task_id` = ? and `state` = ?", s.CurrentTask.ID, state)
	//rs, err := resultset.ReadFromRows(rows)
	//rs.PrettyPrint(os.Stdout)
	if err != nil {
		return nil, errors.New("query keys: " + err.Error())
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, errors.New("scan state row: " + err.Error())
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (s *SQLiteResultStore) Mark(key string, state string) error {
	if len(s.CurrentTask.ID) == 0 {
		return errors.New("task has not been setup")
	}
	_, err := s.db.Exec("insert into `group_state`(`task_id`, `key`, `state`) values (?, ?, ?) on conflict(`task_id`, `key`) do update set `state` = ?", s.CurrentTask.ID, key, state, state)
	return err
}

func (s *SQLiteResultStore) Close() error { return s.db.Close() }

func (s *SQLiteResultStore) bootstrap() error {
	for _, stmt := range []string{
		"create table if not exists `task`(`id` text, `name` text, `meta` text, `time` int, primary key (id))",
		"create table if not exists `result`(`id` integer primary key autoincrement, `task_id` text, `key` text, `sql` text, `version` text, `data_digest` text, `result` blob, `time` int, `duration` real)",
		"create table if not exists `group_state`(`task_id` text, `key` text, `state` text, primary key (`task_id`, `key`))",
		"create index if not exists `idx_result__task_id__key` on `result`(`task_id`, `key`)",
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("bootstrap: %s while executing %s", err.Error(), stmt)
		}
	}
	return nil
}
