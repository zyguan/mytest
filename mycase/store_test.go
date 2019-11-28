package mycase

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zyguan/mytest/resultset"
)

func TestNewSQLiteResultStore(t *testing.T) {
	store, err := NewSQLiteResultStore(":memory:")
	assert.NoError(t, err)
	store.Close()
}

func TestSqliteResultStore_Setup(t *testing.T) {
	store, err := NewSQLiteResultStore(":memory:")
	assert.NoError(t, err)
	defer store.Close()

	// empty id
	assert.Error(t, store.Setup(TaskInfo{}))

	// task exists
	_, err = store.db.Exec("insert into task (id, name, meta, time) values ('dummy_id', 'dummy_name', 'dummy', 1573430400)")
	assert.NoError(t, err)
	assert.NoError(t, store.Setup(TaskInfo{ID: "dummy_id"}))
	assert.Equal(t, TaskInfo{ID: "dummy_id", Name: "dummy_name", Meta: json.RawMessage("dummy"), Time: time.Unix(1573430400, 0)}, store.CurrentTask)

	// task not exists
	task := TaskInfo{ID: "foo", Name: "bar", Meta: json.RawMessage("42"), Time: time.Unix(1573430400, 0)}
	assert.NoError(t, store.Setup(task))
	assert.Equal(t, task, store.CurrentTask)
	var (
		id   string
		name string
		meta []byte
		ts   int64
	)
	assert.NoError(t, store.db.QueryRow("select id, name, meta, time from task where id = ?", task.ID).Scan(&id, &name, &meta, &ts))
	assert.Equal(t, task.ID, id)
	assert.Equal(t, task.Name, name)
	assert.Equal(t, task.Meta, json.RawMessage(meta))
	assert.Equal(t, task.Time, time.Unix(ts, 0))
}

func TestSQLiteResultStore_ReadWriteResults(t *testing.T) {
	store, err := NewSQLiteResultStore(":memory:")
	assert.NoError(t, err)
	defer store.Close()

	// #1 read-write-keys before setup
	assert.Error(t, store.Write(QueryResult{}))
	_, err = store.Read("key")
	assert.Error(t, err)
	_, err = store.Keys()
	assert.Error(t, err)

	task := TaskInfo{ID: "foo", Name: "bar", Meta: json.RawMessage("42"), Time: time.Unix(1573430400, 0)}
	assert.NoError(t, store.Setup(task))

	rows, err := store.db.Query("select * from task")
	assert.NoError(t, err)
	rs, err := resultset.ReadFromRows(rows)
	assert.NoError(t, err)
	rows.Close()

	qr := QueryResult{
		Time:      time.Unix(1573430460, 0),
		Duration:  3.14,
		Key:       "k",
		SQL:       "select * from task",
		Version:   "sqlite3",
		ResultSet: rs,
	}

	// #2 read empty results
	qrs, err := store.Read(qr.Key)
	assert.NoError(t, err)
	assert.Empty(t, qrs)
	ks, err := store.Keys()
	assert.NoError(t, err)
	assert.Empty(t, ks)

	// #3 write result
	assert.NoError(t, store.Write(qr))

	// #4 read results
	qrs, err = store.Read(qr.Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(qrs))
	assert.Equal(t, qr, qrs[0])
	assert.Equal(t, qr.ResultSet.DataDigest(), qrs[0].ResultSet.DataDigest())
	ks, err = store.Keys()
	assert.NoError(t, err)
	assert.Equal(t, []string{qr.Key}, ks)

	qrs, err = store.Read(qr.Key + "_not_found")
	assert.NoError(t, err)
	assert.Empty(t, qrs)

	// #5 write some other results then read
	assert.NoError(t, store.Write(qr))

	qrs, err = store.Read(qr.Key)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(qrs))
	assert.Equal(t, qr, qrs[0])
	assert.Equal(t, qr, qrs[1])
	ks, err = store.Keys()
	assert.NoError(t, err)
	assert.Equal(t, []string{qr.Key}, ks)

	qr2 := qr
	qr2.Key = "k2"
	assert.NoError(t, store.Write(qr2))

	qrs, err = store.Read(qr.Key)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(qrs))
	qrs, err = store.Read(qr2.Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(qrs))
	ks, err = store.Keys()
	assert.NoError(t, err)
	assert.Equal(t, []string{qr.Key, qr2.Key}, ks)

	// #6 switch task
	task.ID = "bar"
	assert.NoError(t, store.Setup(task))
	qrs, err = store.Read(qr.Key)
	assert.NoError(t, err)
	assert.Empty(t, qrs)
	ks, err = store.Keys()
	assert.NoError(t, err)
	assert.Empty(t, ks)
}

func TestSQLiteResultStore_MarkListKeys(t *testing.T) {
	store, err := NewSQLiteResultStore(":memory:")
	assert.NoError(t, err)
	defer store.Close()

	// #1 mark-list before setup
	assert.Error(t, store.Mark("foo", "bar"))
	_, err = store.KeysByState("foo")
	assert.Error(t, err)

	task := TaskInfo{ID: "foo", Name: "bar", Meta: json.RawMessage("42"), Time: time.Unix(1573430400, 0)}
	assert.NoError(t, store.Setup(task))

	// #2 list empty keys
	ks, err := store.KeysByState("foo")
	assert.NoError(t, err)
	assert.Empty(t, ks)

	// #3 mark key state
	assert.NoError(t, store.Mark("foo", StateOK))
	assert.NoError(t, store.Mark("bar", StateOK))
	assert.NoError(t, store.Mark("baz", StateFail))
	var cnt int
	assert.NoError(t, store.db.QueryRow("select count(1) from key_state where task_id = ?", store.CurrentTask.ID).Scan(&cnt))
	assert.Equal(t, 3, cnt)

	// #4 list keys
	ks, err = store.KeysByState(StateOK)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ks))
	assert.Contains(t, ks, "foo")
	assert.Contains(t, ks, "bar")
	ks, err = store.KeysByState(StateFail)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ks))
	assert.Equal(t, "baz", ks[0])

	// #5 update state
	assert.NoError(t, store.Mark("bar", StateFail))
	ks, err = store.KeysByState(StateOK)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ks))
	assert.Contains(t, ks, "foo")
	ks, err = store.KeysByState(StateFail)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ks))
	assert.Contains(t, ks, "bar")
	assert.Contains(t, ks, "baz")

	// #6 switch task
	task.ID = "bar"
	assert.NoError(t, store.Setup(task))
	ks, err = store.KeysByState(StateOK)
	assert.NoError(t, err)
	assert.Empty(t, ks)
	ks, err = store.KeysByState(StateFail)
	assert.NoError(t, err)
	assert.Empty(t, ks)

	// #7 mark again
	assert.NoError(t, store.Mark("foo", StateOK))
	ks, err = store.KeysByState(StateOK)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ks))
	assert.Contains(t, ks, "foo")

}
