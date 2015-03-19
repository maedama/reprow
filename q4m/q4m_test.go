package q4m

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/lestrrat/go-tcputil"
	"github.com/lestrrat/go-test-mysqld"
	"github.com/maedama/reprow"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	logger, _ = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.DebugLvl)
	mysqld    *mysqltest.TestMysqld
	q4m       *Q4M
	table     = "reprow_test_queue"
	port      int
	makePort  sync.Once
	dsn       string
)

type TestQueue struct {
	StringColumn string
	IntColumn    int
	NullColumn   interface{}
}

func TestStart(t *testing.T) {

	// This test launches/stop/relaunch mysqld in the test, so they would handle it for you
	testMysqldRecoverability(t)

	// Use same mysqld for following tests
	mysqld, err := launchMysqld()
	if err != nil {
		t.Skipf("mysqld failed to initialized e=%s", err.Error())
	}
	defer mysqld.Stop()

	testQueueCompletion(t)
	testPayload(t)
}

func testMysqldRecoverability(t *testing.T) {

	t.Logf("testing mysqld recoverability")
	mysqld, err := launchMysqld()
	if err != nil {
		t.Skipf("mysqld failed to initialized e=%s", err.Error())
	}

	q4m, err = NewQ4M(map[string]interface{}{
		"Dsn":   dsn,
		"Table": table,
	}, logger)

	if err != nil {
		t.Fatalf("q4m failed to initialized e=%s", err.Error())
	}

	jobChannel := make(chan reprow.Job)

	queue := TestQueue{
		StringColumn: "foo",
		IntColumn:    10,
		NullColumn:   nil,
	}

	mustInsertQueue(queue, t)

	q4m.Start(jobChannel)
	defer q4m.Stop()

	mustDequeue(jobChannel, t)
	mysqld.Stop()

	<-time.After(100 * time.Millisecond)
	mysqld, err = launchMysqld()
	if err != nil {
		t.Fatalf("test mysqld failded to restart %s", err.Error())
	}
	<-time.After(100 * time.Millisecond)

	mustInsertQueue(queue, t)
	mustDequeue(jobChannel, t)
	mysqld.Stop()
}

func testPayload(t *testing.T) {
	t.Logf("testing payload")

	jobChannel := make(chan reprow.Job)
	q4m, err := NewQ4M(map[string]interface{}{
		"Dsn":   dsn,
		"Table": table,
	}, logger)
	if err != nil {
		t.Fatalf("test mysqld failded to restart %s", err.Error())
	}

	q4m.Start(jobChannel)
	defer q4m.Stop()

	queue := TestQueue{
		StringColumn: "foo",
		IntColumn:    10,
		NullColumn:   nil,
	}
	mustInsertQueue(queue, t)
	job := mustDequeue(jobChannel, t)
	got := job.Payload()
	job.End()
	expect := map[string]interface{}{
		"intcolumn":    10,
		"stringcolumn": "foo",
		"nullcolumn":   nil,
	}

	if !DeepEqual(got, expect) {
		t.Errorf("payload does not match got=%v expect=%v", got, expect)
	}
}

func testQueueCompletion(t *testing.T) {
	t.Logf("testing queue_end/queue_abort")

	jobChannel := make(chan reprow.Job)
	q4m, err := NewQ4M(map[string]interface{}{
		"Dsn":   dsn,
		"Table": table,
	}, logger)
	if err != nil {
		t.Fatalf("test mysqld failded to restart %s", err.Error())
	}

	q4m.Start(jobChannel)

	queue := TestQueue{
		StringColumn: "foo",
		IntColumn:    10,
		NullColumn:   nil,
	}
	mustInsertQueue(queue, t)
	job := mustDequeue(jobChannel, t)
	job.Abort(0)
	q4m.Stop()
	row := q4m.DB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	if err != nil {
		t.Fatalf("queue count not retrieved %s", err.Error())
	}
	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("queue count not retrieved scan failed %s", err.Error())
	}
	if count != 1 {
		t.Errorf("Queue abort failed count=%d", count)
	}

	q4m.Start(jobChannel)
	job = mustDequeue(jobChannel, t)
	job.End()
	q4m.Stop()

	row = q4m.DB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	if err != nil {
		t.Fatalf("queue count not retrieved %s", err.Error())
	}
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("queue count not retrieved scan failed %s", err.Error())
	}
	if count != 0 {
		t.Errorf("queue end failed count=%d", count)
	}

}

func launchMysqld() (mysqld *mysqltest.TestMysqld, err error) {

	c, err := getMysqldConfig()
	if err != nil {
		return
	}

	mysqld, err = mysqltest.NewMysqld(c)
	if err != nil {
		err = errors.New("faield to initialize mysqld " + err.Error())
		return
	}

	db, err := sql.Open("mysql", dsn)
	defer db.Close()
	if err != nil {
		err = errors.New("faield to open db " + err.Error())
		return
	}

	// Q4M support files
	statements := []string{
		"INSTALL PLUGIN queue SONAME 'libqueue_engine.so'",
		"CREATE FUNCTION queue_wait RETURNS INT SONAME 'libqueue_engine.so'",
		"CREATE FUNCTION queue_end RETURNS INT SONAME 'libqueue_engine.so'",
		"CREATE FUNCTION queue_abort RETURNS INT SONAME 'libqueue_engine.so'",
		"CREATE FUNCTION queue_rowid RETURNS INT SONAME 'libqueue_engine.so'",
		"CREATE FUNCTION queue_set_srcid RETURNS INT SONAME 'libqueue_engine.so'",
		"CREATE FUNCTION queue_compact RETURNS INT SONAME 'libqueue_engine.so'",
		fmt.Sprintf("CREATE TABLE %s(intcolumn int unsigned NOT NULL, stringcolumn varchar(255) NOT NULL, nullcolumn int DEFAULT NULL) Engine=Queue", table),
	}

	for _, stmt := range statements {
		_, err = db.Exec(stmt)
		if err != nil {
			err = errors.New("failed to execute q4m prepare statement e=" + err.Error())
			return
		}
	}

	rows, err := db.Query("SHOW PLUGINS")
	if err != nil {
		return
	}

	var found bool
	for rows.Next() {
		var name string
		var status interface{}
		var pluginType interface{}
		var library interface{}
		var license interface{}
		err = rows.Scan(&name, &status, &pluginType, &library, &license)
		if err != nil {
			err = errors.New("scan failed " + err.Error())
			return
		}
		if name == "QUEUE" {
			found = true
		}
	}

	if !found {
		err = errors.New("q4m not installed")
		return
	}
	return
}

func mustInsertQueue(queue TestQueue, t *testing.T) {

	_, err := q4m.DB.Exec(fmt.Sprintf(
		"INSERT INTO %s (intcolumn, stringcolumn, nullcolumn) VALUES(%d, \"%s\", NULL)",
		table, queue.IntColumn, queue.StringColumn,
	))

	if err != nil {
		t.Fatalf(err.Error())
	}
}

func mustDequeue(jobChannel chan reprow.Job, t *testing.T) reprow.Job {

	select {
	case job := <-jobChannel:
		finWait := make(chan bool)

		go func() {
			finWait <- job.WaitFinalize()
		}()
		select {
		case fin := <-finWait:
			if fin == false {
				t.Fatalf("job not finalized")
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("finalize timetout")
		}
		return job
	case <-time.After(1 * time.Second):
		t.Fatalf("dequeue timetout")
		return nil
	}
}

func getMysqldConfig() (*mysqltest.MysqldConfig, error) {

	// It should return config that would have same dsn every time it is called
	makePort.Do(func() {
		p, err := tcputil.EmptyPort()
		if err != nil {
			return
		}
		port = p
	})
	if port == 0 {
		return nil, errors.New("port not retrieved")
	}
	config := mysqltest.NewConfig()
	config.SkipNetworking = false
	config.Port = port
	mysqldPath := os.Getenv("MYSQLD")
	if len(mysqldPath) > 0 {
		config.Mysqld = mysqldPath
	}

	mysqlInstallDbPath := os.Getenv("MYSQL_INSTALL_DB")
	if len(mysqlInstallDbPath) > 0 {
		config.MysqlInstallDb = mysqlInstallDbPath
	}

	dsn = fmt.Sprintf("root@tcp(127.0.0.1:%d)/mysql", port)
	return config, nil
}

func DeepEqual(a map[string]interface{}, b map[string]interface{}) bool {
	AasJson, _ := json.Marshal(a)
	BasJson, _ := json.Marshal(b)
	if string(AasJson) != string(BasJson) {
		return false
	} else {
		return true
	}
}
