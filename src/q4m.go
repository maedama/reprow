package reprow

import (
	_ "database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Q4MJob struct {
	payload map[string]interface{}
	tx      *sqlx.Tx
	queue   *Q4M
}

func (j *Q4MJob) Queue() *Q4M {
	return j.queue
}

func (j *Q4MJob) Payload() map[string]interface{} {
	return j.payload
}
func (j *Q4MJob) Abort() {
	j.queue.Abort(j)
}
func (j *Q4MJob) End() {
	j.queue.End(j)
}

type Q4M struct {
	dsn   string
	table string
	db    *sqlx.DB
	conns chan bool
}

type Test struct {
	id int
}

func (q *Q4M) Start(outChannel chan Job) {
	for {
		q.conns <- true
		go func() {
			tx, err := q.db.Beginx()
			if err != nil {
				logger.Errorf("Failed to make new transaction e=%s", err.Error())
				return
			}
			row := tx.QueryRow(fmt.Sprintf("SELECT queue_wait(\"%s\")", q.table))
			var res int
			err = row.Scan(&res)
			if err != nil {
				logger.Errorf("Failed for to query e=%s", err.Error())
				<-q.conns
				return
			}

			if res != 1 {
				logger.Debugf("no queue retrieved")
				<-q.conns
				return
			}

			payload := map[string]interface{}{}
			var payload2 Test
			tx.Get(&payload2, fmt.Sprintf("SELECT * FROM %s", q.table))
			if err != nil {
				logger.Errorf("Failed  to query e=%s", err.Error())
				<-q.conns
				return
			}
			logger.Infof("%s", payload2)
			job := Q4MJob{
				payload: payload,
				queue:   q,
				tx:      tx,
			}
			outChannel <- &job

		}()
	}
}

func (q *Q4M) Stop() {}

func (q *Q4M) Init(config map[string]interface{}) {

	dsn, present := config["dsn"]
	if !present {
		panic("dns not provided")
	}
	db, err := sqlx.Connect("mysql", dsn.(string))
	if err != nil {
		panic(fmt.Sprintf("failed to connect e=%s", err.Error()))
	}
	q.db = db

	concurrency, present := config["concurrency"]
	if !present {
		panic("concurrency not provided")
	}
	//db.DB.SetMaxOpenConns(concurrency.(int))
	//db.DB.SetMaxIdleConns(concurrency.(int))
	q.conns = make(chan bool, concurrency.(int))

	table, present := config["table"]
	if !present {
		panic("table not provided")
	}
	q.table = table.(string)
}

func (q *Q4M) Abort(job *Q4MJob) {
	job.tx.QueryRow(fmt.Sprintf("SELECT queue_abort()"))
	job.tx.Commit() // For golang connection pooling
	<-q.conns
}

func (q *Q4M) End(job *Q4MJob) {
	job.tx.QueryRow(fmt.Sprintf("SELECT queue_end()"))
	job.tx.Commit() // For golang connection pooling
	<-q.conns
}
