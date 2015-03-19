// q4m package implements q4m as reprow.Queue.
// q4m is a message queue implemented as mysql storage engine see http://q4m.github.io/ for detail
package q4m

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/arnehormann/sqlinternals/mysqlinternals"
	"github.com/asaskevich/govalidator"
	"github.com/cihub/seelog"
	_ "github.com/go-sql-driver/mysql"
	"github.com/maedama/reprow"
	"github.com/mitchellh/mapstructure"
	"strconv"
	"sync"
)

func init() {
	reprow.RegisterQueue("q4m", &Q4MBuilder{})
}

type Q4MBuilder struct{}

func (b Q4MBuilder) NewQueue(config map[string]interface{}, logger seelog.LoggerInterface) (reprow.Queue, error) {
	return NewQ4M(config, logger)
}

func NewQ4M(config map[string]interface{}, logger seelog.LoggerInterface) (*Q4M, error) {
	q4m := Q4M{}
	err := q4m.configure(config, logger)
	return &q4m, err
}

// Q4M implements q4m as reprow.Queue
type Q4M struct {
	DB       *sql.DB
	logger   seelog.LoggerInterface
	config   Config
	wantDown bool
	running  bool
	wg       sync.WaitGroup
}

type Config struct {
	Dsn   string `valid:"string,required"`
	Table string `valid:"string,required"`
}

func (q *Q4M) Start(outChannel chan reprow.Job) error {
	if q.running == true {
		return errors.New("Dequeue already called")
	} else {
		q.running = true
		go q.run(outChannel)
		return nil
	}
}

func (q *Q4M) run(outChannel chan reprow.Job) {

	for q.wantDown == false {
		job := Job{
			ready: make(chan bool),
			queue: q,
		}
		outChannel <- &job
		q.wg.Add(1)
		go func(job *Job) {
			defer job.finalize()
			defer q.wg.Done()
			tx, err := q.DB.Begin()
			if err != nil {
				q.logger.Errorf("Failed to make new transaction e=%s", err.Error())
				return
			}
			job.tx = tx
			//TODO: queue wait timeout is currently set to 5 as hard coded value
			// Changing this values will affect, time it takes to safully shutting down, because currently there is no signal handling done arround here

			row := tx.QueryRow(fmt.Sprintf("SELECT queue_wait(\"%s\", 5)", q.config.Table))
			var res int
			err = row.Scan(&res)
			if err != nil {
				q.logger.Errorf("Failed for to query e=%s", err.Error())
				return
			}

			if res != 1 {
				q.logger.Debugf("no queue retrieved")
				return
			}

			//var payload2 Test
			row = tx.QueryRow(fmt.Sprintf("SELECT * FROM %s", q.config.Table))
			payload, err := rowToMap(row)
			if err != nil {
				q.logger.Errorf("Failed for to get first row err=%s", err.Error())
				return
			}
			job.payload = payload
			job.queue = q
		}(&job)
	}
}

func (q *Q4M) Stop() error {
	q.logger.Infof("stopping queue")
	if q.running == false {
		return errors.New("not running")
	} else {
		q.wantDown = true
		q.wg.Wait()
		q.running = false
		q.wantDown = false
		return nil
	}
}

func (q *Q4M) Abort(job *Job) {
	q.abortTx(job.tx)
}

func (q *Q4M) End(job *Job) {
	q.endTx(job.tx)
}

func (q *Q4M) abortTx(tx *sql.Tx) {
	var res int
	q.logger.Debugf("aborting transaction")
	row := tx.QueryRow(fmt.Sprintf("select queue_abort()"))
	err := row.Scan(&res)

	if err != nil {
		q.logger.Errorf("abort failed err=%s", err.Error())
	}
	if res != 1 {
		q.logger.Errorf("response is not 1 for queue_abort")
	}

	tx.Commit() // For golang connection pooling
}
func (q *Q4M) endTx(tx *sql.Tx) {
	var res int
	row := tx.QueryRow(fmt.Sprintf("select queue_end()"))
	err := row.Scan(&res)
	if err != nil {
		q.logger.Errorf("abort failed err=%s", err.Error())
	}
	if res != 1 {
		q.logger.Errorf("response is not 1 for queue_abort")
	}

	tx.Commit() // For golang connection pooling
}

func rowToMap(row *sql.Row) (map[string]interface{}, error) {
	columns, err := mysqlinternals.Columns(row)
	if err != nil {
		return nil, err
	}
	scanArgs := make([]interface{}, len(columns))
	values := make([][]byte, len(columns))
	record := make(map[string]interface{})

	for i, _ := range values {
		scanArgs[i] = &(values[i])
	}

	err = row.Scan(scanArgs...)
	if err != nil {
		return nil, err
	}

	for i, col := range columns {
		if values[i] == nil {
			record[col.Name()] = nil
		} else if col.IsText() {
			record[col.Name()] = string(values[i])
		} else if col.IsInteger() {
			record[col.Name()], _ = strconv.ParseInt(string(values[i]), 10, 64)
		} else if col.IsFloatingPoint() {
			record[col.Name()], _ = strconv.ParseFloat(string(values[i]), 64)
		} else if col.IsBlob() {
			record[col.Name()] = values[i]
		} else {
			record[col.Name()] = string(values[i])
		}
	}

	return record, nil

}

func (q *Q4M) configure(c map[string]interface{}, logger seelog.LoggerInterface) error {

	q.wantDown = false

	q.logger = logger

	var config Config

	err := mapstructure.Decode(c, &config)
	if err != nil {
		return err
	}
	_, err = govalidator.ValidateStruct(config)

	if err != nil {
		return err
	}

	q.config = config

	db, err := sql.Open("mysql", config.Dsn)
	if err != nil {
		return err
	}
	q.DB = db

	return nil
}
