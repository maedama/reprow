package q4m

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type Job struct {
	payload map[string]interface{}
	tx      *sql.Tx
	queue   *Q4M
	ready   chan bool
}

func (j *Job) Queue() *Q4M {
	return j.queue
}

func (j *Job) Payload() map[string]interface{} {
	return j.payload
}
func (j *Job) Abort(retryAfter int) {
	if retryAfter > 0 {
		j.queue.logger.Errorf("Retry after is not supported for this queue backend")
	}
	j.queue.Abort(j)
}
func (j *Job) End() {
	j.queue.End(j)
}

func (j *Job) WaitFinalize() bool {
	return <-j.ready
}

func (job *Job) finalize() {
	if job.payload == nil {
		job.ready <- false
		if job.tx != nil {
			job.queue.abortTx(job.tx)
		}
	} else {
		job.ready <- true
	}
}
