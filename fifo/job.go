package fifo

type Job struct {
	payload map[string]interface{}
	queue   *Fifo
}

func (j *Job) Queue() *Fifo {
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
	return true
}
