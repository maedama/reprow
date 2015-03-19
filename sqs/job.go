package sqs

import (
	"github.com/goamz/goamz/sqs"
)

type Job struct {
	queue     *SQS
	message   *sqs.Message
	finalized chan bool
}

func (j *Job) Queue() *SQS {
	return j.queue
}

func (j *Job) Payload() map[string]interface{} {
	return map[string]interface{}{
		"Body":                   j.message.Body,
		"MessageId":              j.message.MessageId,
		"MD5OfBody":              j.message.MD5OfBody,
		"ReceiptHandle":          j.message.ReceiptHandle,
		"MessageAttribute":       j.message.MessageAttribute,
		"MD5OfMessageAttributes": j.message.MD5OfMessageAttributes,
	}
}
func (j *Job) Abort(retryAfter int) {
	j.queue.Abort(j, retryAfter)
}
func (j *Job) End() {
	j.queue.End(j)
}

func (j *Job) WaitFinalize() bool {
	return <-j.finalized
}
