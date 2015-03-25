package reprow

import (
	"fmt"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/sqs"
	"strconv"
	"time"
)

type SQSJob struct {
	queue   *SQS
	message *sqs.Message
}

type SQS struct {
	queue   *sqs.Queue
	timeout int
}

func (j *SQSJob) Queue() *SQS {
	return j.queue
}

func (j *SQSJob) Payload() map[string]interface{} {
	return map[string]interface{}{
		"Body":                   j.message.Body,
		"MessageId":              j.message.MessageId,
		"MD5OfBody":              j.message.MD5OfBody,
		"ReceiptHandle":          j.message.ReceiptHandle,
		"MessageAttribute":       j.message.MessageAttribute,
		"MD5OfMessageAttributes": j.message.MD5OfMessageAttributes,
	}
}
func (j *SQSJob) Abort() {
	j.queue.Abort(j)
}
func (j *SQSJob) End() {
	j.queue.End(j)
}

func (s *SQS) Start(outChannel chan Job) {
	for {
		params := map[string]string{
			"MaxNumberOfMessages": "10",
			"VisibilityTimeout":   strconv.Itoa(s.timeout),
			"WaitTimeSeconds":     "10", //TODO
		}
		resp, err := s.queue.ReceiveMessageWithParameters(params)
		if err != nil {
			logger.Errorf("Failed to receive message e=%s", err.Error())
			time.Sleep(1)
			continue
		}
		for _, message := range resp.Messages {
			job := SQSJob{
				message: &message,
				queue:   s,
			}
			outChannel <- &job
		}
	}
}

func (s *SQS) Stop() {}

func (s *SQS) Init(config map[string]interface{}) {

	accessKeyId, present := config["AWS_ACCESS_KEY_ID"]
	if !present {
		panic("Need aws key id")
	}

	secret, present := config["AWS_SECRET_ACCESS_KEY"]
	if !present {
		panic("Need secret access key")
	}

	regionName, present := config["AWS_REGION"]
	if !present {
		panic("Need region")
	}

	region, present := aws.Regions[regionName.(string)]
	if !present {
		panic(fmt.Sprintf("unknown region %s", regionName))
	}

	url, present := config["QUEUE_URL"]
	if !present {
		panic("queur url not defined")
	}

	var auth = aws.Auth{
		AccessKey: accessKeyId.(string),
		SecretKey: secret.(string),
	}

	conn := sqs.New(auth, region)
	q := conn.QueueFromArn(url.(string))
	s.queue = q

	timeout := config["VISIBILITY_TIMEOUT"]
	if !present {
		panic("visibility timeout required")
	}
	s.timeout = timeout.(int)

}

func (s *SQS) Abort(job *SQSJob) {
	_, err := s.queue.ChangeMessageVisibility(job.message, 0)
	if err != nil {
		logger.Errorf("Fatal error when aborting sqs job e=%s", err.Error())
	}
}

func (s *SQS) End(job *SQSJob) {
	_, err := s.queue.DeleteMessage(job.message)
	logger.Errorf("Fatal error when ending sqs job e=%s", err.Error())
}
