// SQS package implements sqs as reprow.Queue
package sqs

import (
	"errors"
	"fmt"
	"github.com/asaskevich/govalidator"
	"github.com/cihub/seelog"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/sqs"
	"github.com/maedama/reprow"
	"github.com/mitchellh/mapstructure"
	"strconv"
	"sync"
	"time"
)

var (
	MaxNumberOfMessages = 10
)

func init() {
	reprow.RegisterQueue("sqs", &SQSBuilder{})
}

type SQSBuilder struct{}

func (b *SQSBuilder) NewQueue(config map[string]interface{}, logger seelog.LoggerInterface) (reprow.Queue, error) {
	return NewSQS(config, logger)
}

func NewSQS(config map[string]interface{}, logger seelog.LoggerInterface) (*SQS, error) {
	sqs := SQS{}
	err := sqs.configure(config, logger)
	return &sqs, err
}

type SQS struct {
	queue         *sqs.Queue
	logger        seelog.LoggerInterface
	config        Config
	wantDown      bool
	done          chan bool
	bufferTimeout time.Duration
}

type Config struct {
	AccessKeyId       string `valid:"string,required" mapstructure:"access_key_id"`
	SecretAccessKey   string `valid:"string,required" mapstructure:"secret_access_key"`
	Region            string `valid:"string,required"`
	Url               string `valid:"string,required"`
	VisibilityTimeout int    `valid:"int,required" mapstructure:"visibility_timeout"`
	MaxConcurrency    int    `valid:"int,required" mapstructure:"max_concurrency"`
	BufferTimeout     string `valid:"required" mapstructure:"buffer_timeout"`
}

func (s *SQS) Start(outChannel chan reprow.Job) error {
	if s.done != nil {
		return errors.New("Start called twice")
	} else {
		s.done = make(chan bool)
		go func() {
			s.run(outChannel)
			s.done <- true
		}()
		return nil
	}
}

func (s *SQS) run(outChannel chan reprow.Job) {

	semaphore := make(chan bool, s.config.MaxConcurrency)
	var wg sync.WaitGroup

	for s.wantDown == false {
		semaphore <- true
		wg.Add(1)

		// Job prepartion is done in single goroutine.
		// This will allow as to make request in single request as much as possible.
		// If in future, there is a need to pararellize this process
		// it is recommended to take sync.Mutex.Lock() when writing to output channel inorder to achieve similar property
		jobs := make([]*Job, 0, MaxNumberOfMessages)
		for i := 0; i < MaxNumberOfMessages; i++ {
			job := Job{
				queue:     s,
				finalized: make(chan bool),
			}
			var sent bool

			select {
			case outChannel <- &job:
				sent = true
			case <-time.After(s.bufferTimeout):
				s.logger.Debugf("Buffering timeout reached proceed to making a request")
				sent = false
			}

			if sent {
				jobs = append(jobs, &job)
			} else {
				break
			}
		}

		if len(jobs) > 0 {
			go func() {
				s.finalizeJobs(jobs)
				<-semaphore
				wg.Done()
			}()
		} else {
			<-semaphore
			wg.Done()
		}
	}
	wg.Wait()
}

func (s *SQS) finalizeJobs(jobs []*Job) {

	params := map[string]string{
		"MaxNumberOfMessages": strconv.Itoa(len(jobs)),
		"VisibilityTimeout":   strconv.Itoa(s.config.VisibilityTimeout),
		"WaitTimeSeconds":     "10", //TODO
	}

	resp, err := s.queue.ReceiveMessageWithParameters(params)
	for i, job := range jobs {
		if err != nil || i >= len(resp.Messages) {
			job.finalized <- false
		} else {
			job.message = &resp.Messages[i]
			job.finalized <- true
			s.logger.Infof("reprow/sqs: created job Id=%s", job.message.MessageId)
		}
	}
	if len(resp.Messages) == 0 {
		s.logger.Infof("no message retrieved")
	}
	if err != nil {
		s.logger.Errorf("Failed to receive message e=%s", err.Error())
		<-time.After(time.Second)
	}

}

func (s *SQS) Stop() error {
	if s.done == nil {
		return errors.New("it's not running")
	} else {
		s.logger.Infof("Waiting for all alive long polling request to finish. It will take arround 10 seconds")
		s.wantDown = true
		<-s.done
		return nil
	}
}

func (s *SQS) Abort(job *Job, retryAfter int) {
	s.logger.Debugf("reprow/sqs: aborting job Id=%s retryAfter:%d", job.message.MessageId, retryAfter)
	_, err := s.queue.ChangeMessageVisibility(job.message, retryAfter)
	if err != nil {
		s.logger.Errorf("Fatal error when aborting sqs job e=%s", err.Error())
	}
}

func (s *SQS) End(job *Job) {
	_, err := s.queue.DeleteMessage(job.message)
	if err != nil {
		s.logger.Errorf("Fatal error when aborting sqs job id=%s, e=%s", job.message.MessageId, err.Error())
	}
	s.logger.Debugf("reprow/sqs: ending job Id=%s", job.message.MessageId)
}

func (s *SQS) configure(c map[string]interface{}, logger seelog.LoggerInterface) error {
	s.wantDown = false

	s.logger = logger

	var config Config
	err := mapstructure.Decode(c, &config)
	if err != nil {
		return err
	}

	_, err = govalidator.ValidateStruct(config)

	if err != nil {
		return err
	}

	s.config = config

	region, present := aws.Regions[config.Region]
	if !present {
		return errors.New(fmt.Sprintf("unknown region r = %s", region))
	}

	var auth = aws.Auth{
		AccessKey: config.AccessKeyId,
		SecretKey: config.SecretAccessKey,
	}

	s.bufferTimeout, err = time.ParseDuration(config.BufferTimeout)
	if err != nil {
		return errors.New("buffer_timeout failed to parse: " + err.Error())
	}

	conn := sqs.New(auth, region)
	q := conn.QueueFromArn(config.Url)
	s.queue = q
	return nil

}
