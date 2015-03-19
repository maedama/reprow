package http_proxy

import (
	"encoding/json"
	"errors"
	"github.com/asaskevich/govalidator"
	"github.com/cihub/seelog"
	"github.com/maedama/reprow"
	"github.com/mitchellh/mapstructure"
	"github.com/parnurzeal/gorequest"
	"strconv"
	"time"
)

func init() {
	reprow.RegisterRunner("http_proxy", &HttpProxyBuilder{})
}

type HttpProxyBuilder struct{}

func (q *HttpProxyBuilder) NewRunner(config map[string]interface{}, logger seelog.LoggerInterface) (reprow.Runner, error) {
	return NewRunner(config, logger)
}

func NewRunner(config map[string]interface{}, logger seelog.LoggerInterface) (*HttpProxy, error) {
	h := HttpProxy{}
	err := h.configure(config, logger)
	return &h, err
}

type HttpProxy struct {
	logger  seelog.LoggerInterface
	config  Config
	timeout time.Duration
}

type Config struct {
	Url               string `valid:"requri,required"`
	Concurrency       int    `valid:"int,required"`
	Timeout           string `valid:"required"`
	DefaultRetryAfter int    `valid:"int" mapstructure:"default_retry_after"`
}

func (h *HttpProxy) MaximumConcurrency() int { return h.config.Concurrency }

func (h *HttpProxy) Run(job reprow.Job) error {
	byte, _ := json.Marshal(job.Payload())
	resp, _, err := gorequest.New().Post(h.config.Url).
		Timeout(h.timeout).
		Set("Authorization", "Bearer Test").
		Send(string(byte)).
		End()

	if err != nil {
		h.logger.Errorf("backend response not retrieved:%s", err)
		job.Abort(h.config.DefaultRetryAfter)
		return errors.New("backend response not retrieved")
	} else {
		switch resp.StatusCode {
		case 200:
			job.End()
			return nil
		default:
			h.logger.Errorf("backend returned invalid status code:%d", resp.StatusCode)

			var retryAfter int
			retryAfterHeader := resp.Header.Get("Retry-After")
			if len(retryAfterHeader) > 0 {
				var err error
				retryAfter, err = strconv.Atoi(retryAfterHeader)
				if err != nil {
					h.logger.Errorf("malformed Retry-After header. integer value is the only supported value")
					retryAfter = h.config.DefaultRetryAfter
				}
			} else {
				retryAfter = h.config.DefaultRetryAfter
			}

			job.Abort(retryAfter)
			return errors.New("status code not 200")
		}
	}
}

func (h *HttpProxy) configure(c map[string]interface{}, logger seelog.LoggerInterface) error {
	h.logger = logger
	var config Config
	err := mapstructure.Decode(c, &config)
	if err != nil {
		return err
	}
	_, err = govalidator.ValidateStruct(config)

	if err != nil {
		return err
	}

	h.timeout, err = time.ParseDuration(config.Timeout)
	if err != nil {
		return errors.New("timeout failed to parse: " + err.Error())
	}

	h.config = config
	return nil
}
