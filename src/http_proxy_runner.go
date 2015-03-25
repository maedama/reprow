package reprow

import (
	"github.com/parnurzeal/gorequest"
	"time"
)

type HttpProxyRunner struct{}

func (r *HttpProxyRunner) Init()                   {}
func (r *HttpProxyRunner) MaximumConcurrency() int { return 1 }

func (r *HttpProxyRunner) Run(job Job) bool {
	resp, _, err := gorequest.New().Post("http://127.0.0.1:5000").
		Timeout(2000*time.Millisecond).
		Set("Authorization", "Bearer Test").
		Send(job).
		End()

	if err != nil {
		logger.Errorf("backend response not retrieved:%s", err)
		job.Abort()
	} else {
		switch resp.StatusCode {
		case 200:
			job.End()
		default:
			logger.Errorf("backend returned invalid status code:%d", resp.StatusCode)
			job.Abort()
		}
	}
	return true
}
