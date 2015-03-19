package http_proxy

import (
	"github.com/cihub/seelog"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

type TestJob struct {
	status string
}

var logger, _ = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.DebugLvl)

func (j *TestJob) Payload() map[string]interface{} { return map[string]interface{}{"foo": "var"} }
func (j *TestJob) Abort(retryAfter int)            { j.status = "aborted" }
func (j *TestJob) End()                            { j.status = "completed" }
func (j *TestJob) WaitFinalize() bool              { return true }

func TestRun(t *testing.T) {
	testRunProxyPayload(t)
	testRunResponseHandling(t)
}

func testRunProxyPayload(t *testing.T) {

	reqCh := make(chan *http.Request)
	reqBodyCh := make(chan string)

	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		reqCh <- req
		body, _ := ioutil.ReadAll(req.Body)
		reqBodyCh <- string(body)
		rw.Write([]byte("HTTP/1.0 200 OK\r\nConnection: close\r\n\r\nHello."))
	}))
	defer ts.Close()
	runner, err := NewRunner(map[string]interface{}{
		"url":                 ts.URL,
		"concurrency":         1,
		"timeout":             "1s",
		"default_retry_after": 1,
	}, logger)
	if err != nil {
		t.Fatalf("backed not configured e=" + err.Error())
	}
	job := TestJob{}
	go func() {
		err = runner.Run(&job)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}()
	req := <-reqCh
	if req.Header.Get("Content-Type") != "application/json" {
		t.Errorf("Content type not json")
	}

	body := <-reqBodyCh
	if body != "{\"foo\":\"var\"}" {
		t.Errorf("JSON payload not retrieved e=%s", body)
	}

}

func testRunResponseHandling(t *testing.T) {

	go func() {
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte("HTTP/1.0 200 OK\r\nConnection: close\r\n\r\nHello."))
		}))
		defer ts.Close()
		runner, err := NewRunner(map[string]interface{}{
			"url":                 ts.URL,
			"concurrency":         1,
			"timeout":             "1s",
			"default_retry_after": 1,
		}, logger)
		if err != nil {
			t.Fatalf("backed not configured e=" + err.Error())
		}

		job := TestJob{}
		err = runner.Run(&job)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if job.status != "completed" {
			t.Fatalf("Job not completed")
		}

	}()

	go func() {
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte("HTTP/1.0 500 OK\r\nConnection: close\r\n\r\nHello."))
		}))
		defer ts.Close()
		runner, err := NewRunner(map[string]interface{}{
			"url":                 ts.URL,
			"concurrency":         1,
			"timeout":             "1s",
			"default_retry_after": 1,
		}, logger)
		if err != nil {
			t.Fatalf("backed not configured e=" + err.Error())
		}

		job := TestJob{}
		err = runner.Run(&job)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if job.status != "aborted" {
			t.Fatalf("Job not completed")
		}
	}()

}
