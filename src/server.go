package reprow

import (
	"fmt"
	"github.com/cihub/seelog"
	"os"
	"os/signal"
	"syscall"
)

var (
	logger seelog.LoggerInterface
)

type Server struct {
	queue  Queue
	runner Runner
}

func (s *Server) Init() {
	var err error
	logger, err = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.DebugLvl)
	if err != nil {
		panic(err)
	}
	/*s.queue = &FifoQueue{}
	s.queue.Init(map[string]interface{}{"path": "/tmp/queue"})
	*/
	s.queue = &Q4M{}
	s.queue.Init(map[string]interface{}{
		"dsn":         "root@tcp(127.0.0.1:13306)/reprow_test",
		"table":       "test_queue",
		"concurrency": 1,
	})

	s.runner = &HttpProxyRunner{}
	s.runner.Init()
}

func (s *Server) Run() {
	defer seelog.Flush()

	jobChannel := make(chan Job)
	runners := make(chan bool, s.runner.MaximumConcurrency()) // Limiting concurrency
	go func() {
		for job := range jobChannel {
			runners <- true
			done := make(chan bool)
			go func(job Job) {
				done <- s.runner.Run(job)
			}(job)
			<-done
			<-runners
		}
	}()
	go func() {
		s.queue.Start(jobChannel)
	}()

	exit := make(chan int)
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(
			sigCh,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT)

		for {
			sig := <-sigCh
			switch sig {
			default:
				fmt.Println("signal captured")
				exit <- 1
			}
		}
	}()

	exitCode := <-exit
	os.Exit(exitCode)
}
