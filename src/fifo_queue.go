package reprow

import (
	"bufio"
	"encoding/json"
	"github.com/ActiveState/tail"
	"os"
	"syscall"
)

type FifoJob struct {
	payload map[string]interface{}
	queue   *FifoQueue
}

func (j *FifoJob) Queue() *FifoQueue {
	return j.queue
}

func (j *FifoJob) Payload() map[string]interface{} {
	return j.payload
}
func (j *FifoJob) Abort() {
	j.queue.Abort(j)
}
func (j *FifoJob) End() {
	j.queue.End(j)
}

type FifoQueue struct {
	fifo_path string
	fifo_r    *os.File
}

func (f *FifoQueue) Start(outChannel chan Job) {

	t, err := tail.TailFile(f.fifo_path, tail.Config{Follow: true})
	if err != nil {
		panic("failed to tail file")
	}

	for line := range t.Lines {
		job := FifoJob{
			queue: f,
		}
		err := json.Unmarshal([]byte(line.Text), &job.payload)
		if err != nil {
			logger.Errorf("failed to deserialize queue. skipping queue=%s err=%s", line.Text, err.Error())
		} else {
			outChannel <- &job
		}
	}
}

func (f *FifoQueue) Stop() {}

func (f *FifoQueue) Init(config map[string]interface{}) {
	path, present := config["path"]
	if !present {
		panic("path to linux fifo is requried")
	}
	f.fifo_path = path.(string)
	f.openFifoQueue()
}

func (f *FifoQueue) openFifoQueue() {
	fifo_r, err := os.OpenFile(f.fifo_path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0644)
	if err != nil {
		panic(err.Error())
	}

	stat, err := fifo_r.Stat()
	if err != nil {
		panic(err.Error())
	}

	if stat.Mode()&os.ModeNamedPipe == 0 {
		panic("fifo file is required")
	}

	f.fifo_r = fifo_r
}

func (f *FifoQueue) Abort(job *FifoJob) {
	fifo_w, err := os.OpenFile(f.fifo_path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0644)

	if err != nil {
		logger.Errorf("Failed to open file e=%s", err.Error())
	}
	w := bufio.NewWriter(fifo_w)
	jsonText, err := json.Marshal(job)
	if err != nil {
		logger.Errorf("Failed to serialize to json e=%s", err.Error())
	} else {
		_, err := w.Write(jsonText)
		if err != nil {
			logger.Errorf("Failed to write json e=%s", err.Error())
		}

		_, err = w.WriteString("\n")
		if err != nil {
			logger.Errorf("Failed to write e=%s", err.Error())
		}

		err = w.Flush()
		if err != nil {
			logger.Errorf("Failed to flush  e=%s", err.Error())
		}
	}
}

func (q *FifoQueue) End(j *FifoJob) {
	jsonText, _ := json.Marshal(j.payload)
	logger.Debugf("end job payload=%s", jsonText)
}
