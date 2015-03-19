// Fifo package implements queue as linux named pipe.
// Such an fifo can be created as follows(Depends on environment)
//	mkfifo /tmp/queue
// This is a feature mainly targeted for development, because when ever the host machine goes down, all queue might get lost
package fifo

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ActiveState/tail"
	"github.com/asaskevich/govalidator"
	"github.com/cihub/seelog"
	"github.com/maedama/reprow"
	"github.com/mitchellh/mapstructure"
	"os"
	"syscall"
)

func init() {
	reprow.RegisterQueue("fifo", &FifoBuilder{})
}

type FifoBuilder struct{}

func (b FifoBuilder) NewQueue(config map[string]interface{}, logger seelog.LoggerInterface) (reprow.Queue, error) {
	return NewFifo(config, logger)
}

func NewFifo(config map[string]interface{}, logger seelog.LoggerInterface) (*Fifo, error) {
	fifo := Fifo{}
	err := fifo.configure(config, logger)
	return &fifo, err
}

type Fifo struct {
	logger   seelog.LoggerInterface
	config   Config
	wantDown chan bool
	done     chan bool
}

type Config struct {
	Path string `valid:"string,required"`
}

func (f *Fifo) Start(outChannel chan reprow.Job) error {
	if f.done != nil {
		return errors.New("run called twice")
	} else {
		f.wantDown = make(chan bool)
		f.done = make(chan bool)
		go func() {
			f.run(outChannel)
			f.done <- true
		}()
		return nil
	}
}

func (f *Fifo) run(outChannel chan reprow.Job) {
	t, err := tail.TailFile(f.config.Path, tail.Config{Follow: true})
	if err != nil {
		panic("failed to tail file")
	}
	go func() {
		<-f.wantDown
		close(t.Lines)
	}()

	for line := range t.Lines {
		job := Job{
			queue: f,
		}
		err := json.Unmarshal([]byte(line.Text), &job.payload)
		if err != nil {
			f.logger.Errorf("failed to deserialize queue. skipping queue=%s err=%s", line.Text, err.Error())
		} else {
			outChannel <- &job
		}
	}
}

func (f *Fifo) Stop() error {
	if f.done == nil {
		return errors.New("not running")
	} else {
		f.wantDown <- true
		<-f.done
		return nil
	}
}

func (f *Fifo) openFifo() error {

	file, err := os.OpenFile(f.config.Path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0644)
	defer file.Close()
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Mode()&os.ModeNamedPipe == 0 {
		return errors.New("file is not a named pipe. use mkfifo to make file")
	}
	return nil
}

func (f *Fifo) Abort(job *Job) {
	fifo_w, err := os.OpenFile(f.config.Path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0644)

	if err != nil {
		f.logger.Errorf("Failed to open file e=%s", err.Error())
	}
	w := bufio.NewWriter(fifo_w)
	jsonText, err := json.Marshal(job)
	if err != nil {
		f.logger.Errorf("Failed to serialize to json e=%s", err.Error())
	} else {
		_, err := w.Write(jsonText)
		if err != nil {
			f.logger.Errorf("Failed to write json e=%s", err.Error())
		}

		_, err = w.WriteString("\n")
		if err != nil {
			f.logger.Errorf("Failed to write e=%s", err.Error())
		}

		err = w.Flush()
		if err != nil {
			f.logger.Errorf("Failed to flush  e=%s", err.Error())
		}
	}
}

func (f *Fifo) End(j *Job) {
	jsonText, _ := json.Marshal(j.payload)
	f.logger.Debugf("end job payload=%s", jsonText)
}

func (f *Fifo) configure(c map[string]interface{}, logger seelog.LoggerInterface) error {
	f.logger = logger
	var config Config
	err := mapstructure.Decode(c, &config)
	if err != nil {
		return err
	}
	_, err = govalidator.ValidateStruct(config)

	if err != nil {
		return err
	}
	f.config = config

	err = f.openFifo()
	if err != nil {
		return errors.New(fmt.Sprintf("fifo with path %s invalid by %s", f.config.Path, err.Error()))
	}
	return nil
}
