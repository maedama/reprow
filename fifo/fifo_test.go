package fifo

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/maedama/reprow"
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"time"
)

var logger, _ = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.DebugLvl)

func TestStart(t *testing.T) {
	fifoPath := tempnam()

	err := syscall.Mkfifo(fifoPath, syscall.S_IFIFO|0666)
	if err != nil {
		t.Skip("skipping test because it failed to create fifo")
	}
	defer os.Remove(fifoPath)

	queue, err := NewFifo(map[string]interface{}{
		"Path": fifoPath,
	}, logger)
	if err != nil {
		t.Fatalf("failed to make fifo queue")
	}

	stream := make(chan reprow.Job)
	queue.Start(stream)
	defer queue.Stop()

	t.Logf("testing payload")
	payload := map[string]interface{}{
		"foo":  10,
		"var":  nil,
		"hoge": []int{1},
	}

	bytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("unable to marshal %s", err.Error())
	}
	err = ioutil.WriteFile(fifoPath, bytes, 0644)
	if err != nil {
		t.Fatalf("unable to write file e=" + err.Error())
	}
	err = ioutil.WriteFile(fifoPath, []byte("\n"), 0644)
	if err != nil {
		t.Fatalf("unable to write file e=" + err.Error())
	}

	select {
	case res := <-stream:
		if !DeepEqual(res.Payload(), payload) {
			t.Errorf("payload not match got=%v exp=%v", res.Payload(), payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout reading stream")
	}

}

// This might return filename that exists. But It should not matter very much
func tempnam() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("/tmp/%x", b)
}

func DeepEqual(a map[string]interface{}, b map[string]interface{}) bool {
	AasJson, _ := json.Marshal(a)
	BasJson, _ := json.Marshal(b)
	if string(AasJson) != string(BasJson) {
		return false
	} else {
		return true
	}
}
