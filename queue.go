package reprow

import (
	"github.com/cihub/seelog"
)

var (
	queues = make(map[string]QueueBuilder)
)

// Queue describes interface for queue backend implementation.
type Queue interface {
	Start(chan Job) error // Dequeue function should receive output channel for jobs and start dequeue cycle in background
	Stop() error          // When ever stop dequeue is called, queue would block until dequeue cycle is  try to stop it's dequeue
}

// QueueBuilder is interface for building queue instances.
// When ever own queue is implemented, queue builder should be registered via RegisterQueue functions
type QueueBuilder interface {
	NewQueue(config map[string]interface{}, logger seelog.LoggerInterface) (Queue, error) // Generate new queue
}

// RegisterQueue is used to register queue to reprow systems.
// It should be called in init functions for each queue implementations.
func RegisterQueue(name string, queue QueueBuilder) {
	if queue == nil {
		panic("reprow: Queue is nil")
	}
	if _, dup := queues[name]; dup {
		panic("reprow: Register called twice for queue " + name)
	}
	queues[name] = queue
}
