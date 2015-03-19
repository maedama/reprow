package reprow

import (
	"github.com/cihub/seelog"
)

var (
	runners = make(map[string]RunnerBuilder)
)

// QRunner describes interface for runner services
type Runner interface {
	Run(Job) error
	MaximumConcurrency() int
}

// RunnerBuilder is interface for building runner instances.
// When ever own runner is implemented, runner builder should be registered via RegisterRunner functions
type RunnerBuilder interface {
	NewRunner(config map[string]interface{}, logger seelog.LoggerInterface) (Runner, error)
}

// RegisterRunner is used to register runner to reprow systems.
// It should be called in init functions for each runner implementations.
func RegisterRunner(name string, runner RunnerBuilder) {
	if runner == nil {
		panic("reprow: Runner is nil")
	}
	if _, dup := runners[name]; dup {
		panic("reprow: Register called twice for runner " + name)
	}
	runners[name] = runner
}
