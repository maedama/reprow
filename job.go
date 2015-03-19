package reprow

// Job is an interface for various Jobs.
// It's internal defers among Queue implementations.
// For example, to abort Q4M queue, we should now about which mysql session the job came from.
type Job interface {
	Payload() map[string]interface{} // Payload that is passed to runners
	Abort(retryAfter int)            // It should Abort the job. RetryAfter parameter is optional
	End()                            // It should complete a job
	WaitFinalize() bool              // For conccurrency control. Allows making sure runner is available before executing job initialization
}
