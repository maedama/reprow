package reprow

type Job interface {
	Payload() map[string]interface{}
	Abort()
	End()
}
