package reprow

type Queue interface {
	Start(chan Job)
	Stop()
	Init(config map[string]interface{})
}
