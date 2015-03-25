package reprow

type Runner interface {
	Init()
	Run(Job) bool
	MaximumConcurrency() int
}
