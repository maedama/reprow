package main

import (
	"github.com/maedama/reprow"
)

func main() {
	server := reprow.Server{}
	server.Init()
	server.Run()
}
