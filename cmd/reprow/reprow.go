package main

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/maedama/reprow"
	_ "github.com/maedama/reprow/fifo"
	_ "github.com/maedama/reprow/http_proxy"
	_ "github.com/maedama/reprow/q4m"
	_ "github.com/maedama/reprow/sqs"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type CLIOptions struct {
	ConfigFile string `short:"c" long:"config" description:"config.yaml" required:"true"`
	Help       bool   `short:"h" long:"help" description:"output this message"`
}

func main() {

	opts := &CLIOptions{}
	p := flags.NewParser(opts, flags.PrintErrors)
	_, err := p.Parse()

	if err != nil {
		fmt.Println(err)
		p.WriteHelp(os.Stdout)
		os.Exit(1)
	}

	if opts.Help {
		p.WriteHelp(os.Stdout)
		os.Exit(1)
	}

	config := make(map[interface{}]interface{})
	dat, err := ioutil.ReadFile(opts.ConfigFile)
	if err != nil {
		panic(err.Error())
	}

	err = yaml.Unmarshal(dat, &config)
	if err != nil {
		panic(err.Error())
	}

	server, err := reprow.NewServer(config)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	exitCode := server.Run()
	os.Exit(exitCode)
}
