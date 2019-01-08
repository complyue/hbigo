package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/complyue/hbigo/pkg/proto"
	"github.com/golang/glog"
	"github.com/peterh/liner"
)

func init() {
	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err := flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %s", err)
		}
	}
}

type hbiInterpreter struct {
	hbi.HoContext
	Prompt string
}

func (interp *hbiInterpreter) Dial(addr string) {
	fmt.Printf("Dialing %s ...\n", addr)
	fmt.Printf("  ** not impl. yet, sorry.\n")
}

func main() {
	flag.Parse()

	defer func() {
		if e := recover(); e != nil {
			glog.Errorf("Unexpected error: %+v", errors.RichError(e))
			os.Exit(3)
		}
	}()

	ctx := &hbiInterpreter{
		HoContext: hbi.NewHoContext(),
		Prompt:    "HBI:> ",
	}
	proto.PrepareHosting(ctx)
	ctx.Put("print", fmt.Println)

	line := liner.NewLiner()
	defer line.Close()
	line.SetCtrlCAborts(true)

	var (
		code   string
		result interface{}
		ok     bool
		err    error
	)

	for {

		code, err = line.Prompt(fmt.Sprintf(ctx.Prompt))
		if err != nil {
			switch err {
			case io.EOF: // Ctrl^D
			case liner.ErrPromptAborted: // Ctrl^C
			default:
				panic(errors.RichError(err))
			}
			break
		}
		if len(code) < 1 {
			continue
		}
		line.AppendHistory(code)

		result, ok, err = ctx.Exec(code)
		if err != nil {
			fmt.Printf("[Error]: %+v\n", err)
		} else if !ok {
			fmt.Printf("[Nay]: %+v\n", result)
		} else {
			fmt.Printf("%#v\n", result)
		}

	}

	println("\nBye.")

}
