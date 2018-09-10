package main

import (
	"flag"
	"github.com/complyue/hbigo"
	. "github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"github.com/peterh/liner"
	"io"
	"log"
	"os"
)

func init() {
	var err error

	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err = flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %s", err)
		}
	}

}

func main() {
	var err error
	defer func() {
		// disconnect wire, don't crash the whole process
		if err := recover(); err != nil {
			glog.Errorf("Unexpected error: %+v", RichError(err))
			os.Exit(3)
		}
	}()

	flag.Parse()

	var hbic *hbi.TCPConn
	hbic, err = hbi.DialTCP(hbi.NewHoContext(), "localhost:3232")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer hbic.Close()

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	for {

		if hbic.Posting.Cancelled() {
			break
		}

		code, err := line.Prompt("hbi-chat> ")
		if err != nil {
			switch err {
			case io.EOF: // Ctrl^D
			case liner.ErrPromptAborted: // Ctrl^C
			default:
				panic(err)
			}
			break
		}

		hbic.Notif(code)
	}

	log.Printf("Bye.")

}
