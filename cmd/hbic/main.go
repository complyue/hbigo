package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"github.com/peterh/liner"
)

func init() {
	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err := flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %+v", err)
		}
	}
}

var (
	peerAddr    string
	startChoppy bool
	remoteMode  bool
)

func init() {
	flag.StringVar(&peerAddr, "peer", "localhost:3232", "HBI peer address")
	flag.BoolVar(&startChoppy, "choppy", false, "Start in choppy mode")
	flag.BoolVar(&remoteMode, "remote", false, "Start in remote mode")
}

func promptCmdUsage() {
	fmt.Print(`Commands:
  :l
  :local
    switch to local mode

  :r
  :remote
    switch to remote mode
`)
}

func main() {
	var err error
	defer func() {
		if err := recover(); err != nil {
			glog.Errorf("Unexpected error: %+v", errors.RichError(err))
			os.Exit(3)
		}
		println("\nBye.\n")
	}()

	flag.Parse()

	dc := NewDiagnosticContext()
	dc.Choppy = startChoppy
	dc.Put("dc", dc) // make it self aware

	var hbic *hbi.TCPConn
	hbic, err = hbi.DialTCP(dc, peerAddr)
	if err != nil {
		panic(errors.Wrap(err, "Connection error"))
	}
	defer hbic.Close()

	remoteLiner := liner.NewLiner()
	defer remoteLiner.Close()
	remoteLiner.SetCtrlCAborts(true)
	localLiner := liner.NewLiner()
	defer localLiner.Close()
	localLiner.SetCtrlCAborts(true)

	var (
		RemotePrompt = fmt.Sprintf("hbi#%s> ", hbic.Conn.RemoteAddr())
		LocalPrompt  = fmt.Sprintf("hbi#%s> ", hbic.Conn.LocalAddr().(*net.TCPAddr).IP)
		line         = localLiner
		prompt       = LocalPrompt
		code         = ""
	)

	if remoteMode {
		line = remoteLiner
		prompt = RemotePrompt
		err = hbic.Notif(`
reveal("API Available:\n%s", API)
`)
		if err != nil {
			panic(err)
		}
	}

	// plant some common artifacts into local context
	_, _, err = dc.HoContext.Exec(`
p2p := PoToPeer()
co, err := p2p.Co()
if err == nil {
  co.Close()
  co = nil
}
`)
	if err != nil {
		panic(err)
	}

	// wait a little while, for possible server msg flushing to come shown,
	// before prompt.
	time.Sleep(200 * time.Millisecond)

	for !hbic.Posting.Cancelled() {

		code, err = line.Prompt(prompt)
		if err != nil {
			switch err {
			case liner.ErrPromptAborted: // Ctrl^C
				hbic.Cancel(err)
				fallthrough
			case io.EOF: // Ctrl^D
				err = nil // shield from deferred handler
				return
			}
			panic(errors.RichError(err))
		}

		if len(code) < 1 {
			continue
		}
		if code[0] == '?' || code == "h" || code == "help" {
			promptCmdUsage()
			continue
		}
		if code[0] == ':' {
			// cmd entered
			switch code[1:] {
			case "r":
				fallthrough
			case "remote":
				remoteMode = true
				line = remoteLiner
				prompt = RemotePrompt
			case "l":
				fallthrough
			case "local":
				remoteMode = false
				line = localLiner
				prompt = LocalPrompt
			case "?":
				fallthrough
			case "h":
				fallthrough
			case "help":
				promptCmdUsage()
			default:
				fmt.Printf("Unknown cmd: %s\n", code[1:])
			}
			continue
		}

		line.AppendHistory(code)

		if remoteMode {
			hbic.Notif(code)
		} else {
			var (
				result interface{}
				ok     bool
			)
			result, ok, err = dc.HoContext.Exec(code)
			if err != nil {
				fmt.Printf("%+v\n", err)
			}
			if ok {
				fmt.Printf("Out[local]:\n%+v\n", result)
			} else {
				// eval-ed to nothing
			}
		}
	}

}
