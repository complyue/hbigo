package main

import (
	"flag"
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
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

var (
	peerAddr string
	echoMode bool
)

func init() {
	flag.StringVar(&peerAddr, "peer", "localhost:3232", "HBI peer address")
	flag.BoolVar(&echoMode, "echo", false, "Start in ECHO (po) mode")
}

func promptCmdUsage() {
	fmt.Print(`Commands:
  :po
    switch to posting mode
  :ho
    switch to hosting mode
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

	var hbic *hbi.TCPConn
	hbic, err = hbi.DialTCP(hbi.NewHoContext(), peerAddr)
	if err != nil {
		panic(errors.Wrap(err, "Connection error"))
	}
	defer hbic.Close()

	poLiner := liner.NewLiner()
	defer poLiner.Close()
	poLiner.SetCtrlCAborts(true)
	hoLiner := liner.NewLiner()
	defer hoLiner.Close()
	hoLiner.SetCtrlCAborts(true)

	const (
		HoPrompt = "hbi#ho> "
		PoPrompt = "hbi#po> "
	)
	var (
		poMode = false
		line   = hoLiner
		prompt = HoPrompt
		code   = ""
	)

	if echoMode {
		poMode = true
		line = poLiner
		prompt = PoPrompt
		err = hbic.Notif(`
import (
	"bytes"
	"fmt"
)
func echo(args ...interface{}) {
	var s bytes.Buffer
	for _, arg := range args {
		s.WriteString(fmt.Sprintf("%+v\n", arg))
	}
	PoToPeer().Notif( fmt.Sprintf("println(%#v)",s.String()) )
}

echo("API:", API)
`)
		if err != nil {
			panic(err)
		}
	}

	// plant some common artifacts into hosting env
	_, _, err = hbic.Hosting.Exec(`
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
			case "po":
				poMode = true
				line = poLiner
				prompt = "hbi#po> "
			case "ho":
				poMode = false
				line = hoLiner
				prompt = "hbi#ho> "
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

		if poMode {
			hbic.Notif(code)
		} else {
			var (
				result interface{}
				ok     bool
			)
			result, ok, err = hbic.Hosting.Exec(code)
			if err != nil {
				fmt.Printf("%+v\n", err)
			}
			if ok {
				fmt.Printf("Out[ho]:\n%+v\n", result)
			} else {
				// eval-ed to nothing
			}
		}
	}

}
