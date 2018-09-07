/*
Hosting Based Interfacing for Go 1

Status: full Go 1 support for hbi connectivity, including:
	notif (pub)
	corun (sub or pub)
	coget (rpc)
	binary stream

TODO:
	resource aware flow control w/ back pressure

*/
package hbi

import (
	"flag"
	"github.com/complyue/hbigo/pkg/conn"
	"github.com/golang/glog"
	"log"
)

type (
	WireError  = conn.WireError
	UsageError = conn.UsageError

	Context        = conn.Context
	ContextFactory = conn.ContextFactory

	Connection = conn.Connection
)

var (
	CoDone = conn.CoDone

	NewContext = conn.NewContext

	ListenTCP = conn.ListenTCP

	MakeTCP = conn.MakeTCP
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
