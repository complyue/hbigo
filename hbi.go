/*
Hosting Based Interfacing for Go 1

HBI connectivity, including:
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
	hbierrs "github.com/complyue/hbigo/pkg/errors"
	"github.com/complyue/hbigo/pkg/proto"
	"github.com/golang/glog"
	"log"
)

type (
	WireError  = hbierrs.WireError
	UsageError = hbierrs.UsageError
	HoContext  = proto.HoContext
	Hosting    = proto.Hosting
	Posting    = proto.Posting
)

var (
	NewHoContext = proto.NewHoContext

	ServeTCP = conn.ServeTCP

	//MakeTCP = conn.MakeTCP
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
