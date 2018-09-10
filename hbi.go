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
	"github.com/complyue/hbigo/pkg/conn"
	"github.com/complyue/hbigo/pkg/proto"
)

type (
	HoContext = proto.HoContext

	Hosting = proto.Hosting
	Posting = proto.Posting
)

var (
	NewHoContext = proto.NewHoContext

	ServeTCP = conn.ServeTCP
	DialTCP  = conn.DialTCP
)
