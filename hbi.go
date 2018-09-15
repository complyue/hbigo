/*
Hosting Based Interfacing for Go 1

HBI connectivity, including:
	notif (pub)
	corun (sub or pub)
	coget (rpc)
	binary stream

TODO:
	resource aware flow control w/ back pressure
	certification based connection security, crypto/tls

*/
package hbi

import (
	"github.com/complyue/hbigo/pkg/conn"
	"github.com/complyue/hbigo/pkg/proto"
)

type (
	HoContext = proto.HoContext
	TCPConn   = conn.TCPConn

	Hosting = proto.Hosting
	Posting = proto.Posting
	Conver  = proto.Conver
)

var (
	NewHoContext = proto.NewHoContext

	ServeTCP = conn.ServeTCP
	DialTCP  = conn.DialTCP
)
