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

import "github.com/complyue/hbigo/pkg/conn"

type (
	WireError = conn.WireError

	HBIC = conn.HBIC
)

var (
	ListenTCP = conn.ListenTCP

	MakeTCP = conn.MakeTCP
)
