package proto

import (
	"fmt"
	. "github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
)

type Hosting interface {
	HoContext

	// identity from the network's view
	NetIdent() string

	CoId() string

	CoRecvObj() (result interface{}, err error)

	CoRecvData(data <-chan []byte) (err error)
}

type HostingEndpoint struct {
	HoContext

	coId string

	// Should be set by implementer
	netIdent   string
	recvPacket func() (*Packet, error)
	recvData   func(data <-chan []byte) (n int64, err error)

	// used to pump landed objects from landing loop goro to application goro
	chObj chan interface{}
}

func NewHostingEndpoint(ctx HoContext) *HostingEndpoint {
	PrepareHosting(ctx)
	return &HostingEndpoint{
		HoContext: ctx,
		netIdent:  "?!?",
		chObj:     make(chan interface{}),
	}
}

func (ho *HostingEndpoint) NetIdent() string {
	return ho.netIdent
}

func (ho *HostingEndpoint) PlugWire(
	netIdent string,
	recvPacket func() (*Packet, error),
	recvData func(data <-chan []byte) (n int64, err error),
) {
	ho.netIdent = netIdent
	ho.recvPacket = recvPacket
	ho.recvData = recvData
}

func (ho *HostingEndpoint) CoId() string {
	// todo should RLock ? as only meant to be called from conversation goro
	return ho.coId
}

func (ho *HostingEndpoint) CoRecvObj() (result interface{}, err error) {
	if ho.coId == "" {
		panic(UsageError{"Not in corun mode ?!"})
	}
	result, err = ho.coRecvObj()
	return
}

func (ho *HostingEndpoint) coRecvObj() (result interface{}, err error) {
	select {
	case <-ho.Done():
		// connection context cancelled
		err = ho.Err()
	case result = <-ho.chObj:
		// most normal case, got result
	}
	return
}

func (ho *HostingEndpoint) CoRecvData(data <-chan []byte) (err error) {
	if ho.coId == "" {
		panic(UsageError{"Not in corun mode ?!"})
	}
	err = ho.coRecvData(data)
	return
}

func (ho *HostingEndpoint) coRecvData(data <-chan []byte) (err error) {
	_, err = ho.recvData(data)
	return
}

func (ho *HostingEndpoint) StartLandingLoop() {
	go ho.landingLoop()
}

func (ho *HostingEndpoint) landingLoop() {
	defer func() {
		// disconnect wire, don't crash the whole process
		if err := recover(); err != nil {
			if err, ok := err.(error); ok {
				glog.Errorf("HBI landing error: %s\n", err.Error())
				ho.Cancel(err)
			} else {
				glog.Error("HBI landing error: %s\n", err)
				ho.Close()
			}
		}
	}()

	// receive proceeding signal channel
	chProc := make(chan struct{})

	// packet channel
	chPkt := make(chan Packet)
	// a packet contains just two strings,
	// it's more optimal to pass value over channel

	// packet receiving goroutine
	go func() {
		defer func() {
			// disconnect wire, don't crash the whole process
			if err := recover(); err != nil {
				if err, ok := err.(error); ok {
					glog.Errorf("HBI receiving error: %s\n", err.Error())
					ho.Cancel(err)
				} else {
					glog.Error("HBI receiving error: %s\n", err)
					ho.Close()
				}
			}
		}()

		for {
			if ho.Cancelled() {
				// connection context cancelled
				glog.V(1).Infof(
					"HBI wire %s receiving stopped err=%#v",
					ho.NetIdent, ho.Err(),
				)
				return
			}

			// blocking read next packet
			pkt, err := ho.recvPacket()
			if err != nil {
				// treat receiving error as fatal, and fully disconnect (i.e.
				// cancel the connection context at all)
				ho.Cancel(err)
				return
			}
			if pkt != nil {
				// blocking put packet for landing
				chPkt <- *pkt
				pkt = nil // eager release mem
			} else {
				// mostly the tcp connection has been disconnected, nop now.
				// expecting a proceed signal on chProc after (auto) re-connected,
				// then we will be receiving next packet from the new tcp connection.
			}

			// wait proceeding signal before attempt to receive next packet,
			// as landing of the packet may start binary data streaming,
			// in which case we should let recvData() lock muRecv before this
			// loop does
			<-chProc
		}
	}()

	// packet landing loop
	for {
		select {
		case <-ho.Done():
			// connection context cancelled
			glog.V(1).Infof(
				"HBI wire %s landing stopped err=%#v",
				ho.NetIdent, ho.Err(),
			)
			return
		case pkt := <-chPkt:
			switch pkt.WireDir {
			case "":
				result, ok, err := ho.Exec(pkt.Payload)
				if err != nil {
					ho.Cancel(err)
					return
				}
				if ok {
					if ho.coId != "" {
						// in corun conversation, send it via chObj to a pending CoRecvObj() call
						ho.chObj <- result
					} else {
						// not in corun mode, drop anyway, todo warn about it ?
					}
				} else {
					// no result from execution, nop
				}
			case "co_begin":
				if ho.coId != "" {
					err := WireError{fmt.Sprintf(
						"Unexpected co_begin [%s]=>[%s]", ho.coId, pkt.Payload,
					)}
					glog.Error(err)
					ho.Cancel(err)
					return
				}
				switch p2p := ho.PoToPeer(); po := p2p.(type) {
				case *PostingEndpoint:
					po.muSend.Lock()
					po.muCo.Lock()
					po.sendPacket(pkt.Payload, "co_ack")
				case nil:
					// nop
				default:
					panic(UsageError{fmt.Sprintf("Unexpected p2p type %T", p2p)})
				}
				ho.coId = pkt.Payload
			case "co_end":
				if ho.coId != pkt.Payload {
					err := WireError{fmt.Sprintf(
						"Unexpected co_end [%s]!=[%s]", pkt.Payload, ho.coId,
					)}
					glog.Error(err)
					ho.Cancel(err)
					return
				}
				switch p2p := ho.PoToPeer(); po := p2p.(type) {
				case *PostingEndpoint:
					ho.coId = ""
					po.muCo.Unlock()
					po.muSend.Unlock()
				case nil:
					// nop
				default:
					panic(UsageError{fmt.Sprintf("Unexpected p2p type %T", p2p)})
				}
			case "co_ack":
				switch p2p := ho.PoToPeer(); po := p2p.(type) {
				case *PostingEndpoint:
					if pkt.Payload != po.co.Id() {
						err := WireError{fmt.Sprintf(
							"Unexpected co_ack [%s]!=[%s]", pkt.Payload, po.co.Id,
						)}
						glog.Error(err)
						ho.Cancel(err)
						return
					}
				case nil:
					// nop
				default:
					panic(UsageError{fmt.Sprintf("Unexpected p2p type %T", p2p)})
				}
			case "corun":
			case "coget":
			case "err":
				// peer error occurred, todo give context package opportunity to handle peer error
				glog.Fatal("HBI disconnecting due to peer error: ", pkt.Payload)
				ho.Close()
			default:
				panic("Unexpected packet: " + pkt.String())
			}
		}

		// signal next packet receiving
		chProc <- struct{}{}
	}

}
