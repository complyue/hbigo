package proto

import (
	"fmt"
	. "github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"github.com/pkg/errors"
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
		if e := recover(); e != nil {
			var err error
			if e, ok := e.(error); ok {
				err = e
			} else {
				err = errors.New(fmt.Sprintf("%v", e))
			}
			err = errors.Wrap(err, "HBI landing error")
			glog.Errorf("%+v", err)
			ho.Cancel(err)
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
			if e := recover(); e != nil {
				var err error
				if e, ok := e.(error); ok {
					err = e
				} else {
					err = errors.New(fmt.Sprintf("%v", e))
				}
				err = errors.Wrap(err, "HBI receiving error")
				glog.Errorf("%+v", err)
				ho.Cancel(err)
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
				if result, ok, err := ho.Exec(pkt.Payload); err != nil {
					ho.Cancel(err)
					return
				} else if ok {
					if ho.coId != "" {
						// in corun conversation, send it via chObj to a pending CoRecvObj() call
						ho.chObj <- result
					} else {
						// not in corun mode, drop anyway, todo warn about it ?
					}
				} else {
					// no result from execution, nop
				}
			case "corun":
				// unidirectional wire, assume implicit co_begin/co_end
				if ho.coId != "" {
					panic(UsageError{"corun reentrance ?!"})
				}
				func() {
					if p2p := ho.PoToPeer(); p2p != nil {
						// bidirectional wire, land corun code in a corun conversation
						if p2p.CoId() != "" {
							panic(UsageError{"corun reentrance ?!"})
						}
						co := p2p.Co()
						defer co.Close()
						if _, _, err := ho.Exec(pkt.Payload); err != nil {
							ho.Cancel(err)
						}
					} else {
						ho.coId = fmt.Sprintf("%p", &pkt.Payload)
						defer func() {
							ho.coId = ""
						}()
						if _, _, err := ho.Exec(pkt.Payload); err != nil {
							ho.Cancel(err)
						}
					}
				}()
			case "coget":
				if ho.coId == "" {
					panic(WireError{"coget without corun conversation ?!"})
				}
				switch p2p := ho.PoToPeer(); po := p2p.(type) {
				case *PostingEndpoint:
					if result, ok, err := ho.Exec(pkt.Payload); err != nil {
						ho.Cancel(err)
					} else if ok {
						po.sendPacket(fmt.Sprintf("%#v", result), "")
					} else {
						panic(UsageError{"coget code landed as void ?!"})
					}
				case nil:
					panic(UsageError{"coget via unidirectional wire ?!"})
				default:
					panic(UsageError{fmt.Sprintf("Unexpected p2p type %T", p2p)})
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
