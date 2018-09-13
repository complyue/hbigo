package proto

import (
	"fmt"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"io"
	"net"
)

type Hosting interface {
	HoContext
	HoCtx() HoContext

	// identity from the network's view
	NetIdent() string
	LocalAddr() net.Addr
	RemoteAddr() net.Addr

	// local hosting conversation id
	CoId() string

	// receive an inbound data object created by landing scripts sent by peer
	// the scripts is expected to be sent from peer by `co.SendCode()`
	CoRecvObj() (result interface{}, err error)

	// receive an inbound binary data stream sent by peer
	// actually it's expected to be sent from peer by `co.SendData()`, the size and layout
	// should have been deducted from previous received data objects
	CoRecvData(data <-chan []byte) (err error)
}

func NewHostingEndpoint(ctx HoContext) *HostingEndpoint {
	PrepareHosting(ctx)
	return &HostingEndpoint{
		HoContext: ctx,
		netIdent:  "?!?",
		chObj:     make(chan interface{}),
	}
}

type HostingEndpoint struct {
	HoContext

	coId string

	// Should be set by implementer
	netIdent              string
	localAddr, remoteAddr net.Addr
	recvPacket            func() (*Packet, error)
	recvData              func(data <-chan []byte) (n int64, err error)
	closer                func() error

	// used to pump landed objects from landing loop goro to application goro
	chObj chan interface{}
}

func (ho *HostingEndpoint) HoCtx() HoContext {
	return ho.HoContext
}

func (ho *HostingEndpoint) CoId() string {
	ho.RLock()
	defer ho.RUnlock()
	return ho.coId
}

func (ho *HostingEndpoint) setCoId(coId string) {
	ho.Lock()
	defer ho.Unlock()
	ho.coId = coId
}

func (ho *HostingEndpoint) NetIdent() string {
	return ho.netIdent
}

func (ho *HostingEndpoint) LocalAddr() net.Addr {
	return ho.localAddr
}

func (ho *HostingEndpoint) RemoteAddr() net.Addr {
	return ho.remoteAddr
}

func (ho *HostingEndpoint) PlugWire(
	netIdent string, localAddr, remoteAddr net.Addr,
	recvPacket func() (*Packet, error),
	recvData func(data <-chan []byte) (n int64, err error),
	closer func() error,
) {
	ho.netIdent = netIdent
	ho.localAddr = localAddr
	ho.remoteAddr = remoteAddr
	ho.recvPacket = recvPacket
	ho.recvData = recvData
	ho.closer = closer
}

func (ho *HostingEndpoint) CoRecvObj() (result interface{}, err error) {
	if ho.CoId() == "" {
		panic(errors.NewUsageError("Called without conversation ?!"))
	}

	result, err = ho.recvObj()
	return
}

func (ho *HostingEndpoint) recvObj() (result interface{}, err error) {
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
	if ho.CoId() == "" {
		panic(errors.NewUsageError("Called without conversation ?!"))
	}

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
			e := errors.RichError(err)
			glog.Errorf("HBI landing error: %+v", e)
			ho.Cancel(e)
		}
	}()

	// receive proceeding signal channel
	chProc := make(chan struct{})

	// packet channel
	chPkt := make(chan Packet)
	// a packet contains just two strings,
	// it's more optimal to pass value over channel

	// packet receiving loop
	go func() {
		defer func() {
			// disconnect wire, don't crash the whole process
			if err := recover(); err != nil {
				e := errors.RichError(err)
				glog.Errorf("HBI receiving error: %+v", e)
				ho.Cancel(e)
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
				if err == io.EOF {
					// live with EOF by far, pkt may be nil or not in this case
				} else {
					// treat receiving error as fatal, and fully disconnect (i.e.
					// cancel the connection context at all)
					ho.Cancel(err)
					return
				}
			}
			if pkt != nil {
				// blocking put packet for landing
				chPkt <- *pkt
				pkt = nil // eager release mem
			} else {
				// mostly the tcp connection has been disconnected,
				glog.V(1).Infof("HBI peer %s disconnected.", ho.netIdent)
				// clear closer to avoid closing the disconnected socket. todo will here something leaked ?
				ho.closer = nil
				// do close so the posting endpoint if present, get closed
				ho.Close()

				// done for now
				return
				// todo do not return when auto-re-connect is implemented, in which case continue here,
				// expecting a proceed signal on chProc after auto-re-connected,
				// then we will be receiving next packet from the new tcp socket.
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
				"HBI wire %s landing stopped err=%+v",
				ho.NetIdent(), ho.Err(),
			)
			return
		case pkt := <-chPkt:
			switch pkt.WireDir {
			case "":
				if result, ok, err := ho.Exec(pkt.Payload); err != nil {
					// panic to stop the loop, will be logged by deferred err handler above
					panic(errors.NewPacketError(err, pkt))
				} else if ok {
					if ho.coId != "" {
						// in conversation, send it via chObj to a pending CoRecvObj() call
						ho.chObj <- result
					} else {
						// not in conversation, drop anyway, todo warn about it ?
					}
				} else {
					// no result from execution, nop
				}
			case "corun":
				// unidirectional wire, assume implicit co_begin/co_end
				if ho.coId != "" {
					panic(errors.NewPacketError("corun reentrance ?!", pkt))
				}
				func() {
					ho.setCoId(fmt.Sprintf("%p", &pkt.Payload))
					defer func() {
						ho.setCoId("")
					}()
					if _, _, err := ho.Exec(pkt.Payload); err != nil {
						panic(errors.NewPacketError("exec failure", pkt))
					}
				}()
			case "coget":
				if ho.coId == "" {
					panic(errors.NewWireError("coget without conversation ?!"))
				}
				switch p2p := ho.PoToPeer(); po := p2p.(type) {
				case *PostingEndpoint:
					if result, ok, err := ho.Exec(pkt.Payload); err != nil {
						panic(errors.NewPacketError("exec failure", pkt))
					} else if ok {
						if _, err := po.sendPacket(fmt.Sprintf("%#v", result), ""); err != nil {
							panic(err)
						}
					} else {
						panic(errors.NewPacketError("coget code exec to void ?!", pkt))
					}
				case nil:
					panic(errors.NewPacketError("coget via unidirectional wire ?!", pkt))
				default:
					panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
				}
			case "co_begin":
				if ho.coId != "" {
					panic(errors.NewPacketError(fmt.Sprintf(
						"Unexpected co_begin [%s]=>[%s]", ho.coId, pkt.Payload,
					), pkt))
				}
				switch p2p := ho.PoToPeer(); po := p2p.(type) {
				case *PostingEndpoint:
					po.muSend.Lock()
					po.muCo.Lock()
					ho.setCoId(pkt.Payload)
					if _, err := po.sendPacket(pkt.Payload, "co_ack"); err != nil {
						panic(err)
					}
				case nil:
					ho.setCoId(pkt.Payload)
				default:
					panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
				}
			case "co_end":
				if pkt.Payload != ho.coId {
					panic(errors.NewPacketError(fmt.Sprintf(
						"Unexpected co_end [%s]!=[%s]", pkt.Payload, ho.coId,
					), pkt))
				}
				switch p2p := ho.PoToPeer(); po := p2p.(type) {
				case *PostingEndpoint:
					ho.setCoId("")
					if _, err := po.sendPacket("", "co_ack"); err != nil {
						panic(err)
					}
					po.muCo.Unlock()
					po.muSend.Unlock()
				case nil:
					ho.setCoId("")
				default:
					panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
				}
			case "co_ack":
				switch p2p := ho.PoToPeer(); po := p2p.(type) {
				case *PostingEndpoint:
					if pkt.Payload != "" {
						// ack to co_begin
						// must match local posting co id, set as local hosting co id
						localCoId := ""
						func() {
							po.RLock()
							defer po.RUnlock()
							if po.co != nil {
								localCoId = po.co.id
							}
						}()
						if pkt.Payload != localCoId {
							panic(errors.NewPacketError(fmt.Sprintf(
								"Unexpected co begin ack [%s]!=[%s]", pkt.Payload, localCoId,
							), pkt))
						}
					} else {
						// ack to co_end
						// nothing more to do than clearing local hosting co id
						// todo necessary to split co_ack into co_ack_begin+co_ack_end ?
					}
					// set or clear local hosting co id
					ho.setCoId(pkt.Payload)
				case nil:
					ho.setCoId(pkt.Payload)
				default:
					panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
				}
			case "err":
				// peer error occurred, todo give context package opportunity to handle peer error
				glog.Error("HBI disconnecting due to peer error: ", pkt.Payload)
				ho.Close()
				return
			default:
				panic(errors.NewPacketError("Unexpected packet", pkt))
			}
		}

		// signal next packet receiving
		chProc <- struct{}{}
	}

}

func (ho *HostingEndpoint) Cancel(err error) {
	// make sure the done channel is closed anyway
	defer ho.HoContext.Cancel(err)

	// cancel posting endpoint if still connected, it tends to use RLock, so process before WLock below, or deadlock!
	if p2p := ho.PoToPeer(); p2p != nil {
		p2p.Cancel(err)
	}

	ho.Lock()
	defer ho.Unlock()

	closer := ho.closer
	if closer == nil {
		// do close only once, if ho.closer is nil, it's already closed
		return
	}
	ho.closer = nil
	// cut the wire at last anyway
	defer func() {
		if e := recover(); e != nil {
			glog.Warningf("Error before closing hosting wire: %+v\n", errors.RichError(e))
		}
		if e := closer(); e != nil {
			glog.Warningf("Error when closing hosting wire: %+v\n", errors.RichError(e))
		}
	}()

}

func (ho *HostingEndpoint) Close() {
	ho.Cancel(nil)
}
