package proto

import (
	"fmt"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/glog"
	"io"
	"net"
	"strings"
	"sync"
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
	// the data stream is expected to be sent from peer by `co.SendData()`, the size and layout
	// should have been deducted from previous scripting landing or received data objects
	CoRecvData(data <-chan []byte) (err error)
}

func NewHostingEndpoint(ctx HoContext) *HostingEndpoint {
	PrepareHosting(ctx)
	return &HostingEndpoint{
		HoContext: ctx,
		netIdent:  "?!?",
		chObj:     make(chan interface{}),
		cndRecv:   sync.NewCond(new(sync.Mutex)),
		chPkt:     make(chan Packet),
	}
}

const AdhocCoId = "<adhoc>"

type HostingEndpoint struct {
	HoContext

	coId string

	// Should be set by implementer
	netIdent              string
	localAddr, remoteAddr net.Addr
	recvPacket            func() (*Packet, error)
	recvData              func(data <-chan []byte) (n int64, err error)
	closer                func() error

	// hosting mutex, all peer code landing activities should be sync-ed by this lock
	muHo sync.Mutex

	// used to pipe landed objects to receivers
	chObj chan interface{}

	// condition that packet receiving can proceed
	cndRecv  *sync.Cond
	procRecv bool

	// packet channel
	chPkt chan Packet
	// a packet contains just two strings,
	// it's more optimal to pass value over channel
}

func (ho *HostingEndpoint) HoCtx() HoContext {
	return ho.HoContext
}

func (ho *HostingEndpoint) CoId() string {
	return ho.coId
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
	if ho.coId == "" {
		panic(errors.NewUsageError("CoRecvObj called without conversation ?!"))
	}

	result, err = ho.recvObj()
	return
}

func (ho *HostingEndpoint) recvObj() (interface{}, error) {
	select {
	case result := <-ho.chObj:
		// most normal case, got result
		return result, nil
	case <-ho.Done():
		// disconnected
		return nil, ho.Err()
	}
}

func (ho *HostingEndpoint) CoRecvData(data <-chan []byte) (err error) {
	if ho.coId == "" {
		panic(errors.NewUsageError("CoRecvData called without conversation ?!"))
	}

	_, err = ho.recvData(data)
	return
}

func (ho *HostingEndpoint) recvBSON(nBytes int, booter interface{}) (interface{}, error) {
	if nBytes <= 0 { // short circuit logic
		return booter, nil
	}

	buf := make([]byte, nBytes)
	bc := make(chan []byte, 1)
	bc <- buf
	close(bc)

	if _, err := ho.recvData(bc); err != nil {
		return nil, err
	}

	if booter == nil {
		booter = map[string]interface{}{}
	}
	if err := bson.Unmarshal(buf, booter); err != nil {
		return nil, err
	}
	return booter, nil
}

func (ho *HostingEndpoint) StartLandingLoops() {

	// start 1 goro running packet receiving loop
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
					ho.NetIdent(), ho.Err(),
				)
				return
			}

			// blocking read next packet
			if glog.V(3) {
				glog.Infof("HBI wire %+v receiving next packet ...", ho.netIdent)
			}
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
				if glog.V(3) {
					glog.Infof("HBI wire %+v handing over packet: ...\n%+v\n",
						ho.netIdent, pkt)
				}
				ho.chPkt <- *pkt
				if glog.V(3) {
					glog.Infof("HBI wire %+v handed over packet:\n%+v\n",
						ho.netIdent, pkt)
				}
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

			// wait proceeding condition before attempt to receive next packet,
			// as landing of the packet may start receiving of streaming binary data,
			// in which case we should let recvData() be called before next packet recv here
			ho.procRecv = false
			for !ho.procRecv { // todo better defer unlock or not ?
				ho.cndRecv.L.Lock()
				if glog.V(3) {
					glog.Infof("HBI wire %+v pending recv next packet ...", ho.netIdent)
				}
				ho.cndRecv.Wait()
				ho.cndRecv.L.Unlock()
			}
		}
	}()

	// primary landing loop, receive & land packets where appropriate
	go func() {
		for {

			select {
			case <-ho.Done():
				// got disconnected
				return
			case pkt := <-ho.chPkt:
				// got next packet to land

				// do land this packet
				gotObjs, err := ho.landOne(pkt)
				if err != nil {
					// should have logged err & disconnected, stop goro here
					return
				}
				// pump result to receivers
				if len(gotObjs) > 0 {
					for _, o := range gotObjs {
						ho.chObj <- o
					}
				}

				// signal next packet receiving
				func() { // todo better defer unlock or not ?
					ho.cndRecv.L.Lock()
					ho.procRecv = true
					ho.cndRecv.Broadcast()
					ho.cndRecv.L.Unlock()
				}()

			}

		}
	}()

}

// return data object(s) to be sent to chObj, by landing the specified packet
func (ho *HostingEndpoint) landOne(pkt Packet) (gotObjs []interface{}, err error) {
	ho.muHo.Lock()
	defer ho.muHo.Unlock()

	defer func() {
		// disconnect wire, don't crash the whole process
		if e := recover(); e != nil {
			err = errors.RichError(e)
		}
		if err != nil {
			glog.Errorf("HBI landing error: %+v", err)
			ho.Cancel(err)
		}
	}()

	if strings.HasPrefix(pkt.WireDir, "coget:") {
		if ho.coId == "" {
			panic(errors.NewWireError("coget without conversation ?!"))
		}
		result, ok, err := ho.Exec(pkt.Payload)
		if err != nil {
			panic(err)
		} else if !ok {
			panic(errors.NewPacketError("coget code exec to void ?!", pkt))
		}
		serialization := pkt.WireDir[6:]
		switch p2p := ho.PoToPeer(); po := p2p.(type) {
		case *PostingEndpoint:
			if len(serialization) <= 0 {
				// simple value, no serialization,
				// i.e. use native textual representation of hosting language
				if _, err := po.sendPacket(fmt.Sprintf("%#v", result), ""); err != nil {
					panic(err)
				}
			} else if strings.HasPrefix(serialization, "bson:") {
				// structured value, use hinted bson serialization
				bsonHint := serialization[5:]
				if err := po.sendBSON(result, bsonHint); err != nil {
					panic(err)
				}
			} else {
				panic(errors.NewPacketError(fmt.Sprintf(
					"unsupported coget serialization: %+v", serialization,
				), pkt))
			}
		case nil:
			panic(errors.NewPacketError("coget via unidirectional wire ?!", pkt))
		default:
			panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
		}
		// done with coget, no other wireDir interpretations
		return nil, nil
	}

	switch pkt.WireDir {
	case "":
		if result, ok, err := ho.Exec(pkt.Payload); err != nil {
			// panic to stop the loop, will be logged by deferred err handler above
			panic(errors.NewPacketError(err, pkt))
		} else if ok {
			if ho.coId != "" {
				// in conversation, send it via chObj to a pending CoRecvObj() call
				return []interface{}{result}, nil
			} else {
				// not in conversation, drop anyway, todo warn about it ?
			}
		} else {
			// no result from execution, nop
		}
	case "corun":
		// start an ad-hoc conversation, assume implicit co_begin/co_end
		if ho.coId != "" {
			panic(errors.Errorf("corun with conversation ? %s", ho.coId))
		}

		ho.coId = AdhocCoId

		if err = ho.CoExec(pkt.Payload); err != nil {
			panic(errors.NewPacketError(err, pkt))
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
			ho.coId = pkt.Payload
			if _, err := po.sendPacket(pkt.Payload, "co_ack"); err != nil {
				panic(err)
			}
		case nil:
			ho.coId = pkt.Payload
		default:
			panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
		}
	case "co_end":
		if pkt.Payload != ho.CoId() {
			panic(errors.NewPacketError(fmt.Sprintf(
				"Unexpected co_end [%s]!=[%s]", pkt.Payload, ho.coId,
			), pkt))
		}
		switch p2p := ho.PoToPeer(); po := p2p.(type) {
		case *PostingEndpoint:
			ho.coId = ""
			if _, err := po.sendPacket("", "co_ack"); err != nil {
				panic(err)
			}
			po.muSend.Unlock()
		case nil:
			ho.coId = ""
		default:
			panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
		}
	case "co_ack":
		switch p2p := ho.PoToPeer(); p2p.(type) {
		case *PostingEndpoint:
			if pkt.Payload != "" {
				// ack to co_begin
				// the corresponding local posting conversation may have already finished without
				// blocking recv, in which case the next packet right away should be an empty co_ack
				// todo detect & report violating cases
			} else {
				// ack to co_end
				// nothing more to do than clearing local hosting co id
				// todo necessary to split co_ack into co_ack_begin+co_ack_end ?
			}
			// set or clear local hosting co id
			ho.coId = pkt.Payload
		case nil:
			ho.coId = pkt.Payload
		default:
			panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
		}
	case "err":
		// peer error occurred, todo give context package opportunity to handle peer error
		err = errors.Errorf("HBI wire %s disconnecting due to peer error:\n%s",
			ho.netIdent, pkt.Payload)
		glog.Error(err)
		ho.Close()
		return nil, err
	default:
		panic(errors.NewPacketError("Unexpected packet", pkt))
	}

	return nil, nil
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
