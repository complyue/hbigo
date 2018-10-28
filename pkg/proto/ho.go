package proto

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/complyue/hbigo/pkg/errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/glog"
)

type Hosting interface {
	HoContext
	HoCtx() HoContext

	// send code to the remote conversation.
	// must be in a passive local conversation responding to the remote conversation.
	CoSendCode(code string)

	// send a bson object, which may be a map or a struct value, to remote conversation.
	// must be in a passive local conversation responding to the remote conversation.
	// the `hint` string can be empty for remote to receive a `bson.M`,
	// or it must be a valid Go expression evaluates to a map, or a pointer to a struct,
	// whose type is either unnamed, or must be available within remote hosting context.
	CoSendBSON(o interface{}, hint string)

	// send a binary data stream to remote conversation.
	// must be in a passive local conversation responding to the remote conversation.
	CoSendData(<-chan []byte)

	// identity from the network's view
	NetIdent() string
	LocalAddr() net.Addr
	RemoteAddr() net.Addr

	// receive an inbound data object created by landing scripts sent by peer
	// the scripts is expected to be sent from peer by `co.SendCode()`
	CoRecvObj() (result interface{}, err error)

	// receive an inbound binary data stream sent by peer
	// the data stream is expected to be sent from peer by `co.SendData()`, the size and layout
	// should have been deducted from previous scripting landing or received data objects
	CoRecvData(data <-chan []byte) (err error)
}

type CoPoTask struct {
	coID  string                    // hosting (passive) conversation id
	entry func(po *PostingEndpoint) // closure to perform a posting task
}

func NewHostingEndpoint(ctx HoContext) *HostingEndpoint {
	PrepareHosting(ctx)
	return &HostingEndpoint{
		HoContext: ctx,
		netIdent:  "?!?",

		coPoQue: make(chan CoPoTask, 50), // todo fine tune this buffer size
		hoRcvr:  nil,
		poRcvr:  nil,

		chRecv: make(chan struct{}),
		chPkt:  make(chan Packet),
	}
}

type HostingEndpoint struct {
	HoContext

	// Should be set by implementer
	netIdent              string
	localAddr, remoteAddr net.Addr
	recvPacket            func() (*Packet, error)
	recvData              func(data <-chan []byte) (n int64, err error)
	closer                func() error

	// hosting mutex, all peer code landing activities should be sync-ed by this lock
	muHo sync.Mutex

	// a queue of posting tasks yielded from hosting conversations, the tasks will be
	// executed by posting loop
	coPoQue chan CoPoTask
	// the hosting (passive) object receiver
	hoRcvr chan interface{}
	// the posting (active) object receiver
	poRcvr chan interface{}

	// channel to signal proceed of packet receiving
	chRecv chan struct{}

	// packet channel
	chPkt chan Packet
	// a packet contains just two strings,
	// it's more optimal to pass value over channel
}

func (ho *HostingEndpoint) HoCtx() HoContext {
	return ho.HoContext
}

func (ho *HostingEndpoint) CoSendCode(code string) {
	ho.coPoQue <- CoPoTask{entry: func(po *PostingEndpoint) {
		if _, err := po.sendPacket(code, ""); err != nil {
			panic(err)
		}
	}}
}

func (ho *HostingEndpoint) CoSendData(data <-chan []byte) {
	ho.coPoQue <- CoPoTask{entry: func(po *PostingEndpoint) {
		if _, err := po.sendData(data); err != nil {
			panic(err)
		}
	}}
}

func (ho *HostingEndpoint) CoSendBSON(o interface{}, hint string) {
	ho.coPoQue <- CoPoTask{entry: func(po *PostingEndpoint) {
		if err := po.sendBSON(o, hint); err != nil {
			panic(err)
		}
	}}
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
	if ho.hoRcvr == nil {
		panic(errors.NewUsageError("CoRecvObj called without conversation ?!"))
	}

	result, err = ho.recvObj()
	return
}

func (ho *HostingEndpoint) recvObj() (interface{}, error) {
	// err if already disconnected due to error
	select {
	case <-ho.Done():
		err := ho.Err()
		if err != nil {
			return nil, errors.Wrapf(err, "Wire already disconnected due to error.")
		}
	default:
		// fall through
	}

	if ho.hoRcvr == nil {
		return nil, errors.NewUsageError("Receive is not possible in this case.")
	}
	chObj := ho.hoRcvr

	// block to receive
	select {
	case result := <-chObj:
		// most normal case, got result
		return result, nil
	case <-ho.Done():
		// disconnected
		if err := ho.Err(); err != nil {
			// disconnected due to error, propagate the error
			return nil, errors.Wrapf(err, "Wire already disconnected due to error.")
		}
		// disconnected normally, give a second chance to receive an object
		select {
		case result := <-chObj:
			return result, nil
		case <-time.After(1 * time.Millisecond):
			return nil, errors.New("Wire already disconnected.")
		}
	}
}

func (ho *HostingEndpoint) CoRecvData(data <-chan []byte) (err error) {
	if ho.hoRcvr == nil {
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
		booter = bson.M{}
	}
	if err := bson.Unmarshal(buf, booter); err != nil {
		return nil, err
	}
	return booter, nil
}

func (ho *HostingEndpoint) StartLandingLoops() {

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
					ho.NetIdent(), ho.Err(),
				)
				return
			}

			// blocking read next packet
			if glog.V(3) {
				glog.Infof("HBI wire %+v receiving next packet ...", ho.netIdent)
			}

			// reset receive proceeding state before receiving a packet, it'll be set  by the
			// landing loop, after next received packet handed over to it, and it did the landing.
			//ho.pendNextRecv()

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
					glog.Infof("HBI wire %+v handing over packet: ...\n%+v\n", ho.netIdent, pkt)
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

			// wait proceeding signal before attempt to receive next packet,
			// as landing of the packet may start receiving of streaming binary data,
			// in which case we should let recvData() be called before next packet recv here
			<-ho.chRecv
		}
	}()

	// posting loop for hosting conversations
	go func() {
		defer func() {
			// disconnect wire, don't crash the whole process
			if err := recover(); err != nil {
				e := errors.RichError(err)
				glog.Errorf("HBI unexpected muSend locker error: %+v", e)
				ho.Cancel(e)
			}
		}()

		var coID string
		for {
			var poTask CoPoTask
			// fetch next conversation posting task
			select {
			case <-ho.Done(): // got disconnected
				return
			case poTask = <-ho.coPoQue:
			}

			if poTask.entry != nil {
				panic(errors.NewWireError("Hosting conversation got task before co_begin ?!"))
			}
			if poTask.coID == "" {
				panic(errors.NewWireError("Hosting conversation begin with empty co ID ?!"))
			}
			coID = poTask.coID

			po := ho.PoToPeer().(*PostingEndpoint)
			func() { // hold send mutex and maintain chObj during this hosting (passive) conversation
				po.muSend.Lock()
				defer po.muSend.Unlock()

				if _, err := po.sendPacket(coID, "co_ack_begin"); err != nil {
					panic(err)
				}
				// execute posting tasks in the queue
				for {
					select {
					case <-ho.Done(): // got disconnected
						return
					case poTask = <-ho.coPoQue:
					}

					if poTask.coID == "" { // got a posting task

						poTask.entry(po)

					} else { // got co_end

						if poTask.coID != coID {
							panic(errors.NewWireError(fmt.Sprintf("Conversation id mismatch [%s] vs [%s] ?!", poTask.coID, coID)))
						}
						if poTask.entry != nil {
							panic(errors.NewWireError("Hosting conversation got task with co_end ?!"))
						}
						if _, err := po.sendPacket(coID, "co_ack_end"); err != nil {
							panic(err)
						}
						coID = ""
						return

					}
				}
			}()
		}
	}()

	// landing loop, land packets handed over from packet receiving loop
	go func() {
		defer func() {
			// disconnect wire, don't crash the whole process
			if err := recover(); err != nil {
				e := errors.RichError(err)
				glog.Errorf("HBI unexpected landing error: %+v", e)
				ho.Cancel(e)
			}
		}()

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
					var chObj chan interface{}
					// posting (active) receiver is considered backlog if a hosting (passive) receiver is set
					if ho.hoRcvr != nil {
						chObj = ho.hoRcvr
						if glog.V(3) {
							glog.Infof("Giving landed %d object(s) to hosting receiver %p ...", len(gotObjs), chObj)
						}
					} else if ho.poRcvr != nil {
						chObj = ho.poRcvr
						if glog.V(3) {
							glog.Infof("Giving landed %d object(s) to posting receiver %p ...", len(gotObjs), chObj)
						}
					} else {
						panic(errors.NewWireError("No receiver for landed objects."))
					}
					for i, o := range gotObjs {
						if glog.V(3) {
							glog.Infof("Giving %d/%d object to receiver %p ...", i+1, len(gotObjs), chObj)
						}
						chObj <- o
					}
					if glog.V(3) {
						glog.Infof("All landed %d object(s) given to receiver %p", len(gotObjs), chObj)
					}
				}

				// signal next packet receiving
				ho.chRecv <- struct{}{}

			}

		}
	}()

}

// return data object(s) to be sent to chObj, by landing the specified packet
func (ho *HostingEndpoint) landOne(pkt Packet) (gotObjs []interface{}, err error) {
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

	var afterHoUnLock []func()
	defer func() {
		for _, f := range afterHoUnLock {
			f()
		}
	}()

	ho.muHo.Lock()
	defer ho.muHo.Unlock()

	if strings.HasPrefix(pkt.WireDir, "coget:") {
		if ho.hoRcvr == nil {
			panic(errors.NewWireError("coget without conversation ?!"))
		}
		result, ok, err := ho.Exec(pkt.Payload)
		if err != nil {
			panic(err)
		} else if !ok {
			panic(errors.NewPacketError("coget code exec to void ?!", ho.netIdent, pkt))
		}
		serialization := pkt.WireDir[6:]
		if len(serialization) <= 0 {
			// simple value, no serialization,
			// i.e. use native textual representation of hosting language
			ho.coPoQue <- CoPoTask{entry: func(po *PostingEndpoint) {
				if _, err := po.sendPacket(fmt.Sprintf("%#v", result), ""); err != nil {
					panic(err)
				}
			}}
		} else if strings.HasPrefix(serialization, "bson:") {
			// structured value, use hinted bson serialization
			bsonHint := serialization[5:]
			ho.coPoQue <- CoPoTask{entry: func(po *PostingEndpoint) {
				if err := po.sendBSON(result, bsonHint); err != nil {
					panic(err)
				}
			}}
		} else {
			panic(errors.NewPacketError(fmt.Sprintf(
				"unsupported coget serialization: %+v", serialization,
			), ho.netIdent, pkt))
		}
		// done with coget, no other wireDir interpretations
		return nil, nil
	}

	switch pkt.WireDir {
	case "":
		if result, ok, err := ho.Exec(pkt.Payload); err != nil {
			// panic to stop the loop, will be logged by deferred err handler above
			panic(errors.NewPacketError(err, ho.netIdent, pkt))
		} else if ok {
			if ho.hoRcvr != nil || ho.poRcvr != nil {
				// in conversation, send it via chObj to a pending CoRecvObj() call
				return []interface{}{result}, nil
			} else {
				// not in conversation, drop anyway, todo warn about it ?
			}
		} else {
			// no result from execution, nop
		}

	case "corun":
		if err = ho.CoExec(pkt.Payload); err != nil {
			panic(errors.NewPacketError(err, ho.netIdent, pkt))
		}

	case "co_begin":
		coID := pkt.Payload

		if glog.V(3) {
			glog.Infof("Beginning hosting conversation [%s] ...", coID)
		}

		if ho.hoRcvr != nil {
			panic(errors.NewWireError("Receiver not clean from previous hosting conversation ?!"))
		}
		// allocate a receiving channel and set as current hosting (passive) conversation object receiver
		chObj := make(chan interface{})
		ho.hoRcvr = chObj

		// signal co begin to muSend locker goro, after hosting mutex unlocked to avoid deadlock
		afterHoUnLock = append(afterHoUnLock, func() {
			ho.coPoQue <- CoPoTask{coID: coID}
		})

	case "co_end":
		coID := pkt.Payload
		if glog.V(3) {
			glog.Infof("Ending hosting conversation [%s] ...", coID)
		}
		ho.hoRcvr = nil

		// signal co end to muSend locker goro, after hosting mutex unlocked to avoid deadlock
		afterHoUnLock = append(afterHoUnLock, func() {
			ho.coPoQue <- CoPoTask{coID: coID}
		})

	case "co_ack_begin":
		// ack to co_begin
		switch p2p := ho.PoToPeer(); p2p.(type) {
		case *PostingEndpoint:
			coID := pkt.Payload
			po := p2p.(*PostingEndpoint)
			co := po.co
			if co != nil && co.id == coID {
				// current posting conversation matched ack'ed co id,
				// set its object receiving channel as posting (active) receiver
				ho.poRcvr = co.chObj
				if glog.V(3) {
					glog.Infof("Posting conversation %s acked, receiver %p armed.", co.id, co.chObj)
				}
			} else {
				// the corresponding local posting conversation has already finished without
				// blocking recv, in this case the next packet right away should be an empty co_ack
				// todo detect & report violating cases
			}
		case nil:
			panic(errors.NewUsageError("Acking co to no posting endpoint ?!"))
		default:
			panic(errors.NewUsageError(fmt.Sprintf("Unexpected p2p type %T", p2p)))
		}

	case "co_ack_end":
		// ack to co_end
		switch p2p := ho.PoToPeer(); p2p.(type) {
		case *PostingEndpoint:
			coID := pkt.Payload
			po := p2p.(*PostingEndpoint)
			if po.co != nil && po.co.id == coID {
				// current posting conversation matched ack'ed co id,
				// verify its object receiving channel is current posting (active) receiver and clear
				if ho.poRcvr != po.co.chObj {
					panic(errors.NewWireError("Posting co's object receiving channel not on receiver stack top ?!"))
				}
				ho.poRcvr = nil
			}
		case nil:
			panic(errors.NewUsageError("Acking co to no posting endpoint ?!"))
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
		panic(errors.NewPacketError("Unexpected packet", ho.netIdent, pkt))
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
