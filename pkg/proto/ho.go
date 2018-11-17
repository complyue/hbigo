package proto

import (
	"fmt"
	"io"
	"net"
	"strings"
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

		poRcvr: nil, //
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

	// a queue of posting tasks yielded from hosting conversations, the tasks will be
	// executed by posting loop
	coPoQue chan CoPoTask

	// object receiving channel of the current ack'ed posting conversation
	poRcvr chan interface{}
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

func (ho *HostingEndpoint) CoRecvObj() (interface{}, error) {
	result, ok, err := ho.landOne()
	if !ok {
		panic(errors.NewUsageError("Nothing sent over wire but an object expected."))
	}
	return result, err
}

func (ho *HostingEndpoint) CoRecvData(data <-chan []byte) (err error) {
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

	// posting loop for hosting conversations
	go func() {
		var coID string

		defer func() {
			// disconnect wire, don't crash the whole process
			if err := recover(); err != nil {
				e := errors.RichError(err)
				glog.Errorf("HBI unexpected error in posting from conversation [%s]: %+v", coID, e)
				ho.Cancel(e)
			}
		}()

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
				po.acquireSendTicket()
				defer po.releaseSendTicket()

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

	// landing loop, drive receiving and landing of packets
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
				return
			default:
			}
			result, ok, err := ho.landOne()
			if err != nil {
				panic(err)
			}
			if ok {
				poRcvr := ho.poRcvr
				if poRcvr != nil {
					if glog.V(3) {
						glog.Infof("Sending landed object to posting receiver ...")
					}
					select {
					case <-ho.Done():
						return
					case poRcvr <- result:
						// object sent to posting conversation
					case <-time.After(PoRecvTimeout):
						err := errors.Errorf("Posting receiver timeout after %+v", PoRecvTimeout)
						ho.Cancel(err)
						return
					}
					if glog.V(3) {
						glog.Infof("Sent landed object to posting receiver.")
					}
				}
			}
		}

	}()

}

// todo make this a configurable option
const PoRecvTimeout = 10 * time.Second

// land next packet and return the result
func (ho *HostingEndpoint) landOne() (gotObj interface{}, ok bool, err error) {
	if ho.closer == nil {
		// shortcut if hosting endpoint disconnected by peer detected previously
		glog.Warningf("Landing attempted against a disconnected HBI wire %+v.", ho.netIdent)
		return nil, false, nil
	}

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

	// blocking read next packet
	if glog.V(3) {
		glog.Infof("HBI wire %+v receiving next packet ...", ho.netIdent)
	}

	var pkt *Packet
	pkt, err = ho.recvPacket()
	if err != nil {
		if err == io.EOF && pkt != nil {
			// got last packet before disconnected, land it for now,
			// todo confirm next landing attempt should get EOF again without undesired side-effects,
			// note this might be platform specific.
			err = nil
		} else {
			// treat receiving error as fatal, and fully disconnect (i.e.
			// cancel the connection context at all)
			ho.Cancel(err)
			return nil, false, err
		}
	}
	if pkt == nil {
		// mostly the tcp connection has been disconnected,

		glog.V(1).Infof("HBI peer %s disconnected.", ho.netIdent)

		// clear closer to avoid closing the disconnected socket.
		// todo will here something leaked ?
		ho.closer = nil

		// done with this wire
		ho.Cancel(err)

		return nil, false, err
	}
	if glog.V(3) {
		glog.Infof("HBI wire %+v landing packet: ...\n%+v\n", ho.netIdent, pkt)
	}

	var execResult interface{}
	if strings.HasPrefix(pkt.WireDir, "coget:") {
		execResult, ok, err = ho.Exec(pkt.Payload)
		if err != nil {
			panic(err)
		} else if !ok {
			panic(errors.NewPacketError("coget code exec to void ?!", ho.netIdent, pkt.WireDir, pkt.Payload))
		}
		serialization := pkt.WireDir[6:]
		if len(serialization) <= 0 {
			// simple value, no serialization,
			// i.e. use native textual representation of hosting language
			ho.coPoQue <- CoPoTask{entry: func(po *PostingEndpoint) {
				if _, err = po.sendPacket(fmt.Sprintf("%#v", execResult), ""); err != nil {
					panic(err)
				}
			}}
		} else if strings.HasPrefix(serialization, "bson:") {
			// structured value, use hinted bson serialization
			bsonHint := serialization[5:]
			ho.coPoQue <- CoPoTask{entry: func(po *PostingEndpoint) {
				if err := po.sendBSON(execResult, bsonHint); err != nil {
					panic(err)
				}
			}}
		} else {
			panic(errors.NewPacketError(fmt.Sprintf(
				"unsupported coget serialization: %+v", serialization,
			), ho.netIdent, pkt.WireDir, pkt.Payload))
		}
		// done with coget, no other wireDir interpretations
		return nil, false, nil
	}

	switch pkt.WireDir {
	case "":
		// Note: landOne() may be recursively called from the executed script
		if execResult, ok, err = ho.Exec(pkt.Payload); err != nil {
			// panic to stop the loop, will be logged by deferred err handler above
			panic(errors.NewPacketError(err, ho.netIdent, pkt.WireDir, pkt.Payload))
		} else {
			if glog.V(3) {
				glog.Infof(
					"Vanilla packet landed \n ** HBI-CODE **\n%s\n -- HBI-CODE --, result: ok=%v, %+v",
					pkt.Payload, ok, execResult,
				)
			}
			return execResult, ok, err
		}

	case "corun":
		panic(errors.New("Removed from protocol."))

	case "co_begin":
		coID := pkt.Payload

		if glog.V(3) {
			glog.Infof("Beginning hosting conversation [%s] ...", coID)
		}
		// queue beginning co id, after hosting mutex unlocked to avoid deadlock
		ho.coPoQue <- CoPoTask{coID: coID}

	case "co_end":
		coID := pkt.Payload
		if glog.V(3) {
			glog.Infof("Ending hosting conversation [%s] ...", coID)
		}
		// queue ending co id, after hosting mutex unlocked to avoid deadlock
		ho.coPoQue <- CoPoTask{coID: coID}

	case "co_ack_begin":
		// ack to co_begin
		switch p2p := ho.PoToPeer(); p2p.(type) {
		case *PostingEndpoint:
			coID := pkt.Payload
			po := p2p.(*PostingEndpoint)
			co := po.currCo()
			if co != nil && co.id == coID {
				// current posting conversation matched ack'ed co id,
				// set its object receiving channel as posting (active) receiver
				ho.poRcvr = co.chObj
				if glog.V(3) {
					glog.Infof("Posting conversation %s acked, receiver %p armed.", co.id, co.chObj)
				}
			} else {
				// the corresponding local posting conversation has already finished without
				// blocking recv
				// todo detect & report violating cases

				// clear poRcvr
				ho.poRcvr = nil
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
			co := po.currCo()
			if co != nil && co.id == coID {
				// current posting conversation matched ack'ed co id,
				// verify its object receiving channel is current posting (active) receiver and clear
				if ho.poRcvr != co.chObj {
					panic(errors.NewWireError("Posting co's object receiving channel not on receiver stack top ?!"))
				}
			}
			// clear poRcvr anyway
			ho.poRcvr = nil
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
		return nil, false, err
	default:
		panic(errors.NewPacketError("Unexpected packet", ho.netIdent, pkt.WireDir, pkt.Payload))
	}

	return nil, false, nil
}

func (ho *HostingEndpoint) Cancel(err error) {
	// close the done channel now, if the hosting endpoint still appears connected to subsequent checks,
	// recursive cancellations may come unexpectedly.
	ho.HoContext.Cancel(err)

	// cancel posting endpoint if still connected, it tends to use RLock, so process before WLock below, or deadlock!
	if p2p := ho.PoToPeer(); p2p != nil && !p2p.Cancelled() {
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
