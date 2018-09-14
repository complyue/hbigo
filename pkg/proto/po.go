package proto

import (
	"fmt"
	"github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/glog"
	"net"
	"sync"
)

/*
The context for a service posting endpoint.
*/
type Posting interface {
	// be a cancellable context
	CancellableContext

	// identity from the network's view
	NetIdent() string
	LocalAddr() net.Addr
	RemoteAddr() net.Addr

	// post notifications (it subscribed to or other publications) to the peer
	Notif(code string) (err error)
	// post notifications with a binary data stream
	NotifCoRun(code string, data <-chan []byte) (err error)

	Ho() Hosting

	// send code to remote conversation
	CoSendCode(code string) (err error)
	// send data to remote conversation
	CoSendData(<-chan []byte) (err error)
	// send a bson map to remote conversation
	CoSendBSON(m bson.M) error

	// initiate a local conversation
	// a conversation will hog the underlying posting wire until closed,
	// during which course other traffics, including notifications and other conversations will queue up.
	// so the shorter conversations be, the higher overall system throughput will gain.
	Co() (co Conver, err error)

	Close()
}

func NewPostingEndpoint() *PostingEndpoint {
	return &PostingEndpoint{
		CancellableContext: NewCancellableContext(),
	}
}

type PostingEndpoint struct {
	// embed a cancellable context
	CancellableContext

	// Should be set by implementer
	netIdent              string
	localAddr, remoteAddr net.Addr
	sendPacket            func(payload, wireDir string) (n int64, err error)
	sendData              func(data <-chan []byte) (n int64, err error)
	closer                func() error

	ho *HostingEndpoint

	muSend sync.Mutex
	co     *conver
}

func (po *PostingEndpoint) NetIdent() string {
	return po.netIdent
}

func (po *PostingEndpoint) LocalAddr() net.Addr {
	return po.localAddr
}

func (po *PostingEndpoint) RemoteAddr() net.Addr {
	return po.remoteAddr
}

func (po *PostingEndpoint) PlugWire(
	netIdent string, localAddr, remoteAddr net.Addr,
	sendPacket func(payload, wireDir string) (n int64, err error),
	sendData func(data <-chan []byte) (n int64, err error),
	closer func() error,
	ho *HostingEndpoint,
) {
	po.netIdent = netIdent
	po.localAddr = localAddr
	po.remoteAddr = remoteAddr
	po.sendPacket = sendPacket
	po.sendData = sendData
	po.closer = closer
	po.ho = ho
}

func (po *PostingEndpoint) Notif(code string) (err error) {
	defer func() {
		if err != nil {
			// in case sending error occurred, just log & close the wire
			glog.Error(errors.RichError(err))
			// .Cancel(err) would cause more error than success
			po.ho.Close()
		}
	}()
	po.muSend.Lock()
	defer po.muSend.Unlock()
	if _, err = po.sendPacket(code, ""); err != nil {
		return
	}
	return
}

func (po *PostingEndpoint) NotifCoRun(code string, data <-chan []byte) (err error) {
	defer func() {
		if err != nil {
			// in case sending error occurred, just log & close the wire
			glog.Error(errors.RichError(err))
			// .Cancel(err) would cause more error than success
			po.ho.Close()
		}
	}()
	po.muSend.Lock()
	defer po.muSend.Unlock()
	if _, err = po.sendPacket(code, "corun"); err != nil {
		return
	}
	if _, err = po.sendData(data); err != nil {
		return
	}
	return
}

func (po *PostingEndpoint) Ho() Hosting {
	return po.ho
}

func (po *PostingEndpoint) CoSendCode(code string) (err error) {
	if po.ho.CoId() == "" {
		panic(errors.NewUsageError("CoSend without hosting conversation ?!"))
	}
	_, err = po.sendPacket(code, "")
	return
}

func (po *PostingEndpoint) CoSendData(data <-chan []byte) (err error) {
	if po.ho.CoId() == "" {
		panic(errors.NewUsageError("CoSend without hosting conversation ?!"))
	}
	_, err = po.sendData(data)
	return
}

func (po *PostingEndpoint) CoSendBSON(m bson.M) error {
	buf, err := bson.Marshal(m)
	if err != nil {
		return err
	}
	if glog.V(1) {
		glog.Infof("HBI wire %s sending BSON of %d bytes.", po.netIdent, len(buf))
	}
	bc := make(chan []byte, 1)
	bc <- buf
	close(bc)
	if err = po.CoSendCode(fmt.Sprintf(`
CoRecvBSON(%v)
`, len(buf))); err != nil {
		return err
	}
	if nSent, err := po.sendData(bc); err != nil {
		return err
	} else if glog.V(1) {
		glog.Infof("HBI wire %s sent BSON of %d bytes.", po.netIdent, nSent)
	}
	return nil
}

func (po *PostingEndpoint) Co() (co Conver, err error) {
	po.muSend.Lock()
	defer func() {
		if e := recover(); e != nil {
			err = errors.RichError(e)
		}
		if err != nil {
			if po.co == co {
				po.co = nil
			}
			co = nil
			po.muSend.Unlock()
			po.Cancel(err)
		}
	}()
	if po.co != nil {
		panic(errors.NewUsageError("Unclean co on po ?!"))
		// todo prevent deadlock ?
	}
	po.co = newConver(po)
	_, err = po.sendPacket(po.co.id, "co_begin")
	if err != nil {
		return
	}
	co = po.co
	return
}

func (po *PostingEndpoint) coDone(co Conver) {
	if co != po.co {
		panic(errors.NewUsageError("Unmatched coDone ?!"))
		// todo prevent deadlock ?
	}
	po.sendPacket(po.co.id, "co_end")
	po.co = nil
	po.muSend.Unlock()
}

func (po *PostingEndpoint) Cancel(err error) {
	// make sure the done channel is closed anyway
	defer po.CancellableContext.Cancel(err)

	po.Lock()
	defer po.Unlock()

	closer := po.closer
	if closer == nil {
		// do close only once, if po.closer is nil, it's already closed
		return
	}
	po.closer = nil
	// cut the wire at last anyway
	defer func() {
		if e := recover(); e != nil {
			glog.Warningf("Error before closing posting wire: %+v\n", errors.RichError(e))
		}
		if e := closer(); e != nil {
			glog.Warningf("Error when closing posting wire: %+v\n", errors.RichError(e))
		}
	}()

	if err != nil { // try send full error info to peer before closer
		if po.ho != nil && po.ho.coId != "" {
			// in a hosting conversation,
			// muSend should have been locked in this case
		} else if po.co != nil {
			// in a posting conversation,
			// muSend should have been locked in this case
		} else {
			// todo other cases may deadlock?
			po.muSend.Lock()
			defer po.muSend.Unlock()
		}
		// don't care possible error
		_, _ = po.sendPacket(fmt.Sprintf("%+v", errors.RichError(err)), "err")
	}

	// make sure corun context cancelled as well
	if co := po.co; co != nil {
		co.Cancel(err)
	}
}

func (po *PostingEndpoint) Close() {
	po.Cancel(nil)
}
