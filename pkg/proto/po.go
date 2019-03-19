package proto

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/glog"
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

	// post a notification to the peer
	Notif(code string) (err error)

	// post a notification with a bson object to the peer
	NotifBSON(code string, o interface{}, hint string) error

	// post a notification with a binary data stream to the peer
	NotifData(code string, data <-chan []byte) (err error)

	// the hosting endpoint
	Ho() Hosting

	// initiate a local conversation
	// a conversation will hog the underlying posting wire until closed,
	// during which course other traffics, including notifications and other conversations will queue up.
	// so the shorter conversations be, the higher overall system throughput will gain.
	Co() (co Conver, err error)

	Close()
}

func NewPostingEndpoint() *PostingEndpoint {
	po := &PostingEndpoint{
		CancellableContext: NewCancellableContext(),

		chSendTicket: make(chan struct{}),
		chSendDone:   make(chan struct{}),
	}

	// send ticket granting loop
	go func() {
		for {
			select {
			case <-po.CancellableContext.Done():
				// posting endpoint disconnected
				return
			case po.chSendTicket <- struct{}{}:
				// a send ticket granted
			}
			select {
			case <-po.CancellableContext.Done():
				// posting endpoint disconnected
				return
			case <-po.chSendDone:
				// a send ticket revoked
			}
		}
	}()

	return po
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

	chSendTicket, chSendDone chan struct{}

	muCoPtr sync.Mutex

	co *conver
}

func (po *PostingEndpoint) acquireSendTicket() {
	select {
	case <-po.CancellableContext.Done():
		err := po.CancellableContext.Err()
		if err == nil {
			err = errors.Errorf("Posting endpoint already disconnected.")
		}
		panic(errors.RichError(err))
	case <-po.chSendTicket:
		// got the ticket
		return
	}
}

func (po *PostingEndpoint) releaseSendTicket() {
	select {
	case <-po.CancellableContext.Done():
		err := po.CancellableContext.Err()
		if err == nil {
			err = errors.Errorf("Posting endpoint already disconnected.")
		}
		panic(errors.RichError(err))
	case po.chSendDone <- struct{}{}:
		// released the ticket
		return
	}
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
			// in case sending error occurred, just log & stop hosting for the wire
			glog.Error(errors.RichError(err))
			if !po.Cancelled() {
				po.Cancel(err)
			}
		}
	}()
	po.acquireSendTicket()
	defer po.releaseSendTicket()
	if _, err = po.sendPacket(code, ""); err != nil {
		return
	}
	return
}

func (po *PostingEndpoint) NotifBSON(code string, o interface{}, hint string) (err error) {
	defer func() {
		if err != nil {
			// in case sending error occurred, just log & stop hosting for the wire
			glog.Error(errors.RichError(err))
			if !po.Cancelled() {
				po.Cancel(err)
			}
		}
	}()
	co, e := po.Co()
	if e != nil {
		err = e
		return
	}
	defer co.Close()
	if _, err = po.sendPacket(code, ""); err != nil {
		return
	}
	bb, e := bson.Marshal(o)
	if e != nil {
		err = errors.RichError(e)
		return
	}
	if err = po.sendBSON(bb, hint); err != nil {
		return
	}
	return
}

func (po *PostingEndpoint) NotifData(code string, data <-chan []byte) (err error) {
	defer func() {
		if err != nil {
			// in case sending error occurred, just log & stop hosting for the wire
			glog.Error(errors.RichError(err))
			if !po.Cancelled() {
				po.Cancel(err)
			}
		}
	}()
	co, e := po.Co()
	if e != nil {
		err = e
		return
	}
	defer co.Close()
	if _, err = po.sendPacket(code, ""); err != nil {
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

func (po *PostingEndpoint) sendBSON(bb []byte, hint string) error {
	var scriptBytes, dataBytes int64
	var err error
	if glog.V(3) {
		defer func() {
			glog.Infof("Wire %s sent bson of %d+%d bytes, err=%v.", po.netIdent, scriptBytes, dataBytes, err)
		}()
	}
	if hint == "" {
		// empty hint leads to invalid syntax, convert to literal untyped nil for no hint,
		// and peer will receive a map[string]interface{}
		hint = "nil"
	}

	if len(bb) <= 0 { // short circuit logic
		scriptBytes, err = po.sendPacket(fmt.Sprintf(`
recvBSON(0,%s)
`, hint), "")
		return err
	}

	bc := make(chan []byte, 1)
	bc <- bb
	close(bc)
	if scriptBytes, err = po.sendPacket(fmt.Sprintf(`
recvBSON(%v,%s)
`, len(bb), hint), ""); err != nil {
		return err
	}
	if dataBytes, err = po.sendData(bc); err != nil {
		return err
	}
	return nil
}

func (po *PostingEndpoint) Co() (co Conver, err error) {
	po.acquireSendTicket()
	defer func() {
		if e := recover(); e != nil {
			err = errors.RichError(e)
		}
		if err != nil {
			if po.co == co {
				po.co = nil
			}
			co = nil
		}
		if co == nil {
			po.releaseSendTicket()
			// this wire should not be used for posting anyway, disconnect it
			po.Cancel(err)
		}
	}()

	po.muCoPtr.Lock()
	defer po.muCoPtr.Unlock()

	if po.co != nil {
		err = errors.NewUsageError("Unclean co on po ?!")
		return
	}

	po.co = newConver(po)
	co = po.co
	_, err = po.sendPacket(po.co.id, "co_begin")
	if err != nil {
		return
	}
	return
}

func (po *PostingEndpoint) currCo() (co *conver) {
	po.muCoPtr.Lock()
	co = po.co
	po.muCoPtr.Unlock()
	return
}

func (po *PostingEndpoint) coDone(co Conver) {
	po.muCoPtr.Lock()
	defer po.muCoPtr.Unlock()

	if co != po.co {
		panic(errors.NewUsageError("Unmatched coDone ?!"))
		// todo prevent deadlock ?
	}

	po.co = nil
	if _, err := po.sendPacket(co.(*conver).id, "co_end"); err != nil {
		glog.Errorf("Error sending co_end packet id=%s: %+v", co.(*conver).id, err)
		// unlock muSend before closing the posting endpoint
		po.releaseSendTicket()
		// not using po.Cancel(err), that'll try send peer error thus will deadlock,
		// as po.muSend is still held here.
		po.Close()
		return
	}
	// normal unlock
	po.releaseSendTicket()
}

func (po *PostingEndpoint) Cancel(err error) {
	if po.CancellableContext.Cancelled() {
		// do not repeat cancellation
		return
	}

	// make sure conversation if present, cancelled before posting endpoint
	if co := po.currCo(); co != nil && !co.Cancelled() {
		co.Cancel(err)
	}

	// close the done channel now, if the posting endpoint still appears connected to subsequent checks,
	// recursive cancellations may come unexpectedly.
	po.CancellableContext.Cancel(err)

	// close-to-signal error sent to peer or no need to send
	errSent := make(chan struct{})
	var closer func() error

	defer func() {
		select {
		case <-errSent:
			// error sent to peer
		case <-time.After(5 * time.Second):
			// timeout
		}

		if closer != nil {
			if e := closer(); e != nil {
				glog.Warningf("Error when closing posting wire: %+v\n", errors.RichError(e))
			}
		}
	}()

	po.Lock()
	defer po.Unlock()

	closer = po.closer
	if closer == nil {
		// do close only once, if po.closer is nil, it's already closed
		close(errSent)
		return
	}
	po.closer = nil

	if err == nil { // no err to be sent
		close(errSent)
	} else { // try send full error info to peer before closer
		go func() {
			defer func() {
				if e := recover(); e != nil {
					// don't care possible error
				}
				close(errSent)
			}()

			po.acquireSendTicket()
			defer po.releaseSendTicket()

			// don't care possible error
			_, _ = po.sendPacket(fmt.Sprintf("%+v", errors.RichError(err)), "err")
		}()
	}
}

func (po *PostingEndpoint) Close() {
	po.Cancel(nil)
}
