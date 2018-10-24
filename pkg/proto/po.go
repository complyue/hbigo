package proto

import (
	"fmt"
	"net"
	"sync"

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
	NotifCoRun(code string, data <-chan []byte) (err error)

	// the hosting endpoint
	Ho() Hosting

	// send code to the remote conversation.
	// must be in a passive local conversation responding to the remote conversation.
	CoSendCode(code string) (err error)

	// send a bson object, which may be a map or a struct value, to remote conversation.
	// must be in a passive local conversation responding to the remote conversation.
	// the `hint` string can be empty for remote to receive a `bson.M`,
	// or it must be a valid Go expression evaluates to a map, or a pointer to a struct,
	// whose type is either unnamed, or must be available within remote hosting context.
	CoSendBSON(o interface{}, hint string) error

	// send a binary data stream to remote conversation.
	// must be in a passive local conversation responding to the remote conversation.
	CoSendData(<-chan []byte) (err error)

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

	// points to muSend if locked that for sending from a passive conversation,
	// or should be nil
	coLock *sync.Mutex

	co *conver
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

func (po *PostingEndpoint) NotifBSON(code string, o interface{}, hint string) (err error) {
	defer func() {
		if err != nil {
			// in case sending error occurred, just log & close the wire
			glog.Error(errors.RichError(err))
			// .Cancel(err) would cause more error than success
			po.ho.Close()
		}
	}()
	co, err := po.Co()
	if err != nil {
		return err
	}
	defer co.Close()
	if _, err = po.sendPacket(code, "corun"); err != nil {
		return
	}
	if err = po.sendBSON(o, hint); err != nil {
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
	co, err := po.Co()
	if err != nil {
		return err
	}
	defer co.Close()
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
	if po.ho.coId == "" {
		panic(errors.NewUsageError("CoSend without hosting conversation ?!"))
	}

	if po.coLock == nil {
		po.muSend.Lock()
		po.coLock = &po.muSend
	}

	_, err = po.sendPacket(code, "")
	return
}

func (po *PostingEndpoint) CoSendData(data <-chan []byte) (err error) {
	if po.ho.coId == "" {
		panic(errors.NewUsageError("CoSend without hosting conversation ?!"))
	}

	if po.coLock == nil {
		po.muSend.Lock()
		po.coLock = &po.muSend
	}

	_, err = po.sendData(data)
	return
}

func (po *PostingEndpoint) CoSendBSON(o interface{}, hint string) error {
	if po.ho.coId == "" {
		panic(errors.NewUsageError("CoSendBSON without hosting conversation ?!"))
	}

	if po.coLock == nil {
		po.muSend.Lock()
		po.coLock = &po.muSend
	}

	return po.sendBSON(o, hint)
}

func (po *PostingEndpoint) sendBSON(o interface{}, hint string) error {
	if hint == "" {
		// empty hint leads to invalid syntax, convert to literal untyped nil for no hint,
		// and peer will receive a map[string]interface{}
		hint = "nil"
	}

	if o == nil { // short circuit logic
		_, err := po.sendPacket(fmt.Sprintf(`
recvBSON(0,%s)
`, hint), "")
		return err
	}

	buf, err := bson.Marshal(o)
	if err != nil {
		return err
	}
	bc := make(chan []byte, 1)
	bc <- buf
	close(bc)
	if _, err = po.sendPacket(fmt.Sprintf(`
recvBSON(%v,%s)
`, len(buf), hint), ""); err != nil {
		return err
	}
	if _, err := po.sendData(bc); err != nil {
		return err
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
