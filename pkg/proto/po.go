package proto

import (
	"fmt"
	. "github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
	"github.com/golang/glog"
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
	// post notifications or publications to this peer
	Notif(code string) (err error)
	NotifCoRun(code string, data <-chan []byte) (err error)

	Co() (co CoConv)
	CoId() string

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
	netIdent   string
	sendPacket func(payload, wireDir string) (n int64, err error)
	sendData   func(data <-chan []byte) (n int64, err error)
	closer     func() error
	ho         *HostingEndpoint

	muSend, muCo sync.Mutex
	co           *coConv
}

func (po *PostingEndpoint) NetIdent() string {
	return po.netIdent
}

func (po *PostingEndpoint) PlugWire(
	netIdent string,
	sendPacket func(payload, wireDir string) (n int64, err error),
	sendData func(data <-chan []byte) (n int64, err error),
	closer func() error,
	ho *HostingEndpoint,
) {
	po.netIdent = netIdent
	po.sendPacket = sendPacket
	po.sendData = sendData
	po.closer = closer
	po.ho = ho
}

func (po *PostingEndpoint) Notif(code string) (err error) {
	po.muSend.Lock()
	defer po.muSend.Unlock()
	_, err = po.sendPacket(code, "")
	return
}

func (po *PostingEndpoint) NotifCoRun(code string, data <-chan []byte) (err error) {
	po.muSend.Lock()
	defer po.muSend.Unlock()
	_, err = po.sendPacket(code, "corun")
	if err != nil {
		return
	}
	_, err = po.sendData(data)
	return
}

func (po *PostingEndpoint) Co() CoConv {
	po.muSend.Lock()
	po.muCo.Lock()
	if po.co != nil {
		panic(NewUsageError("Unclean co on po ?!"))
		// todo prevent deadlock ?
	}
	po.co = newCoConv(po)
	po.sendPacket(po.co.id, "co_begin")
	return po.co
}

func (po *PostingEndpoint) CoId() string {
	// todo need RLock for concurrent read ?
	if po.co == nil {
		return ""
	} else {
		return po.co.id
	}
}

func (po *PostingEndpoint) coDone(co CoConv) {
	if co != po.co {
		panic(NewUsageError("Unmatched coDone ?!"))
		// todo prevent deadlock ?
	}
	po.sendPacket(po.co.id, "co_end")
	po.co = nil
	po.muCo.Unlock()
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
			glog.Warningf("Error before closing posting wire: %+v\n", RichError(e))
		}
		if e := closer(); e != nil {
			glog.Warningf("Error when closing posting wire: %+v\n", RichError(e))
		}
	}()

	// make sure corun context cancelled as well
	if co := po.co; co != nil {
		co.Cancel(err)
	}

	if err != nil {
		po.muSend.Lock()
		defer po.muSend.Unlock()
		// try send full error info to peer before closer
		po.sendPacket(fmt.Sprintf("%+v", RichError(err)), "err")
	}
}

func (po *PostingEndpoint) Close() {
	po.Cancel(nil)
}
