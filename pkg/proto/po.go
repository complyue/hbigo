package proto

import (
	. "github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
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
	ho *HostingEndpoint,
) {
	po.netIdent = netIdent
	po.sendPacket = sendPacket
	po.sendData = sendData
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
		panic(UsageError{"Unclean co on po ?!"})
		// todo prevent deadlock ?
	}
	po.co = newCoConv(po)
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
		panic(UsageError{"Unmatched coDone ?!"})
		// todo prevent deadlock ?
	}
	po.co = nil
	po.muCo.Unlock()
	po.muSend.Unlock()
}

func (po *PostingEndpoint) Cancel(err error) {
	po.Lock()
	defer po.Unlock()
	// make sure corun context cancelled as well
	if co := po.co; co != nil {
		po.co = nil
		co.Cancel(err)
	}
	po.CancellableContext.Cancel(err)
}

func (po *PostingEndpoint) Close() {
	po.Lock()
	defer po.Unlock()
	// make sure posting context closed with hosting context
	if co := po.co; co != nil {
		po.co = nil
		co.Close()
	}
	// make sure done channel closed
	po.Cancel(nil)
}
