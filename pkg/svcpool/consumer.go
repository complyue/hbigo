package svcpool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"sync"
)

func NewConsumer(
	// service pool address
	poolAddr string,
	// factory func to construct local hosting contexts to communicate with service
	ctxFact func() hbi.HoContext,
) (consumer *Consumer, err error) {
	consumer = &Consumer{
		PoolAddr: poolAddr,
		CtxFact:  ctxFact,
		SvcConns: make(map[string]map[string]*hbi.TCPConn),
	}
	err = consumer.connectMaster()
	if err != nil {
		consumer = nil
		return
	}
	return
}

type Consumer struct {
	sync.Mutex // embed a lock to be synced for thread safety

	// service pool address
	PoolAddr string

	// HBI connection to service pool
	PoolMaster *hbi.TCPConn

	// default context factory
	CtxFact func() hbi.HoContext

	// established service connections, first by proc address string, then by tunnel id string
	// direct access not recommended, but if so, should be synced when accessing from multiple goros
	SvcConns map[string]map[string]*hbi.TCPConn
}

// call to this method should be properly synced
func (consumer *Consumer) connectMaster() (err error) {
	if consumer.PoolMaster != nil && !consumer.PoolMaster.Posting.Cancelled() {
		// still connected
		return
	}
	var poolMaster *hbi.TCPConn
	poolMaster, err = hbi.DialTCP(
		hbi.NewHoContext(), consumer.PoolAddr,
	)
	if err != nil {
		return
	}
	consumer.PoolMaster = poolMaster
	return
}

// get an HBI connection to an assigned service proc by session id
func (consumer *Consumer) GetService(
	// factory func to construct the local hosting context if a new connection must be created
	// can be nil to use consumer's default factory func
	ctxFact func() hbi.HoContext,
	// normally use an empty string for tunnel id,
	// multiple connections can be created to same service proc, one for each unique tunnel
	tunnel string,
	// stateless services can just use an empty string for session id, while sticky stay false
	// for non-empty session id:
	// 	*) if sticky is true, only one worker proc is allowed to work on this session in any time
	//      the pool master will always assign this very worker proc, unless it retired or crashed,
	// 		then a new worker process will come up to replace it, but none of the other workers can
	// 		touch the specified session.
	//  *) if sticky is false, multiple workers are allowed to work on this session
	// 		the pool master will assign a worker proc having worked on the specified session if exists
	//      if no worker has worked on this session, or all workers on this worker are busy enough,
	//		a new idle worker proc will be assigned.
	session string, sticky bool,
) (service *hbi.TCPConn, err error) {
	var procAddr string
	procAddr, err = consumer.AssignProc(session, sticky)
	if err != nil {
		return
	}

	// AssignProc is locking, don't lock before it does
	consumer.Lock()
	defer consumer.Unlock()

	tunnels, ok := consumer.SvcConns[procAddr]
	if !ok {
		tunnels = make(map[string]*hbi.TCPConn)
		consumer.SvcConns[procAddr] = tunnels
	}

	if service, ok = tunnels[tunnel]; ok {
		if service.Posting.Cancelled() {
			delete(tunnels, tunnel)
			ok = false
		} else {
			return
		}
	}

	if ctxFact == nil {
		ctxFact = consumer.CtxFact
	}
	service, err = hbi.DialTCP(ctxFact(), procAddr)
	if err != nil {
		return
	}
	tunnels[tunnel] = service

	return
}

func (consumer *Consumer) AssignProc(session string, sticky bool) (string, error) {
	consumer.Lock()
	defer consumer.Unlock()

	consumer.connectMaster()
	co, err := consumer.PoolMaster.PoToPeer().Co()
	if err != nil {
		return "", err
	}
	defer co.Close()
	if addrStr, err := co.Get(fmt.Sprintf(`
AssignProc(%#v,%#v)
`, session, sticky), nil); err != nil {
		return "", nil
	} else {
		procAddr := addrStr.(string)
		return procAddr, nil
	}
}

func (consumer *Consumer) ReleaseProc(procAddr string) error {
	consumer.Lock()
	defer consumer.Unlock()

	consumer.connectMaster()
	co, err := consumer.PoolMaster.PoToPeer().Co()
	if err != nil {
		return err
	}
	defer co.Close()
	if _, err := co.Get(fmt.Sprintf(`
ReleaseProc(%#v)
`, procAddr), nil); err != nil {
		return err
	}

	return nil
}
