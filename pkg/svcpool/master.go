package svcpool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"net"
	"runtime"
	"time"
)

func NewMaster(
	poolSize int, hotBack int, processStartTimeout time.Duration,
) (*Master, error) {
	if poolSize < 1 || hotBack < 0 || hotBack > poolSize {
		return nil, errors.RichError(fmt.Sprintf("Invalid pool size and hot back: %d/%d", hotBack, poolSize))
	}

	var err error
	var master = &Master{
		poolSize:            poolSize,
		hotBack:             hotBack,
		processStartTimeout: processStartTimeout,
		allWorkers:          make(map[*procWorker]struct{}),
		pendingWorkers:      make(map[*procWorker]time.Time),
		idleWorkers:         make(chan *procWorker, poolSize),
		workersByPid:        make(map[int]*procWorker),
		workersBySession:    make(map[string]*procWorker),
	}

	return master, err
}

type Master struct {
	poolSize            int
	hotBack             int
	processStartTimeout time.Duration
	teamAddr            *net.TCPAddr
	allWorkers          map[*procWorker]struct{}
	// map worker to its last process start time, removed after successfully registered
	pendingWorkers   map[*procWorker]time.Time
	idleWorkers      chan *procWorker
	workersByPid     map[int]*procWorker
	workersBySession map[string]*procWorker
}

func (pool *Master) Serve(serviceAddr string) {

	// run pool master with parallelism of 1.
	// if parallelism is to be increased, before applying a bigger number than 1:
	// 	*) access to pool master data structures must be properly synced for thread safety
	//  *) race condition for sticky session assignment needs to be prevented by proper syncing
	runtime.GOMAXPROCS(1)

	// start a goro to face proc workers
	go func() {
		hbi.ServeTCP(func() hbi.HoContext {
			return newMaster4Worker(pool)
		}, "127.0.0.1:0", func(listener *net.TCPListener) {
			glog.Infof("HBI service team addr: %+v\n", listener.Addr())
			pool.teamAddr = listener.Addr().(*net.TCPAddr)

			// start hot back processes
			for i := 0; i < pool.hotBack; i++ {
				_, err := newProcWorker(pool)
				if err != nil {
					// crash the process
					glog.Fatal(errors.RichError(err))
				}
			}
		})
	}()

	// serve consumers in blocking mode
	hbi.ServeTCP(func() hbi.HoContext {
		return newMaster4Consumer(pool)
	}, serviceAddr, func(listener *net.TCPListener) {
		glog.Infof("HBI service pool addr: %+v\n", listener.Addr())
	})

}

func (pool *Master) EnsureHot() error {
	for w := range pool.allWorkers {
		w.checkAlive()
	}

	poolQuota := pool.poolSize - len(pool.allWorkers)
	idleQuota := pool.hotBack - len(pool.pendingWorkers) - len(pool.idleWorkers)
	if idleQuota > poolQuota {
		idleQuota = poolQuota
	}
	if idleQuota <= 0 {
		return nil
	}

	glog.Infof(
		"Starting %d hot backing proc workers given current numbers: (%d+%d)/%d",
		idleQuota, len(pool.idleWorkers), len(pool.pendingWorkers), pool.poolSize,
	)
	for ; idleQuota > 0; idleQuota-- {
		if _, err := newProcWorker(pool); err != nil {
			return errors.RichError(err)
		}
	}

	return nil
}
