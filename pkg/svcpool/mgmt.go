package svcpool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func newMaster4Consumer(pool *Master) hbi.HoContext {
	return &master4consumer{
		HoContext: hbi.NewHoContext(),

		pool: pool,
	}
}

type master4consumer struct {
	hbi.HoContext

	pool *Master

	session string
	sticky  bool

	assignedWorker *procWorker
}

// co send back the service proc address assigned
func (m4c *master4consumer) AssignProc(session string, sticky bool) {
	if session == "" && sticky {
		m4c.Ho().Cancel(errors.NewUsageError("Requesting sticky session to empty id ?!"))
		return
	}
	if m4c.sticky && session != m4c.session {
		m4c.Ho().Cancel(errors.Errorf("Changing sticky session [%s]=>[%s] ?!", m4c.session, session))
		return
	}

	m4c.session = session
	m4c.sticky = sticky
	procPort := m4c.pool.assignProc(m4c)

	p2p := m4c.PoToPeer()
	// a conversation should have been initiated by service consumer endpoint
	p2p.CoSendCode(fmt.Sprintf(
		// use the IP via which this consumer has connected to this pool
		`"%s:%d"`, p2p.LocalAddr().(*net.TCPAddr).IP.String(), procPort,
	))
}

// co send back true if the proc is idle after this release, false if its still assigned to some other consumers.
func (m4c *master4consumer) ReleaseProc(procAddr string) {
	// TODO mark the worker as idle after all consumer released it
	m4c.PoToPeer().CoSendCode(`false`)
}

func (pool *Master) assignProc(consumer *master4consumer) (procPort int) {
	var err error
	var ok bool
	var worker *procWorker
	// prepare worker proc anyway before actual return
	defer func() {
		if e := recover(); e != nil {
			err = errors.RichError(e)
		}
		if err != nil {
			glog.Error(err)
		}
		if worker == nil {
			panic(errors.New("No proc assigned ?!"))
		}
		if consumer.session != "" && worker.lastSession != consumer.session {
			if worker.lastSession != "" {
				delete(pool.workersBySession, worker.lastSession)
				glog.Warningf(
					"Consumer %s caused session switch [%s]=>[%s]\n",
					consumer.Ho().NetIdent(), worker.lastSession, consumer.session,
				)
			}
			err = worker.prepareSession(consumer.session)
			if err != nil {
				panic(errors.Wrap(err, "Failed session preparation!"))
			}
			pool.workersBySession[worker.lastSession] = worker
		}
		consumer.assignedWorker = worker
		procPort = worker.ProcPort // set return value
	}()

	poolQuota := pool.poolSize - len(pool.allWorkers)
	idleQuota := pool.hotBack - len(pool.pendingWorkers) - len(pool.idleWorkers)
	if idleQuota > poolQuota {
		idleQuota = poolQuota
	}
	if idleQuota > 0 {
		glog.V(1).Infof(
			"Starting %d hot backing proc workers given current numbers: (%d+%d)/%d",
			idleQuota, len(pool.idleWorkers), len(pool.pendingWorkers), pool.poolSize,
		)
		for ; idleQuota > 0; idleQuota-- {
			_, err = newProcWorker(pool)
			if err != nil {
				panic(errors.RichError(err))
			}
		}
	}

	worker = consumer.assignedWorker
	if worker != nil && worker.checkAlive() {
		// this consumer has had a worker assigned, and still alive, reused it
		return
	}

	if consumer.sticky && consumer.session != "" {
		// if the consumer requests sticky session, and there's already a proc worker on the specified session,
		// must assign this worker whether it is idle or not. concurrency/parallelism is offloaded to the proc impl.
		if worker, ok = pool.workersBySession[consumer.session]; ok {
			if worker.lastSession != consumer.session {
				panic(errors.New("?!"))
			}
			if worker.checkAlive() {
				return
			}
		}
	}

	// search for idle workers for this assignment
	searchedWorkers := make(map[*procWorker]struct{})
	for {
		worker = <-pool.idleWorkers
		if !worker.checkAlive() {
			continue
		}
		if consumer.session == "" {
			// session-less
			if worker.lastSession == "" {
				// ideal no-session match
				return
			}
		} else if worker.lastSession == consumer.session {
			// ideal session match
			return
		}
		if _, ok = searchedWorkers[worker]; ok {
			// reached a known idle worker, meaning we've searched all idle workers without ideal match,
			// use this worker anyway
			return
		}
		// record this idle worker as known, and continue searching for an ideal match
		searchedWorkers[worker] = struct{}{}
		// put it back to ideal cha
		pool.idleWorkers <- worker
	}

	return
}

func (pool *Master) registerWorker(pid int, procPort int, ho hbi.Hosting) (w *procWorker) {
	var ok bool

	w, ok = pool.workersByPid[pid]
	if !ok {
		ho.Cancel(errors.Errorf("Your pid [%d] is not expected!", pid))
		return
	}
	if w.proc.Pid != pid {
		panic(errors.Errorf("?! pid %d vs %d", pid, w.proc.Pid))
	}
	w.ho = ho
	w.ProcPort = procPort
	w.lastSession = ""

	delete(pool.pendingWorkers, w)
	pool.idleWorkers <- w

	// restart a new process after disconnected
	go func() {
		<-ho.Done()
		// don't restart too quickly, or continuous os failures
		// could create extreme system pressure
		time.Sleep(10 * time.Second)
		w.restartProcess(ho.Err())
	}()

	glog.V(1).Infof(
		"Service proc worker process [pid=%d,port=%d] registered [team=%s]",
		w.proc.Pid, procPort, w.pool.teamAddr.String(),
	)
	return
}

func newProcWorker(pool *Master) (worker *procWorker, err error) {
	worker = &procWorker{
		pool: pool,
	}
	err = worker.startProcess()
	if err != nil {
		err = errors.Wrap(err, "Failed start service proce worker process!")
		glog.Error(err)
		return
	}
	pool.allWorkers[worker] = struct{}{}
	return
}

type procWorker struct {
	pool *Master

	proc *os.Process

	lastAct time.Time

	ho hbi.HoContext

	ProcPort int

	lastSession string
}

func (w *procWorker) startProcess() (err error) {
	// todo better to clear workersByPid or not ?
	if w.lastSession != "" {
		delete(w.pool.workersBySession, w.lastSession)
		w.lastSession = ""
	}
	var exePath string
	exePath, err = os.Executable()
	if err != nil {
		return
	}
	cmd := exec.Command(exePath, append([]string{
		fmt.Sprintf("-team=%s", w.pool.teamAddr.String())}, os.Args[1:]...,
	)...)
	cmd.Stdin = nil
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err = cmd.Start()
	if err != nil {
		return
	}
	w.proc = cmd.Process
	glog.V(1).Infof(
		"Started service proc worker process [pid=%d,team=%s].",
		w.proc.Pid, w.pool.teamAddr.String(),
	)
	w.pool.pendingWorkers[w] = time.Now()
	w.pool.workersByPid[w.proc.Pid] = w
	return
}

func (w *procWorker) restartProcess(reason error) {
	workerInfo := ""
	if w.proc != nil {
		workerInfo = fmt.Sprintf("ptr=%p,pid=%v", w, w.proc.Pid)
	} else {
		workerInfo = fmt.Sprintf("ptr=%p", w)
	}
	if reason != nil {
		glog.Errorf("Restarting proc worker [%s] due to error: %+v", workerInfo, reason)
	}
	if startTime, ok := w.pool.pendingWorkers[w]; ok {
		if time.Now().Sub(startTime) < w.pool.processStartTimeout {
			// recently restarted
			glog.Warningf("Repeated restarting of proc worker [%s] ignored.", workerInfo)
			return
		} else {
			// start timed out,
			glog.Warningf("Service proc worker [%s] not registered within %v, attempt restarting ...",
				workerInfo, w.pool.processStartTimeout,
			)
		}
	}
	if w.ho != nil {
		w.ho.Cancel(reason)
		w.ho = nil
	}
	if w.proc != nil {
		_ = w.proc.Kill() // ignore err
		w.proc = nil
	}
	w.startProcess()
}

func (w *procWorker) checkAlive() (alive bool) {
	var err error
	defer func() {
		if !alive {
			w.restartProcess(err)
		}
	}()
	if w.proc == nil || w.ho == nil {
		// not started
		return
	}
	if time.Now().Sub(w.lastAct) < (5 * time.Second) {
		// affirmative recently
		alive = true
		return
	}
	// verify wire is functioning for sending
	err = w.ho.PoToPeer().Notif("ping()")
	if err != nil {
		return
	}
	// check pid exists
	err = w.proc.Signal(syscall.Signal(0))
	if err != nil {
		return
	}
	// aliveness confirmed
	w.lastAct = time.Now()
	alive = true
	return
}

func (w *procWorker) prepareSession(session string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.RichError(e)
		}
		if err != nil {
			w.restartProcess(err)
		}
	}()
	p2p := w.ho.PoToPeer()
	var co hbi.Conver
	co, err = p2p.Co()
	if err != nil {
		return
	}
	defer co.Close()
	var workerSession interface{}
	workerSession, err = co.Get(fmt.Sprintf(`
PrepareSession(%#v)
`, session))
	if err != nil {
		return
	}
	if workerSession != session {
		err = errors.Errorf(
			"Unexpected session preparation result [%s] vs [%s]",
			workerSession, session,
		)
		return
	}
	w.lastSession = workerSession.(string)
	return
}

func (w *procWorker) retired() {
	// start a new process to replace the retired one
	w.restartProcess(nil)
}

func newMaster4Worker(pool *Master) hbi.HoContext {
	return &master4worker{
		HoContext: hbi.NewHoContext(),

		pool: pool,
	}
}

type master4worker struct {
	hbi.HoContext

	pool *Master

	worker *procWorker
}

func (m4w *master4worker) WorkerOnline(pid int, procPort int) {
	m4w.worker = m4w.pool.registerWorker(pid, procPort, m4w.Ho())
}

func (m4w *master4worker) WorkerRetiring() {
	m4w.worker.retired()
}
