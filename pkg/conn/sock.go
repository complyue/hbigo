package conn

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type tcpHBIC struct {
	context              *hbiContext
	addr                 string
	conn                 *net.TCPConn
	muSend, muCo, muRecv sync.Mutex
	inCo                 bool
	objCh                chan interface{}
}

/*

 */
func ListenTCP(ctxFact ContextFactory, addr string) (listener *net.TCPListener, err error) {
	var raddr *net.TCPAddr
	raddr, err = net.ResolveTCPAddr("tcp", addr)
	if nil != err {
		log.Fatal("addr error", err)
		return
	}
	listener, err = net.ListenTCP("tcp", raddr)
	for {
		var conn *net.TCPConn
		conn, err = listener.AcceptTCP()
		if nil != err {
			log.Fatal("accept error", err)
			return
		}
		// todo DoS react
		hbic := &tcpHBIC{
			context: ctxFact().(*hbiContext),
			addr:    conn.LocalAddr().String(),
			conn:    conn,
			objCh:   make(chan interface{}),
		}
		hbic.context.peer = hbic
		go hbic.landingLoop()
	}
}

/*

this func named MakeTCP instead of ConnectTCP, as an HBIC can be disconnected & (re)connected again and again.
*/
func MakeTCP(ctx Context, addr string) (hbic *tcpHBIC, err error) {
	hbic = &tcpHBIC{
		context: ctx.(*hbiContext),
		addr:    addr,
		objCh:   make(chan interface{}),
	}
	hbic.context.peer = hbic
	err = hbic.Connect()
	if err != nil {
		return
	}
	go hbic.landingLoop()
	return
}

func (hbic *tcpHBIC) Connect() (err error) {
	raddr, err := net.ResolveTCPAddr("tcp", hbic.addr)
	if nil != err {
		log.Fatal("addr error", err)
		return
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if nil != err {
		log.Fatal("conn error", err)
		return
	}
	hbic.conn = conn
	return
}

func (hbic *tcpHBIC) Disconnect() (err error) {
	err = hbic.conn.Close()
	return
}

func (hbic *tcpHBIC) Fire(code string) {
	go hbic.Notif(code)
}

func (hbic *tcpHBIC) FireCoRun(code string, data <-chan []byte) {
	go hbic.NotifCoRun(code, data)
}

func (hbic *tcpHBIC) Notif(code string) (err error) {
	hbic.muSend.Lock()
	defer hbic.muSend.Unlock()

	_, err = hbic.sendPacket(code, "")
	if err != nil {
		return
	}

	return
}

func (hbic *tcpHBIC) NotifCoRun(code string, data <-chan []byte) (err error) {
	hbic.muSend.Lock()
	defer hbic.muSend.Unlock()

	_, err = hbic.sendPacket(code, "corun")
	if err != nil {
		return
	}
	_, err = hbic.sendData(data)
	if err != nil {
		return
	}

	return
}

func (hbic *tcpHBIC) Co() Connection {
	hbic.muSend.Lock()
	hbic.muCo.Lock()
	if hbic.inCo {
		panic(UsageError{"Already in corun mode ?!"})
	}
	hbic.inCo = true
	return hbic
}

func (hbic *tcpHBIC) coDone() {
	if !hbic.inCo {
		panic(UsageError{"Not in corun mode ?!"})
	}
	hbic.inCo = false
	hbic.muCo.Unlock()
	hbic.muSend.Unlock()
}

func (hbic *tcpHBIC) CoSendCoRun(code string, data <-chan []byte) (err error) {
	if !hbic.inCo {
		panic(UsageError{"Not in corun mode ?!"})
	}

	_, err = hbic.sendPacket(code, "corun")
	if err != nil {
		return
	}
	_, err = hbic.sendData(data)
	if err != nil {
		return
	}

	return
}

func (hbic *tcpHBIC) CoSendCode(code string) (err error) {
	if !hbic.inCo {
		panic(UsageError{"Not in corun mode ?!"})
	}

	_, err = hbic.sendPacket(code, "")
	if err != nil {
		return
	}

	return
}

func (hbic *tcpHBIC) CoSendData(data <-chan []byte) (err error) {
	if !hbic.inCo {
		panic(UsageError{"Not in corun mode ?!"})
	}

	_, err = hbic.sendData(data)
	if err != nil {
		return
	}

	return
}

func (hbic *tcpHBIC) CoGet(code string) (result interface{}, err error) {
	if !hbic.inCo {
		panic(UsageError{"Not in corun mode ?!"})
	}

	_, err = hbic.sendPacket(code, "coget")
	if err != nil {
		return
	}

	result, err = hbic.CoRecvObj()

	return
}

func (hbic *tcpHBIC) CoRecvObj() (result interface{}, err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoRecvData(data <-chan []byte) (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) landingLoop() {
	hbic.context.peer = hbic

	// packet channel, todo consider buffering ?
	pch := make(chan Packet)
	// a packet contains just two strings,
	// it's more optimal to pass value over channel

	// packet receiving goroutine
	go func() {
		for {
			if hbic.context.Cancelled() {
				// connection context cancelled
				return
			}
			// blocking read next packet
			pkt, err := hbic.recvPacket()
			if err != nil {
				hbic.context.Cancel(err)
				return
			}
			if pkt == nil {
				// no more packet to land, todo further handing ?
				return
			}
			// blocking put packet for landing
			pch <- *pkt
			pkt = nil // eager release mem
		}
	}()

	// packet landing loop
	for {
		select {
		case <-hbic.context.Done():
			break
		case pkt := <-pch:
			// TODO land pkt
			switch pkt.WireDir {
			case "":
			case "co_begin":
			case "co_end":
			case "co_ack":
			case "corun":
			case "coget":
			default:
				panic("?!")
			}
		}
	}

}

/*
CoRun() can be simply considered an example to run a local HBI conversation, following 2 forms are equivalent:

Closure Style:
```
err = hbic.CoRun(func() err error {
	err = ...

	return
}())
```

Defer Style:
```
func() {
	defer hbi.CoDone(hbic.Co())

	err = ...
}()
```

The defer style is more preferable when your method or func is perfectly mapped to an HBI conversation.

*/
func (hbic *tcpHBIC) CoRun(fn func() error) (err error) {
	defer CoDone(hbic.Co())
	err = fn()
	return
}

func (hbic *tcpHBIC) sendPacket(payload, wireDir string) (n int64, err error) {
	header := fmt.Sprintf("[%v#%s]", len(payload), wireDir)
	bufs := net.Buffers{
		[]byte(header), []byte(payload),
	}
	n, err = bufs.WriteTo(hbic.conn)
	return
}

func (hbic *tcpHBIC) recvPacket() (packet *Packet, err error) {
	const MaxHeaderLen = 60
	var (
		wireDir, payload string
		n, start, newLen int
		hdrBuf           = make([]byte, 0, MaxHeaderLen)
		payloadBuf       []byte
	)

	// read header
	for {
		start = len(hdrBuf)
		n, err = hbic.conn.Read(hdrBuf[start:cap(hdrBuf)])
		if err != nil {
			return
		}
		newLen = start + n
		hdrBuf = hdrBuf[:newLen]
		for i, c := range hdrBuf[start:newLen] {
			if ']' == c {
				header := string(hdrBuf[0 : start+i+1])
				if '[' != header[0] {
					err = &WireError{fmt.Sprintf("Invalid header: %s", header)}
					return
				}
				lenEnd := strings.Index(header, "#")
				if -1 == lenEnd {
					err = &WireError{"No # in header!"}
					return
				}
				wireDir = string(header[lenEnd+1 : start+i])
				var payloadLen int
				fmt.Sscan(header[1:lenEnd], &payloadLen)
				if payloadLen < 0 {
					err = &WireError{"Negative payload length!"}
					return
				}
				chunkLen := newLen - i - 1
				payloadBuf = make([]byte, chunkLen, payloadLen)
				if chunkLen > 0 {
					copy(payloadBuf[:chunkLen], hdrBuf[i+1:newLen])
				}
				break
			}
		}
		if payloadBuf != nil {
			break
		}
		if newLen >= MaxHeaderLen {
			err = &WireError{fmt.Sprintf("No header within first %v bytes!", MaxHeaderLen)}
			return
		}
	}

	// read payload
	for len(payloadBuf) < cap(payloadBuf) {
		start = len(payloadBuf)
		n, err = hbic.conn.Read(payloadBuf[start:cap(payloadBuf)])
		newLen = start + n
		payloadBuf = payloadBuf[:newLen]
		if newLen >= cap(payloadBuf) {
			break
		}
	}
	payload = string(payloadBuf)

	packet = &Packet{wireDir, payload}
	return
}

// each []byte will have its len() of data sent, regardless of it cap()
func (hbic *tcpHBIC) sendData(data <-chan []byte) (n int64, err error) {
	var bufs net.Buffers
	var nb int64
	for {
		select {
		case <-hbic.context.Done():
			// context cancelled
			return
		case buf, ok := <-data:
			if !ok {
				// no more buf to send
				break
			}
			if len(buf) <= 0 {
				// zero buf, ignore it
				break
			}
			bufs = append(bufs, buf)
		}
		if len(bufs) <= 0 {
			// all data sent
			return
		}
		nb, err = bufs.WriteTo(hbic.conn)
		if err != nil {
			return
		}
		n += nb
	}
	return
}

// each []byte will be filled up to its full cap
func (hbic *tcpHBIC) recvData(data <-chan []byte) (n int64, err error) {
	var nb int
	for {
		select {
		case <-hbic.context.Done():
			// context cancelled
			return
		case buf, ok := <-data:
			if !ok {
				// no more buf to send
				break
			}
			if len(buf) <= 0 {
				// zero buf, ignore it
				break
			}
			for {
				nb, err = hbic.conn.Read(buf[:cap(buf)])
				if err != nil {
					return
				}
				if nb >= cap(buf) {
					// this buf fully filled
					break
				}
				n += int64(nb)
				// read into rest space
				buf = buf[nb:cap(buf)]
			}
		}
	}
	return
}
