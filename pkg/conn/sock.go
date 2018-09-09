package conn

import (
	"fmt"
	. "github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/proto"
	"github.com/complyue/hbigo/pkg/util"
	"github.com/golang/glog"
	"log"
	"net"
	"strings"
)

type TCPConn struct {
	Hosting
	Posting
}

/*

 */
func ServeTCP(ctxFact func() HoContext, addr string) (listener *net.TCPListener, err error) {
	var raddr *net.TCPAddr
	raddr, err = net.ResolveTCPAddr("tcp", addr)
	if nil != err {
		log.Fatal("addr error", err)
		return
	}
	listener, err = net.ListenTCP("tcp", raddr)
	glog.Infof("HBI server listening %s ...\n", listener.Addr())

	for {
		var conn *net.TCPConn
		conn, err = listener.AcceptTCP()
		if nil != err {
			log.Fatal("accept error", err)
			return
		}
		netIdent := fmt.Sprintf("%s<->%s", conn.LocalAddr(), conn.RemoteAddr())
		glog.V(1).Infof("New HBI connection accepted: %s", netIdent)

		// todo DoS react
		ctx := ctxFact()

		ho := NewHostingEndpoint(ctx)
		hoWire := TCPWire{
			CancellableContext: ho,
			conn:               conn,
		}
		ho.PlugWire(netIdent, hoWire.recvPacket, hoWire.recvData)

		po := NewPostingEndpoint()
		poWire := TCPWire{
			CancellableContext: po,
			conn:               conn,
		}
		po.PlugWire(netIdent, poWire.sendPacket, poWire.sendData, ho)

		ho.SetPoToPeer(po)

		ho.StartLandingLoop()
	}
}

type TCPWire struct {
	util.CancellableContext
	conn *net.TCPConn
}

func (wire *TCPWire) sendPacket(payload, wireDir string) (n int64, err error) {
	header := fmt.Sprintf("[%v#%s]", len(payload), wireDir)
	bufs := net.Buffers{
		[]byte(header), []byte(payload),
	}
	n, err = bufs.WriteTo(wire.conn)
	return
}

func (wire *TCPWire) recvPacket() (packet *Packet, err error) {
	const MaxHeaderLen = 60
	var (
		wireDir, payload string
		n, start, newLen int
		hdrBuf           = make([]byte, 0, MaxHeaderLen)
		payloadBuf       []byte
	)

	// read header
	for {
		if wire.Cancelled() {
			return
		}
		start = len(hdrBuf)
		n, err = wire.conn.Read(hdrBuf[start:cap(hdrBuf)])
		if err != nil {
			return
		}
		newLen = start + n
		hdrBuf = hdrBuf[:newLen]
		for i, c := range hdrBuf[start:newLen] {
			if ']' == c {
				header := string(hdrBuf[0 : start+i+1])
				if '[' != header[0] {
					err = WireError{fmt.Sprintf("Invalid header: %s", header)}
					return
				}
				lenEnd := strings.Index(header, "#")
				if -1 == lenEnd {
					err = WireError{"No # in header!"}
					return
				}
				wireDir = string(header[lenEnd+1 : start+i])
				var payloadLen int
				fmt.Sscan(header[1:lenEnd], &payloadLen)
				if payloadLen < 0 {
					err = WireError{"Negative payload length!"}
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
		if wire.Cancelled() {
			return
		}
		start = len(payloadBuf)
		n, err = wire.conn.Read(payloadBuf[start:cap(payloadBuf)])
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
func (wire *TCPWire) sendData(data <-chan []byte) (n int64, err error) {
	var bufs net.Buffers
	var nb int64
	for {
		select {
		case <-wire.Done():
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
		nb, err = bufs.WriteTo(wire.conn)
		if err != nil {
			return
		}
		n += nb
	}
	return
}

// each []byte will be filled up to its full cap
func (wire *TCPWire) recvData(data <-chan []byte) (n int64, err error) {
	var nb int
	for {
		select {
		case <-wire.Done():
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
				nb, err = wire.conn.Read(buf[:cap(buf)])
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
