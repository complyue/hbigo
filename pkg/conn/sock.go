package conn

import (
	"fmt"
	"log"
	"net"
	"strings"
)

type tcpHBIC struct {
	context *hbiContext
	addr    string
	conn    *net.TCPConn
}

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
		go func(hbic *tcpHBIC) {

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
					pkt, err := hbic.RecvPacket()
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

		}(&tcpHBIC{ctxFact().(*hbiContext), conn.LocalAddr().String(), conn})
	}
}

// Make instead of Connect as it can be disconnected & connected again and again
func MakeTCP(ctx Context, addr string) (hbic *tcpHBIC, err error) {
	hbic = &tcpHBIC{context: ctx.(*hbiContext), addr: addr}
	err = hbic.Connect()
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
	panic("implement me")
}

func (hbic *tcpHBIC) FireCoRun(code string, data chan []byte) {
	panic("implement me")
}

func (hbic *tcpHBIC) Notif(code string) (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) NotifCoRun(code string, data chan []byte) (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoBegin() (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoSendCoRun(code string) (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoGet(code string) (result interface{}, err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoSendCode(code string) (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoSendData(data chan []byte) (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoRecvObj() (result interface{}, err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoRecvData(data chan []byte) (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) CoEnd() (err error) {
	panic("implement me")
}

func (hbic *tcpHBIC) SendPacket(payload, wireDir string) (n int64, err error) {
	header := fmt.Sprintf("[%v#%s]", len(payload), wireDir)
	bufs := net.Buffers{
		[]byte(header), []byte(payload),
	}
	n, err = bufs.WriteTo(hbic.conn)
	return
}

func (hbic *tcpHBIC) RecvPacket() (packet *Packet, err error) {
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
