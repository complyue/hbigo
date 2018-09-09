package proto

import "fmt"

type Packet struct {
	WireDir string
	Payload string
}

func (pkt *Packet) String() string {
	return fmt.Sprintf("[%d#%s]%s", len(pkt.Payload), pkt.WireDir, pkt.Payload)
}
