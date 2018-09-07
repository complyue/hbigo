package conn

import "fmt"

type Packet struct {
	WireDir string
	Payload string
}

func (pkt *Packet) String() string {
	return fmt.Sprintf("[#%s]%s", pkt.WireDir, pkt.Payload)
}
