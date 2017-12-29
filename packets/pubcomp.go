package packets

import (
	"fmt"
	"io"
)

//PubcompPacket is an internal representation of the fields of the
//Pubcomp MQTT packet
type PubcompPacket struct {
	FixedHeader
	MessageId uint16
}

func (pc *PubcompPacket) String() string {
	str := fmt.Sprintf("%s", pc.FixedHeader)
	str += " "
	str += fmt.Sprintf("MessageId: %d", pc.MessageId)
	return str
}

func (pc *PubcompPacket) Write(w io.Writer) error {
	var err error
	pc.FixedHeader.RemainingLength = 2
	packet := pc.FixedHeader.pack()
	packet.Write(encodeUint16(pc.MessageId))
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (pc *PubcompPacket) Unpack(b io.Reader) error {
	pc.MessageId = decodeUint16(b)

	return nil
}

//Details returns a Details struct containing the Qos and
//MessageId of this ControlPacket
func (pc *PubcompPacket) Details() Details {
	return Details{Qos: pc.Qos, MessageId: pc.MessageId}
}
