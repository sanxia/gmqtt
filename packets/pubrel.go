package packets

import (
	"fmt"
	"io"
)

//PubrelPacket is an internal representation of the fields of the
//Pubrel MQTT packet
type PubrelPacket struct {
	FixedHeader
	MessageId uint16
}

func (pr *PubrelPacket) String() string {
	str := fmt.Sprintf("%s", pr.FixedHeader)
	str += " "
	str += fmt.Sprintf("MessageId: %d", pr.MessageId)
	return str
}

func (pr *PubrelPacket) Write(w io.Writer) error {
	var err error
	pr.FixedHeader.RemainingLength = 2
	packet := pr.FixedHeader.pack()
	packet.Write(encodeUint16(pr.MessageId))
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (pr *PubrelPacket) Unpack(b io.Reader) error {
	pr.MessageId = decodeUint16(b)

	return nil
}

//Details returns a Details struct containing the Qos and
//MessageId of this ControlPacket
func (pr *PubrelPacket) Details() Details {
	return Details{Qos: pr.Qos, MessageId: pr.MessageId}
}
