package packets

import (
	"fmt"
	"io"
)

//PubackPacket is an internal representation of the fields of the
//Puback MQTT packet
type PubackPacket struct {
	FixedHeader
	MessageId uint16
}

func (pa *PubackPacket) String() string {
	str := fmt.Sprintf("%s", pa.FixedHeader)
	str += " "
	str += fmt.Sprintf("MessageId: %d", pa.MessageId)
	return str
}

func (pa *PubackPacket) Write(w io.Writer) error {
	var err error
	pa.FixedHeader.RemainingLength = 2
	packet := pa.FixedHeader.pack()
	packet.Write(encodeUint16(pa.MessageId))
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (pa *PubackPacket) Unpack(b io.Reader) error {
	pa.MessageId = decodeUint16(b)

	return nil
}

//Details returns a Details struct containing the Qos and
//MessageId of this ControlPacket
func (pa *PubackPacket) Details() Details {
	return Details{Qos: pa.Qos, MessageId: pa.MessageId}
}
