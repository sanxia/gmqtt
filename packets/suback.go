package packets

import (
	"bytes"
	"fmt"
	"io"
)

//SubackPacket is an internal representation of the fields of the
//Suback MQTT packet
type SubackPacket struct {
	FixedHeader
	MessageId   uint16
	ReturnCodes []byte
}

func (sa *SubackPacket) String() string {
	str := fmt.Sprintf("%s", sa.FixedHeader)
	str += " "
	str += fmt.Sprintf("MessageId: %d", sa.MessageId)
	return str
}

func (sa *SubackPacket) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(encodeUint16(sa.MessageId))
	body.Write(sa.ReturnCodes)
	sa.FixedHeader.RemainingLength = body.Len()
	packet := sa.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (sa *SubackPacket) Unpack(b io.Reader) error {
	var qosBuffer bytes.Buffer
	sa.MessageId = decodeUint16(b)
	qosBuffer.ReadFrom(b)
	sa.ReturnCodes = qosBuffer.Bytes()

	return nil
}

//Details returns a Details struct containing the Qos and
//MessageId of this ControlPacket
func (sa *SubackPacket) Details() Details {
	return Details{Qos: 0, MessageId: sa.MessageId}
}
