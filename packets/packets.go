package packets

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

type Details struct {
	Qos       byte
	MessageId uint16
}

type ControlPacket interface {
	Write(io.Writer) error
	Unpack(io.Reader) error
	String() string
	Details() Details
}

func NewControlPacket(packetType byte) (cp ControlPacket) {
	switch packetType {
	case Connect:
		cp = &ConnectPacket{FixedHeader: FixedHeader{MessageType: Connect}}
	case Connack:
		cp = &ConnackPacket{FixedHeader: FixedHeader{MessageType: Connack}}
	case Disconnect:
		cp = &DisconnectPacket{FixedHeader: FixedHeader{MessageType: Disconnect}}
	case Publish:
		cp = &PublishPacket{FixedHeader: FixedHeader{MessageType: Publish}}
	case Puback:
		cp = &PubackPacket{FixedHeader: FixedHeader{MessageType: Puback}}
	case Pubrec:
		cp = &PubrecPacket{FixedHeader: FixedHeader{MessageType: Pubrec}}
	case Pubrel:
		cp = &PubrelPacket{FixedHeader: FixedHeader{MessageType: Pubrel, Qos: 1}}
	case Pubcomp:
		cp = &PubcompPacket{FixedHeader: FixedHeader{MessageType: Pubcomp}}
	case Subscribe:
		cp = &SubscribePacket{FixedHeader: FixedHeader{MessageType: Subscribe, Qos: 1}}
	case Suback:
		cp = &SubackPacket{FixedHeader: FixedHeader{MessageType: Suback}}
	case Unsubscribe:
		cp = &UnsubscribePacket{FixedHeader: FixedHeader{MessageType: Unsubscribe, Qos: 1}}
	case Unsuback:
		cp = &UnsubackPacket{FixedHeader: FixedHeader{MessageType: Unsuback}}
	case Pingreq:
		cp = &PingreqPacket{FixedHeader: FixedHeader{MessageType: Pingreq}}
	case Pingresp:
		cp = &PingrespPacket{FixedHeader: FixedHeader{MessageType: Pingresp}}
	default:
		return nil
	}
	return cp
}

func NewControlPacketWithHeader(fh FixedHeader) (cp ControlPacket) {
	switch fh.MessageType {
	case Connect:
		cp = &ConnectPacket{FixedHeader: fh}
	case Connack:
		cp = &ConnackPacket{FixedHeader: fh}
	case Disconnect:
		cp = &DisconnectPacket{FixedHeader: fh}
	case Publish:
		cp = &PublishPacket{FixedHeader: fh}
	case Puback:
		cp = &PubackPacket{FixedHeader: fh}
	case Pubrec:
		cp = &PubrecPacket{FixedHeader: fh}
	case Pubrel:
		cp = &PubrelPacket{FixedHeader: fh}
	case Pubcomp:
		cp = &PubcompPacket{FixedHeader: fh}
	case Subscribe:
		cp = &SubscribePacket{FixedHeader: fh}
	case Suback:
		cp = &SubackPacket{FixedHeader: fh}
	case Unsubscribe:
		cp = &UnsubscribePacket{FixedHeader: fh}
	case Unsuback:
		cp = &UnsubackPacket{FixedHeader: fh}
	case Pingreq:
		cp = &PingreqPacket{FixedHeader: fh}
	case Pingresp:
		cp = &PingrespPacket{FixedHeader: fh}
	default:
		return nil
	}
	return cp
}

func ReadPacket(r io.Reader) (cp ControlPacket, err error) {
	var fh FixedHeader
	b := make([]byte, 1)

	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}
	fh.unpack(b[0], r)
	cp = NewControlPacketWithHeader(fh)
	if cp == nil {
		return nil, errors.New("Bad data from client")
	}
	packetBytes := make([]byte, fh.RemainingLength)
	n, err := io.ReadFull(r, packetBytes)
	if err != nil {
		return nil, err
	}
	if n != fh.RemainingLength {
		return nil, errors.New("Failed to read expected data")
	}

	err = cp.Unpack(bytes.NewBuffer(packetBytes))
	return cp, err
}

func boolToByte(b bool) byte {
	switch b {
	case true:
		return 1
	default:
		return 0
	}
}

func decodeByte(b io.Reader) byte {
	num := make([]byte, 1)
	b.Read(num)
	return num[0]
}

func decodeUint16(b io.Reader) uint16 {
	num := make([]byte, 2)
	b.Read(num)
	return binary.BigEndian.Uint16(num)
}

func encodeUint16(num uint16) []byte {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, num)
	return bytes
}

func encodeString(field string) []byte {

	return encodeBytes([]byte(field))
}

func decodeString(b io.Reader) string {
	return string(decodeBytes(b))
}

func decodeBytes(b io.Reader) []byte {
	fieldLength := decodeUint16(b)
	field := make([]byte, fieldLength)
	b.Read(field)
	return field
}

func encodeBytes(field []byte) []byte {
	fieldLength := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldLength, uint16(len(field)))
	return append(fieldLength, field...)
}

func encodeLength(length int) []byte {
	var encLength []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		encLength = append(encLength, digit)
		if length == 0 {
			break
		}
	}
	return encLength
}

func decodeLength(r io.Reader) int {
	var rLength uint32
	var multiplier uint32
	b := make([]byte, 1)
	for multiplier < 27 { //fix: Infinite '(digit & 128) == 1' will cause the dead loop
		io.ReadFull(r, b)
		digit := b[0]
		rLength |= uint32(digit&127) << multiplier
		if (digit & 128) == 0 {
			break
		}
		multiplier += 7
	}
	return int(rLength)
}
