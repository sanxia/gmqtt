package stores

import (
	"fmt"
	"strconv"
)

import (
	"github.com/sanxia/gmqtt/packets"
	"github.com/sanxia/gmqtt/utils"
)

const (
	inboundPrefix  = "i."
	outboundPrefix = "o."
)

// Store is an interface which can be used to provide implementations
// for message persistence.
// Because we may have to store distinct messages with the same
// message ID, we need a unique key for each message. This is
// possible by prepending "i." or "o." to each message id
type Store interface {
	Open()
	Put(key string, message packets.ControlPacket)
	Get(key string) packets.ControlPacket
	All() []string
	Del(key string)
	Close()
	Reset()
}

// A key MUST have the form "X.[messageid]"
// where X is 'i' or 'o'
func mIDFromKey(key string) uint16 {
	s := key[2:]
	i, err := strconv.Atoi(s)
	utils.CheckErr(err)
	return uint16(i)
}

// Return a string of the form "i.[id]"
func inboundKeyFromMID(id uint16) string {
	return fmt.Sprintf("%s%d", inboundPrefix, id)
}

// Return a string of the form "o.[id]"
func outboundKeyFromMID(id uint16) string {
	return fmt.Sprintf("%s%d", outboundPrefix, id)
}

// govern which outgoing messages are persisted
func PersistOutbound(s Store, m packets.ControlPacket) {
	switch m.Details().Qos {
	case 0:
		switch m.(type) {
		case *packets.PubackPacket, *packets.PubcompPacket:
			// Sending puback. delete matching publish
			// from ibound
			s.Del(inboundKeyFromMID(m.Details().MessageId))
		}
	case 1:
		switch m.(type) {
		case *packets.PublishPacket, *packets.PubrelPacket, *packets.SubscribePacket, *packets.UnsubscribePacket:
			// Sending publish. store in obound
			// until puback received
			s.Put(outboundKeyFromMID(m.Details().MessageId), m)
		default:
			utils.ERROR.Println(utils.STR, "Asked to persist an invalid message type")
		}
	case 2:
		switch m.(type) {
		case *packets.PublishPacket:
			// Sending publish. store in obound
			// until pubrel received
			s.Put(outboundKeyFromMID(m.Details().MessageId), m)
		default:
			utils.ERROR.Println(utils.STR, "Asked to persist an invalid message type")
		}
	}
}

// govern which incoming messages are persisted
func PersistInbound(s Store, m packets.ControlPacket) {
	switch m.Details().Qos {
	case 0:
		switch m.(type) {
		case *packets.PubackPacket, *packets.SubackPacket, *packets.UnsubackPacket, *packets.PubcompPacket:
			// Received a puback. delete matching publish
			// from obound
			s.Del(outboundKeyFromMID(m.Details().MessageId))
		case *packets.PublishPacket, *packets.PubrecPacket, *packets.PingrespPacket, *packets.ConnackPacket:
		default:
			utils.ERROR.Println(utils.STR, "Asked to persist an invalid messages type")
		}
	case 1:
		switch m.(type) {
		case *packets.PublishPacket, *packets.PubrelPacket:
			// Received a publish. store it in ibound
			// until puback sent
			s.Put(inboundKeyFromMID(m.Details().MessageId), m)
		default:
			utils.ERROR.Println(utils.STR, "Asked to persist an invalid messages type")
		}
	case 2:
		switch m.(type) {
		case *packets.PublishPacket:
			// Received a publish. store it in ibound
			// until pubrel received
			s.Put(inboundKeyFromMID(m.Details().MessageId), m)
		default:
			utils.ERROR.Println(utils.STR, "Asked to persist an invalid messages type")
		}
	}
}
