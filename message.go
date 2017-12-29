package gmqtt

import (
	"github.com/sanxia/gmqtt/packets"
)

type Message interface {
	Duplicate() bool
	Qos() byte
	Retained() bool
	Topic() string
	MessageId() uint16
	Payload() []byte
}

type message struct {
	duplicate bool
	qos       byte
	retained  bool
	topic     string
	messageId uint16
	payload   []byte
}

func (m *message) Duplicate() bool {
	return m.duplicate
}

func (m *message) Qos() byte {
	return m.qos
}

func (m *message) Retained() bool {
	return m.retained
}

func (m *message) Topic() string {
	return m.topic
}

func (m *message) MessageId() uint16 {
	return m.messageId
}

func (m *message) Payload() []byte {
	return m.payload
}

func messageFromPublish(p *packets.PublishPacket) Message {
	return &message{
		duplicate: p.Dup,
		qos:       p.Qos,
		retained:  p.Retain,
		topic:     p.TopicName,
		messageId: p.MessageId,
		payload:   p.Payload,
	}
}

func newConnectMsgFromOptions(options *ClientOptions) *packets.ConnectPacket {
	m := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)

	m.CleanSession = options.CleanSession
	m.WillFlag = options.WillEnabled
	m.WillRetain = options.WillRetained
	m.ClientIdentifier = options.ClientID

	if options.WillEnabled {
		m.WillQos = options.WillQos
		m.WillTopic = options.WillTopic
		m.WillMessage = options.WillPayload
	}

	if options.Username != "" {
		m.UsernameFlag = true
		m.Username = options.Username
		//mustn't have password without user as well
		if options.Password != "" {
			m.PasswordFlag = true
			m.Password = []byte(options.Password)
		}
	}

	m.Keepalive = uint16(options.KeepAlive)

	return m
}
