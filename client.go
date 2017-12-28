package gmqtt

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"github.com/sanxia/gmqtt/packets"
	"github.com/sanxia/gmqtt/stores"
	"github.com/sanxia/gmqtt/utils"
)

const (
	disconnected uint32 = iota
	connecting
	reconnecting
	connected
)

type Client interface {
	IsConnected() bool
	Connect() Token
	Disconnect(quiesce uint)
	Publish(topic string, qos byte, retained bool, payload interface{}) Token
	Subscribe(topic string, qos byte, callback MessageHandler) Token
	SubscribeMultiple(filters map[string]byte, callback MessageHandler) Token
	Unsubscribe(topics ...string) Token
	AddRoute(topic string, callback MessageHandler)
}

// client implements the Client interface
type client struct {
	lastSent        int64
	lastReceived    int64
	pingOutstanding int32
	status          uint32
	sync.RWMutex
	messageIds
	conn            net.Conn
	ibound          chan packets.ControlPacket
	obound          chan *PacketAndToken
	oboundP         chan *PacketAndToken
	msgRouter       *router
	stopRouter      chan bool
	incomingPubChan chan *packets.PublishPacket
	errors          chan error
	stop            chan struct{}
	persist         stores.Store
	options         ClientOptions
	workers         sync.WaitGroup
}

func NewClient(o *ClientOptions) Client {
	c := &client{}
	c.options = *o

	if c.options.Store == nil {
		c.options.Store = stores.NewMemoryStore()
	}
	switch c.options.ProtocolVersion {
	case 3, 4:
		c.options.protocolVersionExplicit = true
	default:
		c.options.ProtocolVersion = 4
		c.options.protocolVersionExplicit = false
	}
	c.persist = c.options.Store
	c.status = disconnected
	c.messageIds = messageIds{index: make(map[uint16]tokenCompletor)}
	c.msgRouter, c.stopRouter = newRouter()
	c.msgRouter.setDefaultHandler(c.options.DefaultPublishHandler)
	if !c.options.AutoReconnect {
		c.options.MessageChannelDepth = 0
	}
	return c
}

func (c *client) AddRoute(topic string, callback MessageHandler) {
	if callback != nil {
		c.msgRouter.addRoute(topic, callback)
	}
}

// IsConnected returns a bool signifying whether
// the client is connected or not.
func (c *client) IsConnected() bool {
	c.RLock()
	defer c.RUnlock()
	status := atomic.LoadUint32(&c.status)
	switch {
	case status == connected:
		return true
	case c.options.AutoReconnect && status > disconnected:
		return true
	default:
		return false
	}
}

func (c *client) connectionStatus() uint32 {
	c.RLock()
	defer c.RUnlock()
	status := atomic.LoadUint32(&c.status)
	return status
}

func (c *client) setConnected(status uint32) {
	c.Lock()
	defer c.Unlock()
	atomic.StoreUint32(&c.status, uint32(status))
}

//ErrNotConnected is the error returned from function calls that are
//made when the client is not connected to a broker
var ErrNotConnected = errors.New("Not Connected")

// Connect will create a connection to the message broker
// If clean session is false, then a slice will
// be returned containing Receipts for all messages
// that were in-flight at the last disconnect.
// If clean session is true, then any existing client
// state will be removed.
func (c *client) Connect() Token {
	var err error
	t := newToken(packets.Connect).(*ConnectToken)
	utils.DEBUG.Println(utils.CLI, "Connect()")

	c.obound = make(chan *PacketAndToken, c.options.MessageChannelDepth)
	c.oboundP = make(chan *PacketAndToken, c.options.MessageChannelDepth)
	c.ibound = make(chan packets.ControlPacket)

	go func() {
		c.persist.Open()

		c.setConnected(connecting)
		var rc byte
		cm := newConnectMsgFromOptions(&c.options)
		protocolVersion := c.options.ProtocolVersion

		for _, broker := range c.options.Servers {
			c.options.ProtocolVersion = protocolVersion
		CONN:
			utils.DEBUG.Println(utils.CLI, "about to write new connect msg")
			c.conn, err = openConnection(broker, &c.options.TLSConfig, c.options.ConnectTimeout)
			if err == nil {
				utils.DEBUG.Println(utils.CLI, "socket connected to broker")
				switch c.options.ProtocolVersion {
				case 3:
					utils.DEBUG.Println(utils.CLI, "Using MQTT 3.1 protocol")
					cm.ProtocolName = "MQIsdp"
					cm.ProtocolVersion = 3
				default:
					utils.DEBUG.Println(utils.CLI, "Using MQTT 3.1.1 protocol")
					c.options.ProtocolVersion = 4
					cm.ProtocolName = "MQTT"
					cm.ProtocolVersion = 4
				}
				cm.Write(c.conn)

				rc = c.connect()
				if rc != packets.Accepted {
					if c.conn != nil {
						c.conn.Close()
						c.conn = nil
					}
					//if the protocol version was explicitly set don't do any fallback
					if c.options.protocolVersionExplicit {
						utils.ERROR.Println(utils.CLI, "Connecting to", broker, "CONNACK was not CONN_ACCEPTED, but rather", packets.ConnackReturnCodes[rc])
						continue
					}
					if c.options.ProtocolVersion == 4 {
						utils.DEBUG.Println(utils.CLI, "Trying reconnect using MQTT 3.1 protocol")
						c.options.ProtocolVersion = 3
						goto CONN
					}
				}
				break
			} else {
				utils.ERROR.Println(utils.CLI, err.Error())
				utils.WARN.Println(utils.CLI, "failed to connect to broker, trying next")
				rc = packets.ErrNetworkError
			}
		}

		if c.conn == nil {
			utils.ERROR.Println(utils.CLI, "Failed to connect to a broker")
			t.returnCode = rc
			if rc != packets.ErrNetworkError {
				t.err = packets.ConnErrors[rc]
			} else {
				t.err = fmt.Errorf("%s : %s", packets.ConnErrors[rc], err)
			}
			c.setConnected(disconnected)
			c.persist.Close()
			t.flowComplete()
			return
		}

		c.options.protocolVersionExplicit = true

		c.errors = make(chan error, 1)
		c.stop = make(chan struct{})

		if c.options.KeepAlive != 0 {
			atomic.StoreInt32(&c.pingOutstanding, 0)
			atomic.StoreInt64(&c.lastReceived, time.Now().Unix())
			atomic.StoreInt64(&c.lastSent, time.Now().Unix())
			c.workers.Add(1)
			go keepalive(c)
		}

		c.incomingPubChan = make(chan *packets.PublishPacket, c.options.MessageChannelDepth)
		c.msgRouter.matchAndDispatch(c.incomingPubChan, c.options.Order, c)

		c.setConnected(connected)
		utils.DEBUG.Println(utils.CLI, "client is connected")
		if c.options.OnConnect != nil {
			go c.options.OnConnect(c)
		}

		// Take care of any messages in the store
		//var leftovers []Receipt
		if c.options.CleanSession == false {
			//leftovers = c.resume()
		} else {
			c.persist.Reset()
		}

		go errorWatch(c)

		// Do not start incoming until resume has completed
		c.workers.Add(3)
		go alllogic(c)
		go outgoing(c)
		go incoming(c)

		utils.DEBUG.Println(utils.CLI, "exit startClient")
		t.flowComplete()
	}()
	return t
}

// internal function used to reconnect the client when it loses its connection
func (c *client) reconnect() {
	utils.DEBUG.Println(utils.CLI, "enter reconnect")
	var (
		err error

		rc    = byte(1)
		sleep = time.Duration(1 * time.Second)
	)

	for rc != 0 && c.status != disconnected {
		cm := newConnectMsgFromOptions(&c.options)

		for _, broker := range c.options.Servers {
			utils.DEBUG.Println(utils.CLI, "about to write new connect msg")
			c.conn, err = openConnection(broker, &c.options.TLSConfig, c.options.ConnectTimeout)
			if err == nil {
				utils.DEBUG.Println(utils.CLI, "socket connected to broker")
				switch c.options.ProtocolVersion {
				case 3:
					utils.DEBUG.Println(utils.CLI, "Using MQTT 3.1 protocol")
					cm.ProtocolName = "MQIsdp"
					cm.ProtocolVersion = 3
				default:
					utils.DEBUG.Println(utils.CLI, "Using MQTT 3.1.1 protocol")
					cm.ProtocolName = "MQTT"
					cm.ProtocolVersion = 4
				}
				cm.Write(c.conn)

				rc = c.connect()
				if rc != packets.Accepted {
					c.conn.Close()
					c.conn = nil
					//if the protocol version was explicitly set don't do any fallback
					if c.options.protocolVersionExplicit {
						utils.ERROR.Println(utils.CLI, "Connecting to", broker, "CONNACK was not Accepted, but rather", packets.ConnackReturnCodes[rc])
						continue
					}
				}
				break
			} else {
				utils.ERROR.Println(utils.CLI, err.Error())
				utils.WARN.Println(utils.CLI, "failed to connect to broker, trying next")
				rc = packets.ErrNetworkError
			}
		}
		if rc != 0 {
			utils.DEBUG.Println(utils.CLI, "Reconnect failed, sleeping for", int(sleep.Seconds()), "seconds")
			time.Sleep(sleep)
			if sleep < c.options.MaxReconnectInterval {
				sleep *= 2
			}

			if sleep > c.options.MaxReconnectInterval {
				sleep = c.options.MaxReconnectInterval
			}
		}
	}
	// Disconnect() must have been called while we were trying to reconnect.
	if c.connectionStatus() == disconnected {
		utils.DEBUG.Println(utils.CLI, "Client moved to disconnected state while reconnecting, abandoning reconnect")
		return
	}

	if c.options.KeepAlive != 0 {
		atomic.StoreInt32(&c.pingOutstanding, 0)
		atomic.StoreInt64(&c.lastReceived, time.Now().Unix())
		atomic.StoreInt64(&c.lastSent, time.Now().Unix())
		c.workers.Add(1)
		go keepalive(c)
	}

	c.stop = make(chan struct{})

	c.setConnected(connected)
	utils.DEBUG.Println(utils.CLI, "client is reconnected")
	if c.options.OnConnect != nil {
		go c.options.OnConnect(c)
	}

	go errorWatch(c)

	c.workers.Add(3)
	go alllogic(c)
	go outgoing(c)
	go incoming(c)
}

// This function is only used for receiving a connack
// when the connection is first started.
// This prevents receiving incoming data while resume
// is in progress if clean session is false.
func (c *client) connect() byte {
	utils.DEBUG.Println(utils.NET, "connect started")

	ca, err := packets.ReadPacket(c.conn)
	if err != nil {
		utils.ERROR.Println(utils.NET, "connect got error", err)
		return packets.ErrNetworkError
	}
	if ca == nil {
		utils.ERROR.Println(utils.NET, "received nil packet")
		return packets.ErrNetworkError
	}

	msg, ok := ca.(*packets.ConnackPacket)
	if !ok {
		utils.ERROR.Println(utils.NET, "received msg that was not CONNACK")
		return packets.ErrNetworkError
	}

	utils.DEBUG.Println(utils.NET, "received connack")
	return msg.ReturnCode
}

// Disconnect will end the connection with the server, but not before waiting
// the specified number of milliseconds to wait for existing work to be
// completed.
func (c *client) Disconnect(quiesce uint) {
	status := atomic.LoadUint32(&c.status)
	if status == connected {
		utils.DEBUG.Println(utils.CLI, "disconnecting")
		c.setConnected(disconnected)

		dm := packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
		dt := newToken(packets.Disconnect)
		c.oboundP <- &PacketAndToken{p: dm, t: dt}

		// wait for work to finish, or quiesce time consumed
		dt.WaitTimeout(time.Duration(quiesce) * time.Millisecond)
	} else {
		utils.WARN.Println(utils.CLI, "Disconnect() called but not connected (disconnected/reconnecting)")
		c.setConnected(disconnected)
	}

	c.disconnect()
}

// ForceDisconnect will end the connection with the mqtt broker immediately.
func (c *client) forceDisconnect() {
	if !c.IsConnected() {
		utils.WARN.Println(utils.CLI, "already disconnected")
		return
	}
	c.setConnected(disconnected)
	c.conn.Close()
	utils.DEBUG.Println(utils.CLI, "forcefully disconnecting")
	c.disconnect()
}

func (c *client) internalConnLost(err error) {
	// Only do anything if this was called and we are still "connected"
	// forceDisconnect can cause incoming/outgoing/alllogic to end with
	// error from closing the socket but state will be "disconnected"
	if c.IsConnected() {
		c.closeStop()
		c.conn.Close()
		c.workers.Wait()
		if c.options.CleanSession {
			c.messageIds.cleanUp()
		}
		if c.options.AutoReconnect {
			c.setConnected(reconnecting)
			go c.reconnect()
		} else {
			c.setConnected(disconnected)
		}
		if c.options.OnConnectionLost != nil {
			go c.options.OnConnectionLost(c, err)
		}
	}
}

func (c *client) closeStop() {
	c.Lock()
	defer c.Unlock()
	select {
	case <-c.stop:
		utils.DEBUG.Println("In disconnect and stop channel is already closed")
	default:
		close(c.stop)
	}
}

func (c *client) closeConn() {
	c.Lock()
	defer c.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *client) disconnect() {
	c.closeStop()
	c.closeConn()
	c.workers.Wait()
	close(c.stopRouter)
	utils.DEBUG.Println(utils.CLI, "disconnected")
	c.persist.Close()
}

// Publish will publish a message with the specified QoS and content
// to the specified topic.
// Returns a token to track delivery of the message to the broker
func (c *client) Publish(topic string, qos byte, retained bool, payload interface{}) Token {
	token := newToken(packets.Publish).(*PublishToken)
	utils.DEBUG.Println(utils.CLI, "enter Publish")
	switch {
	case !c.IsConnected():
		token.err = ErrNotConnected
		token.flowComplete()
		return token
	case c.connectionStatus() == reconnecting && qos == 0:
		token.flowComplete()
		return token
	}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = qos
	pub.TopicName = topic
	pub.Retain = retained
	switch payload.(type) {
	case string:
		pub.Payload = []byte(payload.(string))
	case []byte:
		pub.Payload = payload.([]byte)
	default:
		token.err = errors.New("Unknown payload type")
		token.flowComplete()
		return token
	}

	utils.DEBUG.Println(utils.CLI, "sending publish message, topic:", topic)
	if pub.Qos != 0 && pub.MessageID == 0 {
		pub.MessageID = c.getID(token)
		token.messageID = pub.MessageID
	}
	stores.PersistOutbound(c.persist, pub)
	c.obound <- &PacketAndToken{p: pub, t: token}
	return token
}

// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
func (c *client) Subscribe(topic string, qos byte, callback MessageHandler) Token {
	token := newToken(packets.Subscribe).(*SubscribeToken)
	utils.DEBUG.Println(utils.CLI, "enter Subscribe")
	if !c.IsConnected() {
		token.err = ErrNotConnected
		token.flowComplete()
		return token
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	if err := validateTopicAndQos(topic, qos); err != nil {
		token.err = err
		return token
	}
	sub.Topics = append(sub.Topics, topic)
	sub.Qoss = append(sub.Qoss, qos)
	utils.DEBUG.Println(utils.CLI, sub.String())

	if callback != nil {
		c.msgRouter.addRoute(topic, callback)
	}

	token.subs = append(token.subs, topic)
	c.oboundP <- &PacketAndToken{p: sub, t: token}
	utils.DEBUG.Println(utils.CLI, "exit Subscribe")
	return token
}

// SubscribeMultiple starts a new subscription for multiple topics. Provide a MessageHandler to
// be executed when a message is published on one of the topics provided.
func (c *client) SubscribeMultiple(filters map[string]byte, callback MessageHandler) Token {
	var err error
	token := newToken(packets.Subscribe).(*SubscribeToken)
	utils.DEBUG.Println(utils.CLI, "enter SubscribeMultiple")
	if !c.IsConnected() {
		token.err = ErrNotConnected
		token.flowComplete()
		return token
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	if sub.Topics, sub.Qoss, err = validateSubscribeMap(filters); err != nil {
		token.err = err
		return token
	}

	if callback != nil {
		for topic := range filters {
			c.msgRouter.addRoute(topic, callback)
		}
	}
	token.subs = make([]string, len(sub.Topics))
	copy(token.subs, sub.Topics)
	c.oboundP <- &PacketAndToken{p: sub, t: token}
	utils.DEBUG.Println(utils.CLI, "exit SubscribeMultiple")
	return token
}

// Unsubscribe will end the subscription from each of the topics provided.
// Messages published to those topics from other clients will no longer be
// received.
func (c *client) Unsubscribe(topics ...string) Token {
	token := newToken(packets.Unsubscribe).(*UnsubscribeToken)
	utils.DEBUG.Println(utils.CLI, "enter Unsubscribe")
	if !c.IsConnected() {
		token.err = ErrNotConnected
		token.flowComplete()
		return token
	}
	unsub := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	unsub.Topics = make([]string, len(topics))
	copy(unsub.Topics, topics)

	c.oboundP <- &PacketAndToken{p: unsub, t: token}
	for _, topic := range topics {
		c.msgRouter.deleteRoute(topic)
	}

	utils.DEBUG.Println(utils.CLI, "exit Unsubscribe")
	return token
}

func (c *client) OptionsReader() ClientOptionsReader {
	r := ClientOptionsReader{options: &c.options}
	return r
}

//DefaultConnectionLostHandler is a definition of a function that simply
//reports to the DEBUG log the reason for the client losing a connection.
func DefaultConnectionLostHandler(client Client, reason error) {
	utils.DEBUG.Println("Connection lost:", reason.Error())
}
