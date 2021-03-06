package gmqtt

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"sync/atomic"
	"time"
)

import (
	"github.com/sanxia/gmqtt/packets"
	"github.com/sanxia/gmqtt/stores"
	"github.com/sanxia/gmqtt/utils"
	"golang.org/x/net/proxy"
	"golang.org/x/net/websocket"
)

func signalError(c chan<- error, err error) {
	select {
	case c <- err:
	default:
	}
}

func openConnection(uri *url.URL, tlsc *tls.Config, timeout time.Duration) (net.Conn, error) {
	switch uri.Scheme {
	case "ws":
		conn, err := websocket.Dial(uri.String(), "mqtt", fmt.Sprintf("http://%s", uri.Host))
		if err != nil {
			return nil, err
		}
		conn.PayloadType = websocket.BinaryFrame
		return conn, err
	case "wss":
		config, _ := websocket.NewConfig(uri.String(), fmt.Sprintf("https://%s", uri.Host))
		config.Protocol = []string{"mqtt"}
		config.TlsConfig = tlsc
		conn, err := websocket.DialConfig(config)
		if err != nil {
			return nil, err
		}
		conn.PayloadType = websocket.BinaryFrame
		return conn, err
	case "tcp":
		allProxy := os.Getenv("all_proxy")
		if len(allProxy) == 0 {
			conn, err := net.DialTimeout("tcp", uri.Host, timeout)
			if err != nil {
				return nil, err
			}
			return conn, nil
		}
		proxyDialer := proxy.FromEnvironment()

		conn, err := proxyDialer.Dial("tcp", uri.Host)
		if err != nil {
			return nil, err
		}
		return conn, nil
	case "unix":
		conn, err := net.DialTimeout("unix", uri.Host, timeout)
		if err != nil {
			return nil, err
		}
		return conn, nil
	case "ssl":
		fallthrough
	case "tls":
		fallthrough
	case "tcps":
		allProxy := os.Getenv("all_proxy")
		if len(allProxy) == 0 {
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", uri.Host, tlsc)
			if err != nil {
				return nil, err
			}
			return conn, nil
		}
		proxyDialer := proxy.FromEnvironment()

		conn, err := proxyDialer.Dial("tcp", uri.Host)
		if err != nil {
			return nil, err
		}

		tlsConn := tls.Client(conn, tlsc)

		err = tlsConn.Handshake()
		if err != nil {
			conn.Close()
			return nil, err
		}

		return tlsConn, nil
	}
	return nil, errors.New("Unknown protocol")
}

// actually read incoming messages off the wire
// send Message object into ibound channel
func incoming(c *client) {
	var err error
	var cp packets.ControlPacket

	defer c.workers.Done()

	utils.DEBUG.Println(utils.NET, "incoming started")

	for {
		if cp, err = packets.ReadPacket(c.conn); err != nil {
			break
		}
		utils.DEBUG.Println(utils.NET, "Received Message")
		select {
		case c.ibound <- cp:
			// Notify keepalive logic that we recently received a packet
			if c.options.KeepAlive != 0 {
				atomic.StoreInt64(&c.lastReceived, time.Now().Unix())
			}
		case <-c.stop:
			// This avoids a deadlock should a message arrive while shutting down.
			// In that case the "reader" of c.ibound might already be gone
			utils.WARN.Println(utils.NET, "incoming dropped a received message during shutdown")
			break
		}
	}
	// We received an error on read.
	// If disconnect is in progress, swallow error and return
	select {
	case <-c.stop:
		utils.DEBUG.Println(utils.NET, "incoming stopped")
		return
	// Not trying to disconnect, send the error to the errors channel
	default:
		utils.ERROR.Println(utils.NET, "incoming stopped with error", err)
		signalError(c.errors, err)
		return
	}
}

// receive a Message object on obound, and then
// actually send outgoing message to the wire
func outgoing(c *client) {
	defer c.workers.Done()
	utils.DEBUG.Println(utils.NET, "outgoing started")

	for {
		utils.DEBUG.Println(utils.NET, "outgoing waiting for an outbound message")
		select {
		case <-c.stop:
			utils.DEBUG.Println(utils.NET, "outgoing stopped")
			return
		case pub := <-c.obound:
			msg := pub.p.(*packets.PublishPacket)

			if c.options.WriteTimeout > 0 {
				c.conn.SetWriteDeadline(time.Now().Add(c.options.WriteTimeout))
			}

			if err := msg.Write(c.conn); err != nil {
				utils.ERROR.Println(utils.NET, "outgoing stopped with error", err)
				signalError(c.errors, err)
				return
			}

			if c.options.WriteTimeout > 0 {
				// If we successfully wrote, we don't want the timeout to happen during an idle period
				// so we reset it to infinite.
				c.conn.SetWriteDeadline(time.Time{})
			}

			if msg.Qos == 0 {
				pub.t.flowComplete()
			}
			utils.DEBUG.Println(utils.NET, "obound wrote msg, id:", msg.MessageId)
		case msg := <-c.oboundP:
			switch msg.p.(type) {
			case *packets.SubscribePacket:
				msg.p.(*packets.SubscribePacket).MessageId = c.getID(msg.t)
			case *packets.UnsubscribePacket:
				msg.p.(*packets.UnsubscribePacket).MessageId = c.getID(msg.t)
			}
			utils.DEBUG.Println(utils.NET, "obound priority msg to write, type", reflect.TypeOf(msg.p))
			if err := msg.p.Write(c.conn); err != nil {
				utils.ERROR.Println(utils.NET, "outgoing stopped with error", err)
				signalError(c.errors, err)
				return
			}
			switch msg.p.(type) {
			case *packets.DisconnectPacket:
				msg.t.(*DisconnectToken).flowComplete()
				utils.DEBUG.Println(utils.NET, "outbound wrote disconnect, stopping")
				return
			}
		}
		// Reset ping timer after sending control packet.
		if c.options.KeepAlive != 0 {
			atomic.StoreInt64(&c.lastSent, time.Now().Unix())
		}
	}
}

// receive Message objects on ibound
// store messages if necessary
// send replies on obound
// delete messages from store if necessary
func alllogic(c *client) {
	defer c.workers.Done()
	utils.DEBUG.Println(utils.NET, "logic started")

	for {
		utils.DEBUG.Println(utils.NET, "logic waiting for msg on ibound")

		select {
		case msg := <-c.ibound:
			utils.DEBUG.Println(utils.NET, "logic got msg on ibound")
			stores.PersistInbound(c.persist, msg)
			switch m := msg.(type) {
			case *packets.PingrespPacket:
				utils.DEBUG.Println(utils.NET, "received pingresp")
				atomic.StoreInt32(&c.pingOutstanding, 0)
			case *packets.SubackPacket:
				utils.DEBUG.Println(utils.NET, "received suback, id:", m.MessageId)
				token := c.getToken(m.MessageId)
				switch t := token.(type) {
				case *SubscribeToken:
					utils.DEBUG.Println(utils.NET, "granted qoss", m.ReturnCodes)
					for i, qos := range m.ReturnCodes {
						t.subResult[t.subs[i]] = qos
					}
				}
				token.flowComplete()
				c.freeID(m.MessageId)
			case *packets.UnsubackPacket:
				utils.DEBUG.Println(utils.NET, "received unsuback, id:", m.MessageId)
				c.getToken(m.MessageId).flowComplete()
				c.freeID(m.MessageId)
			case *packets.PublishPacket:
				utils.DEBUG.Println(utils.NET, "received publish, msgId:", m.MessageId)
				utils.DEBUG.Println(utils.NET, "putting msg on onPubChan")
				switch m.Qos {
				case 2:
					c.incomingPubChan <- m
					utils.DEBUG.Println(utils.NET, "done putting msg on incomingPubChan")
					pr := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
					pr.MessageId = m.MessageId
					utils.DEBUG.Println(utils.NET, "putting pubrec msg on obound")
					select {
					case c.oboundP <- &PacketAndToken{p: pr, t: nil}:
					case <-c.stop:
					}
					utils.DEBUG.Println(utils.NET, "done putting pubrec msg on obound")
				case 1:
					c.incomingPubChan <- m
					utils.DEBUG.Println(utils.NET, "done putting msg on incomingPubChan")
					pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
					pa.MessageId = m.MessageId
					utils.DEBUG.Println(utils.NET, "putting puback msg on obound")
					stores.PersistOutbound(c.persist, pa)
					select {
					case c.oboundP <- &PacketAndToken{p: pa, t: nil}:
					case <-c.stop:
					}
					utils.DEBUG.Println(utils.NET, "done putting puback msg on obound")
				case 0:
					select {
					case c.incomingPubChan <- m:
					case <-c.stop:
					}
					utils.DEBUG.Println(utils.NET, "done putting msg on incomingPubChan")
				}
			case *packets.PubackPacket:
				utils.DEBUG.Println(utils.NET, "received puback, id:", m.MessageId)
				// c.receipts.get(msg.MsgId()) <- Receipt{}
				// c.receipts.end(msg.MsgId())
				c.getToken(m.MessageId).flowComplete()
				c.freeID(m.MessageId)
			case *packets.PubrecPacket:
				utils.DEBUG.Println(utils.NET, "received pubrec, id:", m.MessageId)
				prel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
				prel.MessageId = m.MessageId
				select {
				case c.oboundP <- &PacketAndToken{p: prel, t: nil}:
				case <-c.stop:
				}
			case *packets.PubrelPacket:
				utils.DEBUG.Println(utils.NET, "received pubrel, id:", m.MessageId)
				pc := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
				pc.MessageId = m.MessageId
				stores.PersistOutbound(c.persist, pc)
				select {
				case c.oboundP <- &PacketAndToken{p: pc, t: nil}:
				case <-c.stop:
				}
			case *packets.PubcompPacket:
				utils.DEBUG.Println(utils.NET, "received pubcomp, id:", m.MessageId)
				c.getToken(m.MessageId).flowComplete()
				c.freeID(m.MessageId)
			}
		case <-c.stop:
			utils.WARN.Println(utils.NET, "logic stopped")
			return
		}
	}
}

func errorWatch(c *client) {
	select {
	case <-c.stop:
		utils.WARN.Println(utils.NET, "errorWatch stopped")
		return
	case err := <-c.errors:
		utils.ERROR.Println(utils.NET, "error triggered, stopping")
		go c.internalConnLost(err)
		return
	}
}
