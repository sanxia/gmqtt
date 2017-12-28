package gmqtt

import (
	"errors"
	"sync/atomic"
	"time"
)

import (
	"github.com/sanxia/gmqtt/packets"
	"github.com/sanxia/gmqtt/utils"
)

func keepalive(c *client) {
	defer c.workers.Done()
	utils.DEBUG.Println(utils.PNG, "keepalive starting")
	var checkInterval int64
	var pingSent time.Time

	if c.options.KeepAlive > 10 {
		checkInterval = 5
	} else {
		checkInterval = c.options.KeepAlive / 2
	}

	intervalTicker := time.NewTicker(time.Duration(checkInterval * int64(time.Second)))
	defer intervalTicker.Stop()

	for {
		select {
		case <-c.stop:
			utils.DEBUG.Println(utils.PNG, "keepalive stopped")
			return
		case <-intervalTicker.C:
			utils.DEBUG.Println(utils.PNG, "ping check", time.Now().Unix()-atomic.LoadInt64(&c.lastSent))
			if time.Now().Unix()-atomic.LoadInt64(&c.lastSent) >= c.options.KeepAlive || time.Now().Unix()-atomic.LoadInt64(&c.lastReceived) >= c.options.KeepAlive {
				if atomic.LoadInt32(&c.pingOutstanding) == 0 {
					utils.DEBUG.Println(utils.PNG, "keepalive sending ping")
					ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
					//We don't want to wait behind large messages being sent, the Write call
					//will block until it it able to send the packet.
					atomic.StoreInt32(&c.pingOutstanding, 1)
					ping.Write(c.conn)
					atomic.StoreInt64(&c.lastSent, time.Now().Unix())
					pingSent = time.Now()
				}
			}
			if atomic.LoadInt32(&c.pingOutstanding) > 0 && time.Now().Sub(pingSent) >= c.options.PingTimeout {
				utils.CRITICAL.Println(utils.PNG, "pingresp not received, disconnecting")
				c.errors <- errors.New("pingresp not received, disconnecting")
				return
			}
		}
	}
}
