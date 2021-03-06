package gmqtt

import (
	"fmt"
	"sync"
	"time"
)

import (
	"github.com/sanxia/gmqtt/utils"
)

// MId is 16 bit message id as specified by the MQTT spec.
// In general, these values should not be depended upon by
// the client application.
type MId uint16

type messageIds struct {
	sync.RWMutex
	index map[uint16]tokenCompletor
}

const (
	midMin uint16 = 1
	midMax uint16 = 65535
)

func (mids *messageIds) cleanUp() {
	mids.Lock()
	for _, token := range mids.index {
		switch t := token.(type) {
		case *PublishToken:
			t.err = fmt.Errorf("Connection lost before Publish completed")
		case *SubscribeToken:
			t.err = fmt.Errorf("Connection lost before Subscribe completed")
		case *UnsubscribeToken:
			t.err = fmt.Errorf("Connection lost before Unsubscribe completed")
		case nil:
			continue
		}
		token.flowComplete()
	}
	mids.index = make(map[uint16]tokenCompletor)
	mids.Unlock()
}

func (mids *messageIds) freeID(id uint16) {
	mids.Lock()
	delete(mids.index, id)
	mids.Unlock()
}

func (mids *messageIds) getID(t tokenCompletor) uint16 {
	mids.Lock()
	defer mids.Unlock()
	for i := midMin; i < midMax; i++ {
		if _, ok := mids.index[i]; !ok {
			mids.index[i] = t
			return i
		}
	}
	return 0
}

func (mids *messageIds) getToken(id uint16) tokenCompletor {
	mids.RLock()
	defer mids.RUnlock()
	if token, ok := mids.index[id]; ok {
		return token
	}
	return &DummyToken{id: id}
}

type DummyToken struct {
	id uint16
}

func (d *DummyToken) Wait() bool {
	return true
}

func (d *DummyToken) WaitTimeout(t time.Duration) bool {
	return true
}

func (d *DummyToken) flowComplete() {
	utils.ERROR.Printf("A lookup for token %d returned nil\n", d.id)
}

func (d *DummyToken) Error() error {
	return nil
}
