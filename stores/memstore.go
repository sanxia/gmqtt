package stores

import (
	"sync"
)

import (
	"github.com/sanxia/gmqtt/packets"
	"github.com/sanxia/gmqtt/utils"
)

type MemoryStore struct {
	sync.RWMutex
	messages map[string]packets.ControlPacket
	opened   bool
}

func NewMemoryStore() *MemoryStore {
	store := &MemoryStore{
		messages: make(map[string]packets.ControlPacket),
		opened:   false,
	}
	return store
}

// Open initializes a MemoryStore instance.
func (store *MemoryStore) Open() {
	store.Lock()
	defer store.Unlock()
	store.opened = true
	utils.DEBUG.Println(utils.STR, "memorystore initialized")
}

// Put takes a key and a pointer to a Message and stores the
// message.
func (store *MemoryStore) Put(key string, message packets.ControlPacket) {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		utils.ERROR.Println(utils.STR, "Trying to use memory store, but not open")
		return
	}
	store.messages[key] = message
}

// Get takes a key and looks in the store for a matching Message
// returning either the Message pointer or nil.
func (store *MemoryStore) Get(key string) packets.ControlPacket {
	store.RLock()
	defer store.RUnlock()
	if !store.opened {
		utils.ERROR.Println(utils.STR, "Trying to use memory store, but not open")
		return nil
	}
	mid := mIDFromKey(key)
	m := store.messages[key]
	if m == nil {
		utils.CRITICAL.Println(utils.STR, "memorystore get: message", mid, "not found")
	} else {
		utils.DEBUG.Println(utils.STR, "memorystore get: message", mid, "found")
	}
	return m
}

// All returns a slice of strings containing all the keys currently
// in the MemoryStore.
func (store *MemoryStore) All() []string {
	store.RLock()
	defer store.RUnlock()
	if !store.opened {
		utils.ERROR.Println(utils.STR, "Trying to use memory store, but not open")
		return nil
	}
	keys := []string{}
	for k := range store.messages {
		keys = append(keys, k)
	}
	return keys
}

// Del takes a key, searches the MemoryStore and if the key is found
// deletes the Message pointer associated with it.
func (store *MemoryStore) Del(key string) {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		utils.ERROR.Println(utils.STR, "Trying to use memory store, but not open")
		return
	}
	mid := mIDFromKey(key)
	m := store.messages[key]
	if m == nil {
		utils.WARN.Println(utils.STR, "memorystore del: message", mid, "not found")
	} else {
		delete(store.messages, key)
		utils.DEBUG.Println(utils.STR, "memorystore del: message", mid, "was deleted")
	}
}

// Close will disallow modifications to the state of the store.
func (store *MemoryStore) Close() {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		utils.ERROR.Println(utils.STR, "Trying to close memory store, but not open")
		return
	}
	store.opened = false
	utils.DEBUG.Println(utils.STR, "memorystore closed")
}

// Reset eliminates all persisted message data in the store.
func (store *MemoryStore) Reset() {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		utils.ERROR.Println(utils.STR, "Trying to reset memory store, but not open")
	}
	store.messages = make(map[string]packets.ControlPacket)
	utils.WARN.Println(utils.STR, "memorystore wiped")
}
