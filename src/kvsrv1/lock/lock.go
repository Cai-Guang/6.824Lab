package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	lockKey string
	id      string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lockKey: l, id: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		id, version, err := lk.ck.Get(lk.lockKey)

		if err == rpc.ErrNoKey {
			id = ""
			version = 0
		}

		if id == "" { // No lock found, acquire the lock
			putErr := lk.ck.Put(lk.lockKey, lk.id, version)
			if putErr == rpc.OK {
				return
			}
		} else if id == lk.id { // Lock found, already acquired
			return
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	for {
		id, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey || id != lk.id {
			return
		}

		putErr := lk.ck.Put(lk.lockKey, "", version)
		if putErr == rpc.OK {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

}
