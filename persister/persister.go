package persister

import (
	"kvraft/config"

	"github.com/jmhodges/levigo"
)

type Persister struct {
	db *levigo.DB
}

func MakePersister(leveldbPath string) *Persister {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(leveldbPath, opts)
	config.CheckError("make leveldb instance error", err)

	return &Persister{db}
}

func (this *Persister) Close() {
	this.db.Close()
}

func (this *Persister) SaveRaftState(key []byte, value []byte) error {
	wo := levigo.NewWriteOptions()
	return this.db.Put(wo, key, value)
}

func (this *Persister) ReadRaftState(key []byte) ([]byte, error) {
	ro := levigo.NewReadOptions()
	return this.db.Get(ro, key)
}

func (this *Persister) Get(key []byte) ([]byte, error) {
	ro := levigo.NewReadOptions()
	return this.db.Get(ro, key)
}

func (this *Persister) Put(key []byte, value []byte) error {
	wo := levigo.NewWriteOptions()
	return this.db.Put(wo, key, value)
}

func (this *Persister) Del(key []byte) error {
	wo := levigo.NewWriteOptions()
	return this.db.Delete(wo, key)
}
