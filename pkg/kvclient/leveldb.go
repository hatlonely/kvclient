package kvclient

import (
	"fmt"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// NewLevelDBBuilder create a new LevelDBBuilder
func NewLevelDBBuilder() *LevelDBBuilder {
	return &LevelDBBuilder{
		Directory:     "leveldb/",
		DontFillCache: false,
		Strict:        0,
		NoWriteMerge:  false,
		Sync:          false,
	}
}

// LevelDBBuilder builder
type LevelDBBuilder struct {
	Directory     string
	DontFillCache bool
	Strict        int
	NoWriteMerge  bool
	Sync          bool
}

// WithDirectory option
func (b *LevelDBBuilder) WithDirectory(directory string) *LevelDBBuilder {
	b.Directory = directory
	return b
}

// WithDontFillCache option
func (b *LevelDBBuilder) WithDontFillCache(dontFillCache bool) *LevelDBBuilder {
	b.DontFillCache = dontFillCache
	return b
}

// WithStrict option
func (b *LevelDBBuilder) WithStrict(strict int) *LevelDBBuilder {
	b.Strict = strict
	return b
}

// WithNoWriteMerge option
func (b *LevelDBBuilder) WithNoWriteMerge(noWriteMerge bool) *LevelDBBuilder {
	b.NoWriteMerge = noWriteMerge
	return b
}

// WithSync option
func (b *LevelDBBuilder) WithSync(sync bool) *LevelDBBuilder {
	b.Sync = sync
	return b
}

// Build a new LevelDB
func (b *LevelDBBuilder) Build() (*LevelDB, error) {
	db, err := leveldb.OpenFile(b.Directory, nil)
	if err != nil {
		return nil, err
	}

	roptions := &opt.ReadOptions{
		DontFillCache: b.DontFillCache,
		Strict:        opt.Strict(b.Strict),
	}

	woptions := &opt.WriteOptions{
		NoWriteMerge: b.NoWriteMerge,
		Sync:         b.Sync,
	}

	return &LevelDB{
		db:       db,
		roptions: roptions,
		woptions: woptions,
	}, nil
}

// LevelDB datasource
type LevelDB struct {
	db       *leveldb.DB
	roptions *opt.ReadOptions
	woptions *opt.WriteOptions
}

// Close leveldb
func (l *LevelDB) Close() error {
	return l.db.Close()
}

// Get key
func (l *LevelDB) Get(key string) ([]byte, error) {
	val, err := l.db.Get([]byte(key), l.roptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Set key value
func (l *LevelDB) Set(key string, val []byte) error {
	return l.db.Put([]byte(key), val, l.woptions)
}

// Del key
func (l *LevelDB) Del(key string) error {
	return l.db.Delete([]byte(key), l.woptions)
}

// SetBatch keys vals
func (l *LevelDB) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	var errs []error
	batch := &leveldb.Batch{}
	for i := range keys {
		batch.Put([]byte(keys[i]), vals[i])
		errs = append(errs, nil)
	}
	err := l.db.Write(batch, l.woptions)

	return errs, err
}

// SetEx set with expiration. leveldb doesn't support expiration
func (l *LevelDB) SetEx(key string, val []byte, expiration time.Duration) error {
	panic("Unsupport operation SetEx")
}

// SetNx set if not exist.
func (l *LevelDB) SetNx(key string, val []byte) error {
	val, err := l.Get(key)
	if err != nil {
		return err
	}

	if val != nil {
		return nil
	}

	return l.Set(key, val)
}

// SetExNx set with expiration if not exist. leveldb doesn't support.
func (l *LevelDB) SetExNx(key string, val []byte, expiration time.Duration) error {
	panic("Unsupport operation SetEx")
}
