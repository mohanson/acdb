package acdb

import (
	"encoding/json"
	"os"
	"path"
	"sync"

	"github.com/mohanson/doa"
	"github.com/mohanson/lru"
)

// Driver is the interface that wraps the Set/Get and Del method.
//
// Get gets and returns the bytes or any error encountered. If the key does not exist, ErrNotExist will be returned.
// Set sets bytes with given k.
// Del dels bytes with given k. If the key does not exist, ErrNotExist will be returned.
type Driver interface {
	Get(k string) ([]byte, error)
	Set(k string, v []byte) error
	Del(k string) error
}

// MemDriver cares to store data on memory, this means that MemDriver is fast. Since there is no expiration mechanism,
// be careful that it might eats up all your memory.
type MemDriver struct {
	data map[string][]byte
}

// NewMemDriver returns a MemDriver.
func NewMemDriver() *MemDriver {
	return &MemDriver{
		data: map[string][]byte{},
	}
}

func (d *MemDriver) Get(k string) ([]byte, error) {
	v, b := d.data[k]
	if b {
		return v, nil
	}
	return nil, os.ErrNotExist
}

func (d *MemDriver) Set(k string, v []byte) error {
	d.data[k] = v
	return nil
}

func (d *MemDriver) Del(k string) error {
	delete(d.data, k)
	return nil
}

// DocDriver use the OS's file system to manage data. In general, any high frequency operation is not recommended
// unless you have an enough reason.
type DocDriver struct {
	root string
}

// NewDocDriver returns a DocDriver.
func NewDocDriver(root string) *DocDriver {
	doa.Try1(os.MkdirAll(root, 0755))
	return &DocDriver{
		root: root,
	}
}

func (d *DocDriver) Get(k string) ([]byte, error) {
	return os.ReadFile(path.Join(d.root, k))
}

func (d *DocDriver) Set(k string, v []byte) error {
	return os.WriteFile(path.Join(d.root, k), v, 0644)
}

func (d *DocDriver) Del(k string) error {
	return os.Remove(path.Join(d.root, k))
}

// In computing, cache algorithms (also frequently called cache replacement algorithms or cache replacement policies)
// are optimizing instructions, or algorithms, that a computer program or a hardware-maintained structure can utilize
// in order to manage a cache of information stored on the computer. Caching improves performance by keeping recent or
// often-used data items in a memory locations that are faster or computationally cheaper to access than normal memory
// stores. When the cache is full, the algorithm must choose which items to discard to make room for the new ones.
//
// Least recently used (LRU), discards the least recently used items first. It has a fixed size(for limit memory usages)
// and O(1) time lookup.
type LruDriver struct {
	data *lru.Cache
}

// NewLruDriver returns a LruDriver.
func NewLruDriver(size int) *LruDriver {
	return &LruDriver{
		data: lru.New(size),
	}
}

func (d *LruDriver) Get(k string) ([]byte, error) {
	v, b := d.data.Get(k)
	if b {
		return v.([]byte), nil
	}
	return nil, os.ErrNotExist
}

func (d *LruDriver) Set(k string, v []byte) error {
	d.data.Set(k, v)
	return nil
}

func (d *LruDriver) Del(k string) error {
	d.data.Del(k)
	return nil
}

// MapDriver is based on DocDriver and use LruDriver to provide caching at its
// interface layer. The size of LruDriver is always 1024.
type MapDriver struct {
	doc *DocDriver
	lru *LruDriver
}

// NewMapDriver returns a MapDriver.
func NewMapDriver(root string) *MapDriver {
	return &MapDriver{
		doc: NewDocDriver(root),
		lru: NewLruDriver(1024),
	}
}

func (d *MapDriver) Get(k string) ([]byte, error) {
	var (
		buf []byte
		err error
	)
	buf, err = d.lru.Get(k)
	if err == nil {
		return buf, nil
	}
	buf, err = d.doc.Get(k)
	if err != nil {
		return nil, err
	}
	err = d.lru.Set(k, buf)
	return buf, err
}

func (d *MapDriver) Set(k string, v []byte) error {
	if err := d.lru.Set(k, v); err != nil {
		return err
	}
	if err := d.doc.Set(k, v); err != nil {
		return err
	}
	return nil
}

func (d *MapDriver) Del(k string) error {
	if err := d.lru.Del(k); err != nil {
		return err
	}
	if err := d.doc.Del(k); err != nil {
		return err
	}
	return nil
}

type Client interface {
	Get(k string) ([]byte, error)
	Set(k string, v []byte) error
	GetDecode(string, interface{}) error
	SetEncode(string, interface{}) error
	Del(k string) error
}

// Emerge is a actuator of the given drive. Do not worry, Is's concurrency-safety.
type Emerge struct {
	driver Driver
	m      *sync.Mutex
}

// NewEmerge returns a Emerge.
func NewEmerge(driver Driver) *Emerge {
	return &Emerge{driver: driver, m: &sync.Mutex{}}
}

func (e *Emerge) Get(k string) ([]byte, error) {
	e.m.Lock()
	defer e.m.Unlock()
	return e.driver.Get(k)
}
func (e *Emerge) Set(k string, v []byte) error {
	e.m.Lock()
	defer e.m.Unlock()
	return e.driver.Set(k, v)
}

func (e *Emerge) GetDecode(k string, v interface{}) error {
	b, err := e.Get(k)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (e *Emerge) SetEncode(k string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.Set(k, b)
}

func (e *Emerge) Del(k string) error {
	e.m.Lock()
	defer e.m.Unlock()
	return e.driver.Del(k)
}

// Mem returns a concurrency-safety Client with MemDriver.
func Mem() Client { return NewEmerge(NewMemDriver()) }

// Doc returns a concurrency-safety Client with DocDriver.
func Doc(root string) Client { return NewEmerge(NewDocDriver(root)) }

// Lru returns a concurrency-safety Client with LruDriver.
func Lru(size int) Client { return NewEmerge(NewLruDriver(size)) }

// Map returns a concurrency-safety Client with MapDriver.
func Map(root string) Client { return NewEmerge(NewMapDriver(root)) }
