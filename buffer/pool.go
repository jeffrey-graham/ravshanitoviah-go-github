package buffer

import (
	"bytes"
	"sync"
	"sync/atomic"
)

const maxBufferSize = 64 * 1024

// Page represents an in-memory buffer page managed by BufferPoolManager.
type Page struct {
	ID       int
	Data     []byte
	PinCount int
	IsDirty  bool
}

// Reset clears the page payload and resets metadata before returning to the free list.
func (p *Page) Reset() {
	for i := range p.Data {
		p.Data[i] = 0
	}
	p.ID = -1
	p.PinCount = 0
	p.IsDirty = false
}

// Pool manages a pool of reusable byte buffers and fixed-capacity pages without leaking memory.
type Pool struct {
	mu       sync.Mutex
	pool     sync.Pool
	puts     atomic.Int64
	gets     atomic.Int64
	pages    map[int]*Page
	freeList []*Page
	capacity int
}

// NewPool creates a new bounded buffer and page pool.
func NewPool(capacity int) *Pool {
	if capacity <= 0 {
		capacity = 64
	}
	p := &Pool{
		pages:    make(map[int]*Page, capacity),
		freeList: make([]*Page, 0, capacity),
		capacity: capacity,
	}
	p.pool.New = func() any {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	}
	for i := 0; i < capacity; i++ {
		p.freeList = append(p.freeList, &Page{ID: -1, Data: make([]byte, 4096)})
	}
	return p
}

// Get retrieves a reusable bytes.Buffer from the pool.
func (p *Pool) Get() *bytes.Buffer {
	p.gets.Add(1)
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// Put resets and returns a bytes.Buffer to the pool if within maxBufferSize.
func (p *Pool) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > maxBufferSize {
		return
	}
	buf.Reset()
	p.puts.Add(1)
	p.pool.Put(buf)
}

// FetchPage fetches or allocates a page in the buffer pool.
func (p *Pool) FetchPage(pageID int) *Page {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pg, ok := p.pages[pageID]; ok {
		pg.PinCount++
		return pg
	}
	var pg *Page
	if len(p.freeList) > 0 {
		last := len(p.freeList) - 1
		pg = p.freeList[last]
		p.freeList = p.freeList[:last]
	} else {
		pg = p.evictLocked()
		if pg == nil {
			return nil
		}
	}
	pg.Reset()
	pg.ID = pageID
	pg.PinCount = 1
	p.pages[pageID] = pg
	return pg
}

// UnpinPage decrements the pin count for a page regardless of dirty state and reclaims unpinned pages.
func (p *Pool) UnpinPage(pageID int, isDirty bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	pg, ok := p.pages[pageID]
	if !ok || pg.PinCount <= 0 {
		return false
	}
	if isDirty {
		pg.IsDirty = true
	}
	pg.PinCount--
	if pg.PinCount == 0 && !pg.IsDirty {
		delete(p.pages, pageID)
		pg.Reset()
		p.freeList = append(p.freeList, pg)
	}
	return true
}

func (p *Pool) evictLocked() *Page {
	for id, pg := range p.pages {
		if pg.PinCount == 0 {
			delete(p.pages, id)
			pg.Reset()
			return pg
		}
	}
	return nil
}
