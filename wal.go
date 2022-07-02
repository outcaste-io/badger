package badger

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"

	"github.com/outcaste-io/badger/v3/skl"
	"github.com/outcaste-io/badger/v3/y"
	"github.com/pkg/errors"
)

var errStop = errors.New("Stop iteration")
var errTruncate = errors.New("Do truncate")

type request struct {
	// Input values
	Skl     *skl.Skiplist
	Entries []*Entry
	Wg      sync.WaitGroup
	Err     error
	ref     int32
}

type handoverRequest struct {
	skl      *skl.Skiplist
	callback func()
	err      error
	wg       sync.WaitGroup
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Wg = sync.WaitGroup{}
	req.Err = nil
	req.ref = 0
}

func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	req.Entries = nil
	requestPool.Put(req)
}

func (req *request) Wait() error {
	req.Wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}

type requests []*request

func (reqs requests) DecrRef() {
	for _, req := range reqs {
		req.DecrRef()
	}
}

func (reqs requests) IncrRef() {
	for _, req := range reqs {
		req.IncrRef()
	}
}

func errFile(err error, path string, msg string) error {
	return fmt.Errorf("%s. Path=%s. Error=%w", msg, path, err)
}

type logEntry func(e Entry) error

type safeRead struct {
	k []byte
	v []byte

	recordOffset uint32
	lf           *logFile
}

// hashReader implements io.Reader, io.ByteReader interfaces. It also keeps track of the number
// bytes read. The hashReader writes to h (hash) what it reads from r.
type hashReader struct {
	r         io.Reader
	h         hash.Hash32
	bytesRead int // Number of bytes read.
}

func newHashReader(r io.Reader) *hashReader {
	hash := crc32.New(y.CastagnoliCrcTable)
	return &hashReader{
		r: r,
		h: hash,
	}
}

// Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure.
func (t *hashReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil {
		return n, err
	}
	t.bytesRead += n
	return t.h.Write(p[:n])
}

// ReadByte reads exactly one byte from the reader. Returns error on failure.
func (t *hashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)
	return b[0], err
}

// Sum32 returns the sum32 of the underlying hash.
func (t *hashReader) Sum32() uint32 {
	return t.h.Sum32()
}

// Entry reads an entry from the provided reader. It also validates the checksum for every entry
// read. Returns error on failure.
func (r *safeRead) Entry(reader io.Reader) (*Entry, error) {
	tee := newHashReader(reader)
	var h header
	hlen, err := h.DecodeFrom(tee)
	if err != nil {
		return nil, err
	}
	if h.klen > uint32(1<<16) { // Key length must be below uint16.
		return nil, errTruncate
	}
	kl := int(h.klen)
	if cap(r.k) < kl {
		r.k = make([]byte, 2*kl)
	}
	vl := int(h.vlen)
	if cap(r.v) < vl {
		r.v = make([]byte, 2*vl)
	}

	e := &Entry{}
	e.offset = r.recordOffset
	e.hlen = hlen
	buf := make([]byte, h.klen+h.vlen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			err = errTruncate
		}
		return nil, err
	}
	if r.lf.encryptionEnabled() {
		if buf, err = r.lf.decryptKV(buf[:], r.recordOffset); err != nil {
			return nil, err
		}
	}
	e.Key = buf[:h.klen]
	e.Value = buf[h.klen:]
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			err = errTruncate
		}
		return nil, err
	}
	crc := y.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		return nil, errTruncate
	}
	e.meta = h.meta
	e.UserMeta = h.userMeta
	e.ExpiresAt = h.expiresAt
	return e, nil
}
