package fs

import (
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
)

type closeSignalFileReader struct {
	closed chan struct{}
	count  atomic.Int32
}

func (r *closeSignalFileReader) Read(_ meta.Context, _ uint64, buf []byte) (int, syscall.Errno) {
	buf[0] = 'x'
	return 1, 0
}

func (*closeSignalFileReader) GetLength() uint64 { return 1 }

func (r *closeSignalFileReader) Close(meta.Context) {
	if r.count.Add(1) == 1 {
		close(r.closed)
	}
}

type closeSignalDataReader struct {
	file vfs.FileReader
}

func (r *closeSignalDataReader) Open(Ino, uint64) vfs.FileReader { return r.file }
func (*closeSignalDataReader) Truncate(Ino, uint64)              {}
func (*closeSignalDataReader) Invalidate(Ino, uint64, uint64)    {}
func (*closeSignalDataReader) Shutdown()                         {}

func newCloseTestMeta(t *testing.T) meta.Meta {
	t.Helper()
	m := meta.NewClient("sqlite3://"+filepath.Join(t.TempDir(), "meta.db"), meta.DefaultConf())
	t.Cleanup(func() { _ = m.Shutdown() })
	if err := m.Init(&meta.Format{Name: "test", BlockSize: 4096}, true); err != nil {
		t.Fatalf("initialize local SQLite metadata: %v", err)
	}
	if err := m.NewSession(true); err != nil {
		t.Fatalf("open local SQLite metadata session: %v", err)
	}
	return m
}

func TestFileCloseReleasesZeroModeReader(t *testing.T) {
	reader := &closeSignalFileReader{closed: make(chan struct{})}
	fs := &FileSystem{
		reader:                &closeSignalDataReader{file: reader},
		readSizeHistogram:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_read_size"}),
		opsDurationsHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_operation_duration"}),
	}
	f := &File{
		fs:    fs,
		path:  "/file",
		flags: 0,
		info:  &FileStat{attr: &meta.Attr{Typ: meta.TypeFile, Length: 1}},
	}

	if n, err := f.Read(meta.Background(), make([]byte, 1)); err != nil || n != 1 {
		t.Fatalf("zero-mode read = (%d, %v), want (1, nil)", n, err)
	}
	if f.rdata == nil {
		t.Fatal("zero-mode read did not create a reader")
	}
	if err := f.Close(meta.Background()); err != 0 {
		t.Fatalf("close zero-mode file: %v", err)
	}
	if f.rdata != nil {
		t.Fatal("close did not detach the reader")
	}

	select {
	case <-reader.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delayed reader close")
	}
	if got := reader.count.Load(); got != 1 {
		t.Fatalf("reader close count = %d, want 1", got)
	}
}

func TestFileCloseReleasesReadModeReader(t *testing.T) {
	reader := &closeSignalFileReader{closed: make(chan struct{})}
	f := &File{
		fs: &FileSystem{
			m:                     newCloseTestMeta(t),
			opsDurationsHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_operation_duration"}),
		},
		inode: meta.RootInode,
		path:  "/file",
		flags: meta.MODE_MASK_R,
		info:  &FileStat{attr: &meta.Attr{Typ: meta.TypeFile, Length: 1}},
		rdata: reader,
	}

	if err := f.Close(meta.Background()); err != 0 {
		t.Fatalf("close read-mode file: %v", err)
	}
	if f.rdata != nil {
		t.Fatal("close did not detach the reader")
	}
	select {
	case <-reader.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delayed reader close")
	}
	if got := reader.count.Load(); got != 1 {
		t.Fatalf("reader close count = %d, want 1", got)
	}
}
