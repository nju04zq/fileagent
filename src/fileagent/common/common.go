package common

import (
	"errors"
	"fmt"
	"hash/crc64"
	"os"
	"sync"
	"time"

	"fileagent/log"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB

	CHUNK_SIZE = 1 * MB
)

const (
	INT64_BYTES      = 8
	BYTE_BITS        = 8
	CHECKSUM_BYTES   = 16 // CRC64, in string
	CRC64_BLOCK_SIZE = 1 * MB

	SEQ_SIZE    = INT64_BYTES
	SEQ_INVALID = -1
	SEQ_META    = -2

	FILE_AGENT_THREADS      = 8
	FILE_AGENT_BLOCK_SIZE   = 256 * KB
	FILE_AGENT_CHUNK_BLOCKS = 4
	FILE_AGENT_CHUNK_SIZE   = FILE_AGENT_BLOCK_SIZE * FILE_AGENT_CHUNK_BLOCKS
	FILE_AGENT_POOL_CHUNKS  = 4
	FILE_AGENT_POOL_SIZE    = FILE_AGENT_CHUNK_SIZE * FILE_AGENT_POOL_CHUNKS
	FILE_AGENT_RD_BUF_SIZE  = FILE_AGENT_THREADS
	FILE_AGENT_WR_BUF_SIZE  = 4

	FILE_AGENT_SVR_ACCEPT_TIMEOUT = 1 * time.Second
	FILE_AGENT_SVR_WAIT_TIMEOUT   = 10 * time.Second
	FILE_AGENT_RW_TIMEOUT         = 10 * time.Second
	FILE_AGENT_PROGRESS_INTERVAL  = 500 * time.Millisecond
)

func Clonebuf(buf []byte) []byte {
	dst := make([]byte, len(buf))
	for i, ch := range buf {
		dst[i] = ch
	}
	return dst
}

var ABORT_ERR = errors.New("Aborted")

type FileAgentBlock struct {
	Seq int64
	Buf []byte
}

type TransferRate struct {
	mutex  sync.Mutex
	Holder string
	Total  int64
	bytes  int64
	start  time.Time
}

func (rate *TransferRate) Add(inc int) {
	rate.mutex.Lock()
	defer rate.mutex.Unlock()
	if rate.start.IsZero() {
		rate.start = time.Now()
	}
	rate.bytes += int64(inc)
}

func (rate *TransferRate) String() string {
	rate.mutex.Lock()
	defer rate.mutex.Unlock()
	total := ""
	if rate.Total > 0 {
		total = fmt.Sprintf("/%s", PrettyBytes(rate.Total))
	}
	if rate.bytes == 0 {
		return fmt.Sprintf("%s: 0B%s 0B/s", rate.Holder, total)
	}
	d := time.Now().Sub(rate.start) / time.Millisecond
	if d == 0 {
		return fmt.Sprintf("%s: %s%s -", rate.Holder, PrettyBytes(rate.bytes), total)
	}
	speed := rate.bytes / int64(d) * int64(time.Second/time.Millisecond)
	return fmt.Sprintf("%s: %s%s %s/s", rate.Holder, PrettyBytes(rate.bytes), total, PrettyBytes(speed))
}

func (rate *TransferRate) ETA() string {
	rate.mutex.Lock()
	defer rate.mutex.Unlock()
	if rate.Total == 0 || rate.bytes == 0 {
		return "ETA -"
	}
	d := time.Now().Sub(rate.start) / time.Millisecond
	if d == 0 {
		return "ETA -"
	}
	speed := rate.bytes / int64(d)
	left := rate.Total - rate.bytes
	eta := left / speed * int64(time.Millisecond)
	return fmt.Sprintf("ETA %s", PrettyDuration(time.Duration(eta)))
}

func PrettyBytes(bytes int64) string {
	if bytes < 1*KB {
		return fmt.Sprintf("%dB", bytes)
	} else if bytes < 1*MB {
		return fmt.Sprintf("%.2fKB", float64(bytes)/KB)
	} else if bytes < 1*GB {
		return fmt.Sprintf("%.2fMB", float64(bytes)/MB)
	} else {
		return fmt.Sprintf("%.2fGB", float64(bytes)/GB)
	}
}

func PrettyDuration(d time.Duration) string {
	if d < time.Second {
		return "0s"
	} else if d < time.Minute {
		return fmt.Sprintf("%ds", d/time.Second)
	} else {
		m := d / time.Minute
		s := (d % time.Minute) / time.Second
		if s == 0 {
			return fmt.Sprintf("%dm", m)
		} else {
			return fmt.Sprintf("%dm%ds", m, s)
		}
	}
	return ""
}

func Int64ToByte(num int64) []byte {
	i := uint64(num)
	buf := make([]byte, INT64_BYTES)
	j := len(buf) - 1
	for i > 0 {
		buf[j] = byte(i & 0xff)
		j--
		i >>= BYTE_BITS
	}
	return buf
}

func ByteToInt64(buf []byte) int64 {
	log.Debug("byteToInt64 %v", buf)
	var seq int64
	for i := 0; i < len(buf); i++ {
		seq <<= BYTE_BITS
		seq |= int64(buf[i])
	}
	return seq
}

type OnelineProgress struct {
	prevLen      int
	notifyFinish chan struct{}
	done         chan struct{}
}

func (p *OnelineProgress) Init() *OnelineProgress {
	p.notifyFinish = make(chan struct{})
	p.done = make(chan struct{})
	return p
}

func (p *OnelineProgress) Finish() {
	log.Info("Notify progress finish")
	p.notifyFinish <- struct{}{}
	log.Info("Wait for progress finish")
	<-p.done
	log.Info("Progress finished")
}

func (p *OnelineProgress) Exit() <-chan struct{} {
	return p.notifyFinish
}

func (p *OnelineProgress) NotifyDone() {
	p.done <- struct{}{}
}

func (p *OnelineProgress) Update(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if !log.IsConsoleLoggerOff() {
		log.Info(msg)
		return
	}
	p.clearPrev(msg)
	p.dumpOneline(msg)
}

func (p *OnelineProgress) clearPrev(msg string) {
	if len(msg) < p.prevLen {
		p.dumpOneline(p.spaces(p.prevLen))
	}
	p.prevLen = len(msg)
}

func (p *OnelineProgress) spaces(i int) string {
	buf := make([]byte, i)
	for i, _ := range buf {
		buf[i] = ' '
	}
	return string(buf)
}

func (p *OnelineProgress) dumpOneline(s string) {
	fmt.Printf("\r" + s)
}

func monitorMd5(progress *OnelineProgress, rate *TransferRate) {
	startTs := time.Now()
	update := func() {
		duration := PrettyDuration(time.Now().Sub(startTs))
		progress.Update("%s %s", rate.String(), duration)
	}
	abort := false
	ticker := time.NewTicker(500 * time.Millisecond)
	for !abort {
		select {
		case <-ticker.C:
			update()
		case <-progress.Exit():
			abort = true
		}
	}
	update()
	fmt.Println()
	progress.NotifyDone()
	log.Info("FileAgent server monitor exit")
}

func uint64ToHex(a uint64) string {
	buf := make([]byte, CHECKSUM_BYTES)
	for i := 0; i < len(buf); i++ {
		buf[i] = '0'
	}
	tbl := "0123456789ABCDEF"
	for i := len(buf) - 1; i >= 0; i-- {
		j := a & 0xf
		buf[i] = tbl[j]
		a >>= 4
	}
	return string(buf)
}

func CRC64OfFile(fpath string) (string, error) {
	fmt.Println("Calculating CRC64...")
	fp, err := os.Open(fpath)
	if err != nil {
		return "", err
	}
	defer fp.Close()
	tbl := crc64.MakeTable(crc64.ECMA)
	buf := make([]byte, CRC64_BLOCK_SIZE)
	rate := &TransferRate{Holder: "CRC64"}
	progress := new(OnelineProgress).Init()
	go monitorMd5(progress, rate)
	var m int
	var crcsum uint64
	for {
		m, err = fp.Read(buf)
		if err != nil {
			break
		}
		crcsum = crc64.Update(crcsum, tbl, buf[:m])
		rate.Add(m)
	}
	progress.Finish()
	checksum := uint64ToHex(crcsum)
	fmt.Printf("CRC64 %s\n", checksum)
	return checksum, nil
}
