package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"fileagent/common"
	"fileagent/log"
)

type FileAgentCltLite struct {
	ip       string
	port     int
	fpath    string
	fp       *os.File
	rxrate   *common.TransferRate
	progress *common.OnelineProgress
	startTs  time.Time
}

func (clt *FileAgentCltLite) Init(ip string, port int, fpath string) error {
	fp, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	clt.ip = ip
	clt.port = port
	clt.fpath = fpath
	clt.fp = fp
	clt.rxrate = &common.TransferRate{Holder: "RX"}
	clt.progress = new(common.OnelineProgress).Init()
	return nil
}

func (clt *FileAgentCltLite) Run() error {
	log.Info("FileAgent client start running")
	t1 := time.Now()
	clt.startTs = t1
	err := clt.recv()
	t2 := time.Now()
	clt.fp.Close()
	if err != nil {
		os.Remove(clt.fpath)
		return err
	}
	fmt.Printf("FileAgent finished transfer in %v\n", t2.Sub(t1))
	log.Info("FileAgent client closed")
	return nil
}

func (clt *FileAgentCltLite) recv() error {
	go clt.monitor()
	defer clt.progress.Finish()
	defer clt.fp.Close()
	addr := &net.TCPAddr{
		IP:   net.ParseIP(clt.ip),
		Port: clt.port,
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	chunk := make([]byte, common.CHUNK_SIZE)
	for {
		err = conn.SetDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			log.Error("FileAgent client lite, fail to set dealine, %v", err)
			return err
		}
		m, err := conn.Read(chunk)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Error("FileAgent client lite, fail to read, %v", err)
			return err
		}
		n, err := clt.fp.Write(chunk[:m])
		if err != nil {
			log.Error("FileAgent client lite, fail to write, %v", err)
			return err
		}
		if n != m {
			return fmt.Errorf("FileAgent client lite, should write %d, get %d", m, n)
		}
		clt.rxrate.Add(m)
	}
	return nil
}

func (clt *FileAgentCltLite) monitor() {
	update := func() {
		rx := clt.rxrate.String()
		duration := common.PrettyDuration(time.Now().Sub(clt.startTs))
		clt.progress.Update("%s, %s", rx, duration)
	}
	abort := false
	ticker := time.NewTicker(common.FILE_AGENT_PROGRESS_INTERVAL)
	for !abort {
		select {
		case <-ticker.C:
			update()
		case <-clt.progress.Exit():
			abort = true
		}
	}
	update()
	fmt.Println()
	clt.progress.NotifyDone()
	log.Info("FileAgent server monitor exit")
}

type FileAgentClt struct {
	ip       string
	port     int
	fpath    string
	fsize    int64
	checksum string
	fp       *os.File
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	pool     *FileAgentPool
	wrdone   chan struct{}
	wrbuf    chan []byte
	err      chan error
	wrrate   *common.TransferRate
	rxrate   *common.TransferRate
	progress *common.OnelineProgress
	startTs  time.Time
}

func (clt *FileAgentClt) Init(ip string, port int, fpath string) error {
	fp, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	clt.ip = ip
	clt.port = port
	clt.fpath = fpath
	clt.fp = fp
	clt.wrbuf = make(chan []byte, common.FILE_AGENT_POOL_CHUNKS)
	clt.pool = new(FileAgentPool).init(clt.wrbuf)
	clt.wrdone = make(chan struct{})
	clt.err = make(chan error, common.FILE_AGENT_THREADS+1) // +1 for write thread
	clt.wrrate = &common.TransferRate{Holder: "WR"}
	clt.rxrate = &common.TransferRate{Holder: "RX"}
	clt.progress = new(common.OnelineProgress).Init()
	return nil
}

func (clt *FileAgentClt) Run(ctx context.Context) error {
	log.Info("FileAgent client start running")
	t1 := time.Now()
	clt.startTs = t1
	err := clt.recv(ctx)
	t2 := time.Now()
	clt.fp.Close()
	if err != nil {
		os.Remove(clt.fpath)
		return err
	}
	fmt.Printf("FileAgent finished transfer in %v\n", t2.Sub(t1))
	checksum, err := common.CRC64OfFile(clt.fpath)
	if err != nil {
		return err
	} else if checksum != clt.checksum {
		return fmt.Errorf("FileAgent checksum mismatch, %s != %s", checksum, clt.checksum)
	}
	fmt.Println("File checksum verified, transfer done.")
	log.Info("FileAgent client closed")
	return nil
}

func (clt *FileAgentClt) recv(ctx context.Context) error {
	ctx, clt.cancel = context.WithCancel(ctx)
	defer clt.cancel()
	go clt.write(ctx)
	go clt.monitor()
	err := clt.spawnRx(ctx)
	if err != nil {
		log.Error("FileAgent, fail to spawn RX %v", err)
		close(clt.wrbuf)
		return err
	}
	log.Info("FileAgent client wait for recv done...")
	clt.wg.Wait()
	defer clt.progress.Finish()
	if err := clt.geterr(); err != nil {
		log.Info("FileAgent client closed, err %v", err)
		return err
	}
	if err := clt.pool.finalflush(); err != nil {
		log.Error("FileAgent final flush failed, %v", err)
		return err
	}
	log.Info("FileAgent client wait for write done...")
	<-clt.wrdone
	log.Info("FileAgent client exit recv")
	return nil
}

func (clt *FileAgentClt) geterr() error {
	if len(clt.err) > 0 {
		return <-clt.err
	} else {
		return nil
	}
}

func (clt *FileAgentClt) monitor() {
	update := func() {
		wr, rx := clt.wrrate.String(), clt.rxrate.String()
		duration := common.PrettyDuration(time.Now().Sub(clt.startTs))
		eta := clt.wrrate.ETA()
		clt.progress.Update("%s, %s, %s, %s", wr, rx, duration, eta)
	}
	abort := false
	ticker := time.NewTicker(common.FILE_AGENT_PROGRESS_INTERVAL)
	for !abort {
		select {
		case <-ticker.C:
			update()
		case <-clt.progress.Exit():
			abort = true
		}
	}
	update()
	fmt.Println()
	clt.progress.NotifyDone()
	log.Info("FileAgent client monitor exit")
}

func (clt *FileAgentClt) write(ctx context.Context) {
	log.Info("FileAgent server, start writing...")
	err := clt.writeBuf(ctx)
	if err != nil {
		clt.err <- err
		clt.cancel()
	}
	clt.wrdone <- struct{}{}
	log.Info("FileAgent client, write done")
}

func (clt *FileAgentClt) writeBuf(ctx context.Context) error {
	for buf := range clt.wrbuf {
		log.Debug("FileAgent client write %d, %v", len(buf), buf)
		m, err := clt.fp.Write(buf)
		if err != nil {
			return err
		}
		log.Debug("FileAgent client write %d bytes", m)
		if m != len(buf) {
			return fmt.Errorf("Write %d, expect %d", m, len(buf))
		}
		clt.wrrate.Add(m)
	}
	return nil
}

func (clt *FileAgentClt) spawnRx(ctx context.Context) error {
	log.Info("FileAgent client spawn RXs")
	var err error
	addr := &net.TCPAddr{
		IP:   net.ParseIP(clt.ip),
		Port: clt.port,
	}
	rxs := make([]*FileAgentRx, 0, common.FILE_AGENT_THREADS)
	for i := 0; i < common.FILE_AGENT_THREADS; i++ {
		var conn *net.TCPConn
		conn, err = net.DialTCP("tcp", nil, addr)
		if err != nil {
			break
		}
		rx := &FileAgentRx{
			name:   fmt.Sprintf("%v", conn.LocalAddr()),
			cancel: clt.cancel,
			conn:   conn,
			pool:   clt.pool,
			err:    clt.err,
			rate:   clt.rxrate,
		}
		rx.init()
		if i == 0 {
			clt.fsize, clt.checksum, err = rx.readMeta()
			if err != nil {
				break
			}
			clt.rxrate.Total, clt.wrrate.Total = clt.fsize, clt.fsize
		}
		rxs = append(rxs, rx)
	}
	for _, rx := range rxs {
		if err != nil {
			rx.conn.Close()
			continue
		}
		clt.wg.Add(1)
		go func(rx *FileAgentRx) {
			defer clt.wg.Done()
			rx.run(ctx)
		}(rx)
	}
	return err
}

type FileAgentPool struct {
	mutex   sync.Mutex
	cond    *sync.Cond
	seqmin  int64
	seqsize int64
	seqmask []int
	buf     []byte
	wrbuf   chan<- []byte
}

func (pool *FileAgentPool) init(wrbuf chan<- []byte) *FileAgentPool {
	pool.cond = sync.NewCond(&pool.mutex)
	pool.seqmin = 0
	pool.seqsize = common.FILE_AGENT_POOL_CHUNKS * common.FILE_AGENT_CHUNK_BLOCKS
	pool.seqmask = make([]int, pool.seqsize)
	pool.buf = make([]byte, common.FILE_AGENT_POOL_SIZE)
	pool.wrbuf = wrbuf
	return pool
}

func (pool *FileAgentPool) put(block *common.FileAgentBlock) error {
	log.Debug("FileAgent pool, put block %d", block.Seq)
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	seq := block.Seq
	if seq < pool.seqmin {
		return fmt.Errorf("seq %d < seqmin %d", seq, pool.seqmin)
	}
	for seq >= pool.seqmin+pool.seqsize {
		pool.cond.Wait()
	}
	j := int(seq - pool.seqmin)
	if pool.seqmask[j] > 0 {
		return fmt.Errorf("seq %d already exist", seq)
	}
	pool.seqmask[j] = len(block.Buf)
	offset := j * common.FILE_AGENT_BLOCK_SIZE
	for i := 0; i < len(block.Buf); i++ {
		pool.buf[offset+i] = block.Buf[i]
	}
	pool.flushChunk()
	return nil
}

func (pool *FileAgentPool) flushChunk() {
	log.Debug("FileAgent pool mask before flush, %v", pool.seqmask)
	i, size := 0, 0
	for ; i < int(pool.seqsize); i++ {
		if pool.seqmask[i] == 0 {
			break
		}
		size += pool.seqmask[i]
	}
	if i < common.FILE_AGENT_CHUNK_BLOCKS {
		return
	}
	log.Debug("FileAgent, flush chunk, %d, %d", pool.seqmin, i)
	buf := common.Clonebuf(pool.buf[:size])
	log.Debug("Throw buf to pool, %v", buf)
	pool.wrbuf <- buf
	pool.seqmin += int64(i)
	j := 0
	for ; i < int(pool.seqsize); i++ {
		pool.seqmask[j] = pool.seqmask[i]
		j++
	}
	for ; j < int(pool.seqsize); j++ {
		pool.seqmask[j] = 0
	}
	i = 0
	for j := size; j < common.FILE_AGENT_POOL_SIZE; j++ {
		pool.buf[i] = pool.buf[j]
		i++
	}
	log.Debug("FileAgent pool mask after flush, %d, %v", pool.seqmin, pool.seqmask)
	pool.cond.Broadcast()
}

func (pool *FileAgentPool) finalflush() error {
	log.Info("FileAgent pool, final flush")
	defer close(pool.wrbuf)
	i, size := 0, 0
	for ; i < int(pool.seqsize); i++ {
		if pool.seqmask[i] == 0 {
			break
		}
		size += pool.seqmask[i]
	}
	j := i
	log.Info("FileAgent pool mask, %v", pool.seqmask)
	for ; i < int(pool.seqsize); i++ {
		if pool.seqmask[i] > 0 {
			return fmt.Errorf("Seq %d-%d missing during final flush",
				int(pool.seqmin)+j, int(pool.seqmin)+i-1)
		}
	}
	if j == 0 {
		return nil
	}
	buf := common.Clonebuf(pool.buf[:size])
	pool.wrbuf <- buf
	return nil
}

type RdBuf struct {
	rxname      string
	eof         bool
	conn        *net.TCPConn
	buf         []byte
	rdOffset    int
	wrOffset    int
	blocksize   int
	maxsize     int
	minFreeSize int
}

func (rdbuf *RdBuf) init(conn *net.TCPConn, rxname string) *RdBuf {
	packsize := common.SEQ_SIZE + common.FILE_AGENT_BLOCK_SIZE
	metasize := common.INT64_BYTES + common.CHECKSUM_BYTES
	if metasize > common.FILE_AGENT_BLOCK_SIZE {
		packsize = common.SEQ_SIZE + metasize
	}
	rdbuf.rxname = rxname
	rdbuf.conn = conn
	rdbuf.blocksize = common.FILE_AGENT_BLOCK_SIZE
	rdbuf.maxsize = packsize*2 + packsize>>2
	rdbuf.buf = make([]byte, rdbuf.maxsize)
	rdbuf.rdOffset = 0
	rdbuf.wrOffset = 0
	rdbuf.minFreeSize = packsize >> 2
	return rdbuf
}

func (rdbuf *RdBuf) readSeq() (int64, error) {
	if rdbuf.rdlen() < common.SEQ_SIZE {
		if err := rdbuf.read(common.SEQ_SIZE); err != nil {
			return -1, err
		}
	}
	size := rdbuf.rdlen()
	if size == 0 && rdbuf.eof {
		return -1, io.EOF
	} else if size < common.SEQ_SIZE {
		return -1, fmt.Errorf("Incomplete seq with size %d", size)
	}
	start, end := rdbuf.rdOffset, rdbuf.rdOffset+common.SEQ_SIZE
	seq := common.ByteToInt64(rdbuf.buf[start:end])
	rdbuf.rdOffset += common.SEQ_SIZE
	return seq, nil
}

func (rdbuf *RdBuf) rdlen() int {
	return rdbuf.wrOffset - rdbuf.rdOffset
}

func (rdbuf *RdBuf) readBlock() ([]byte, error) {
	buf, err := rdbuf.readBuf(rdbuf.blocksize)
	if err != nil {
		return nil, err
	}
	return common.Clonebuf(buf), nil
}

func (rdbuf *RdBuf) readBuf(rdsize int) ([]byte, error) {
	if rdbuf.rdlen() == 0 && rdbuf.eof {
		return nil, io.EOF
	} else if rdbuf.rdlen() < rdsize && !rdbuf.eof {
		need := rdsize - rdbuf.rdlen()
		if err := rdbuf.read(need); err != nil {
			return nil, err
		}
	}
	size := rdbuf.rdlen()
	if size == 0 && rdbuf.eof {
		return nil, io.EOF
	}
	if size > rdsize {
		size = rdsize
	}
	buf := rdbuf.buf[rdbuf.rdOffset : rdbuf.rdOffset+size]
	rdbuf.rdOffset += size
	log.Debug("rdbuf %s, read %d, %v", rdbuf.rxname, rdsize, buf)
	return buf, nil
}

func (rdbuf *RdBuf) rewind(minBytes int) {
	if rdbuf.wrOffset+rdbuf.minFreeSize < rdbuf.maxsize &&
		rdbuf.wrOffset+minBytes < rdbuf.maxsize {
		return
	}
	i := 0
	for j := rdbuf.rdOffset; j < rdbuf.wrOffset; j++ {
		rdbuf.buf[i] = rdbuf.buf[j]
		i++
	}
	rdbuf.rdOffset = 0
	rdbuf.wrOffset = i
}

func (rdbuf *RdBuf) read(minBytes int) error {
	log.Debug("rdbuf %s will read %d bytes", rdbuf.rxname, minBytes)
	rdbuf.rewind(minBytes)
	if leftSize := rdbuf.maxsize - rdbuf.wrOffset; minBytes > leftSize {
		return fmt.Errorf("Minbytes %d > leftSize %d", minBytes, leftSize)
	}
	bytes := 0
	for {
		deadline := time.Now().Add(common.FILE_AGENT_RW_TIMEOUT)
		if err := rdbuf.conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		m, err := rdbuf.conn.Read(rdbuf.buf[rdbuf.wrOffset:])
		log.Debug("FileAgent RX %s, read %d, %v, %v", rdbuf.rxname, m, err,
			rdbuf.buf[rdbuf.wrOffset:rdbuf.wrOffset+m])
		//log.Debug("FileAgent RX %s, read %d", rx.name, m)
		bytes += m
		rdbuf.wrOffset += m
		if err == io.EOF {
			rdbuf.eof = true
			return nil
		} else if err != nil {
			return err
		} else if bytes >= minBytes {
			return err
		}
	}
	return nil
}

type FileAgentRx struct {
	name      string
	cancel    context.CancelFunc
	conn      *net.TCPConn
	pool      *FileAgentPool
	rdbuf     *RdBuf
	block     *common.FileAgentBlock
	offset    int
	totalSize int
	err       chan<- error
	rate      *common.TransferRate
}

func (rx *FileAgentRx) init() {
	rx.rdbuf = new(RdBuf).init(rx.conn, rx.name)
	rx.block = &common.FileAgentBlock{
		Seq: -1,
		Buf: make([]byte, common.FILE_AGENT_BLOCK_SIZE),
	}
}

func (rx *FileAgentRx) run(ctx context.Context) {
	log.Info("FileAgent RX %s start running", rx.name)
	if err := rx.recv(ctx); err != nil {
		log.Error("FileAgent RX %s err, %v", rx.name, err)
		rx.err <- err
		rx.cancel()
	}
	rx.conn.Close()
	log.Info("FileAgent RX %s exit, total recv %d", rx.name, rx.totalSize)
}

func (rx *FileAgentRx) recv(ctx context.Context) error {
	var err error
	rdbuf := rx.rdbuf
	for {
		rx.block.Seq, err = rdbuf.readSeq()
		if err != nil {
			break
		}
		log.Debug("FileAgent RX %s recieved block %d", rx.name, rx.block.Seq)
		rx.block.Buf, err = rdbuf.readBlock()
		if err != nil {
			break
		}
		rx.putBlock()
		select {
		case <-ctx.Done():
			log.Info("FileAgent RX %s aborted", rx.name)
			rx.err <- common.ABORT_ERR
			return nil
		default:
			// do nothing
		}
	}
	if err != io.EOF {
		return err
	}
	if rx.block.Seq != -1 {
		rx.putBlock()
	}
	return nil
}

func (rx *FileAgentRx) putBlock() error {
	log.Debug("FileAgent RX %s put block %d, %v", rx.name, rx.block.Seq, rx.block.Buf)
	if err := rx.pool.put(rx.block); err != nil {
		return err
	}
	rx.totalSize += len(rx.block.Buf)
	rx.rate.Add(len(rx.block.Buf))
	return nil
}

func (rx *FileAgentRx) readMeta() (size int64, checksum string, err error) {
	var seq int64
	var buf []byte
	seq, err = rx.rdbuf.readSeq()
	if err != nil {
		return
	}
	if seq != common.SEQ_META {
		err = fmt.Errorf("Expect SEQ %d, get %d", common.SEQ_META, seq)
		return
	}
	metasize := common.INT64_BYTES + common.CHECKSUM_BYTES
	buf, err = rx.rdbuf.readBuf(metasize)
	if err != nil {
		return
	}
	if len(buf) != metasize {
		err = fmt.Errorf("Meta size %d, expect %d", len(buf), metasize)
		return
	}
	size = common.ByteToInt64(buf[:common.INT64_BYTES])
	checksum = string(buf[common.INT64_BYTES:])
	log.Info("File size %d, checksum %s", size, checksum)
	return
}

/*
func (rx *FileAgentRx) recv(ctx context.Context) error {
	var err error
	for {
		if rx.block.Seq == -1 {
			rx.block.Seq, err = rx.readSeq(0)
			if err != nil {
				break
			}
		}
		log.Debug("FileAgent RX %s recieved block %d", rx.name, rx.block.Seq)
		rx.block.Seq, err = rx.readBlock()
		if err != nil {
			break
		}
		select {
		case <-ctx.Done():
			log.Info("FileAgent RX %s aborted", rx.name)
			rx.err <- common.ABORT_ERR
			return nil
		default:
			// do nothing
		}
	}
	if err != io.EOF {
		return err
	}
	if rx.block.Seq != -1 {
		rx.putBlock()
	}
	return nil
}

func (rx *FileAgentRx) readSeq(already int) (int64, error) {
	log.Debug("FileAgent RX %s read seq, already %d", rx.name, already)
	var err error
	m := 0
	if already < common.SEQ_SIZE {
		m, err = rx.read(common.SEQ_SIZE-already, rx.rdbuf[already:])
		if err != nil {
			return -1, err
		}
	} else {
		m = already
	}
	seq := common.ByteToInt64(rx.rdbuf[:common.SEQ_SIZE])
	j := 0
	for i := common.SEQ_SIZE; i < m; i++ {
		rx.block.Buf[j] = rx.rdbuf[i]
		j++
	}
	rx.offset = j
	return seq, nil
}

func (rx *FileAgentRx) readBlock() (int64, error) {
	for {
		log.Debug("FileAgent RX %s read block %d", rx.name, rx.block.Seq)
		m, err := rx.read(0, rx.rdbuf)
		if err == io.EOF {
			return rx.block.Seq, err
		} else if err != nil {
			return -1, err
		}
		i, j := rx.offset, 0
		for i < common.FILE_AGENT_BLOCK_SIZE && j < m {
			rx.block.Buf[i] = rx.rdbuf[j]
			i++
			j++
		}
		rx.offset = i
		if i < common.FILE_AGENT_BLOCK_SIZE {
			continue
		}
		if i >= common.FILE_AGENT_BLOCK_SIZE {
			if err = rx.putBlock(); err != nil {
				return -1, err
			}
		}
		if j >= m {
			return -1, nil
		}
		already := m - j
		for i := 0; j < m; j++ {
			rx.rdbuf[i] = rx.rdbuf[j]
			i++
		}
		seq, err := rx.readSeq(already)
		return seq, err
	}
	return -1, nil
}

func (rx *FileAgentRx) read(minBytes int, buf []byte) (int, error) {
	if minBytes > len(rx.rdbuf) {
		return 0, fmt.Errorf("Minbytes %d > len(buf) %d", minBytes, len(rx.rdbuf))
	}
	bytes := 0
	for {
		deadline := time.Now().Add(common.FILE_AGENT_RW_TIMEOUT)
		if err := rx.conn.SetReadDeadline(deadline); err != nil {
			return bytes, err
		}
		m, err := rx.conn.Read(buf)
		log.Debug("FileAgent RX %s, read %d, %v, %v", rx.name, m, err, buf[:m])
		//log.Debug("FileAgent RX %s, read %d", rx.name, m)
		bytes += m
		if err == io.EOF {
			return bytes, err
		} else if err != nil {
			return bytes, err
		} else if bytes >= minBytes {
			return bytes, err
		} else {
			buf = buf[bytes:]
		}
	}
	return bytes, nil
}
*/

func initLogger(verbose bool) error {
	level := log.LevelAlloff
	if verbose {
		level = log.LevelInfo
	}
	consoleLogger := &log.ConsoleLogger{
		Level: level,
	}
	if err := log.RegisterLogger(consoleLogger); err != nil {
		return err
	}
	log.Info("=====Start client=====")
	return nil
}

func cltMain(ip string, port int, fpath string) {
	clt := new(FileAgentClt)
	err := clt.Init(ip, port, fpath)
	if err != nil {
		panic(err)
	}
	if err := clt.Run(context.Background()); err != nil {
		panic(err)
	}
}

func cltLiteMain(ip string, port int, fpath string) {
	clt := new(FileAgentCltLite)
	err := clt.Init(ip, port, fpath)
	if err != nil {
		panic(err)
	}
	if err := clt.Run(); err != nil {
		panic(err)
	}
}

func main() {
	verbose, lite := false, false
	flag.BoolVar(&verbose, "v", false, "Run with verbose info")
	flag.BoolVar(&lite, "lite", false, "Run single thread version")
	flag.Parse()
	initLogger(verbose)
	args := flag.Args()
	if len(args) < 3 {
		panic("Usage: client SERVER_IP SERVER_PORT LOACL_FILE_PATH")
	}
	ip := args[0]
	fpath := args[2]
	port64, err := strconv.ParseInt(args[1], 10, 32)
	if err != nil {
		panic(err)
	}
	port := int(port64)
	if lite {
		cltLiteMain(ip, port, fpath)
	} else {
		cltMain(ip, port, fpath)
	}
}
