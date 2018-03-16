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

type FileAgentSvrLite struct {
	ip       string
	port     int
	fpath    string
	fsize    int64
	fp       *os.File
	listener *net.TCPListener
	txrate   *common.TransferRate
	progress *common.OnelineProgress
	startTs  time.Time
}

func (svr *FileAgentSvrLite) Init(ip string, port int, fpath string) error {
	fp, err := os.Open(fpath)
	if err != nil {
		return err
	}
	finfo, err := fp.Stat()
	if err != nil {
		return err
	}
	if !finfo.Mode().IsRegular() {
		return fmt.Errorf("%s not a regular file", fpath)
	}
	svr.fsize = finfo.Size()
	addr := &net.TCPAddr{
		IP:   net.ParseIP(ip),
		Port: port,
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return fmt.Errorf("Fail to listen on %s, %v", ip, err)
	}
	svr.ip = ip
	svr.port = port
	svr.listener = listener
	svr.fpath = fpath
	svr.fp = fp
	svr.txrate = &common.TransferRate{Holder: "TX", Total: svr.fsize}
	svr.progress = new(common.OnelineProgress).Init()
	return nil
}

func (svr *FileAgentSvrLite) Run() error {
	log.Info("FileAgent server-lite start running")
	t1 := time.Now()
	svr.startTs = t1
	err := svr.send()
	t2 := time.Now()
	if err == nil {
		fmt.Printf("FileAgent Lite finished transfer in %v\n", t2.Sub(t1))
	}
	log.Info("FileAgent server closed, err %v", err)
	return err
}

func (svr *FileAgentSvrLite) send() error {
	go svr.monitor()
	defer svr.progress.Finish()
	defer svr.fp.Close()
	deadline := time.Now().Add(10 * time.Second)
	err := svr.listener.SetDeadline(deadline)
	if err != nil {
		return fmt.Errorf("FileAgent server lite, fail to set deadline, %v", err)
	}
	conn, err := svr.listener.AcceptTCP()
	if err != nil {
		return fmt.Errorf("FileAgent server lite, fail to accept, %v", err)
	}
	svr.listener.Close()
	defer conn.Close()
	chunk := make([]byte, common.CHUNK_SIZE)
	for {
		m, err := svr.fp.Read(chunk)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("FileAgent server lite, faile to read, %v", err)
		}
		n, err := conn.Write(chunk[:m])
		if err != nil {
			return fmt.Errorf("FileAgent server lite, faile to write, %v", err)
		}
		if n != m {
			return fmt.Errorf("FileAgent server lite, should write %d, get %d", m, n)
		}
		svr.txrate.Add(m)
	}
	return nil
}

func (svr *FileAgentSvrLite) monitor() {
	update := func() {
		tx := svr.txrate.String()
		duration := common.PrettyDuration(time.Now().Sub(svr.startTs))
		svr.progress.Update("%s, %s", tx, duration)
	}
	abort := false
	ticker := time.NewTicker(common.FILE_AGENT_PROGRESS_INTERVAL)
	for !abort {
		select {
		case <-ticker.C:
			update()
		case <-svr.progress.Exit():
			abort = true
		}
	}
	update()
	fmt.Println()
	svr.progress.NotifyDone()
	log.Info("FileAgent server monitor exit")
}

func copybuf(src, dst []byte) {
	for i := 0; i < len(src); i++ {
		dst[i] = src[i]
	}
}

type FileAgentSvr struct {
	ip       string
	port     int
	connCnt  int
	fpath    string
	fsize    int64
	checksum string
	fp       *os.File
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	listener *net.TCPListener
	txCnt    int
	blocks   chan *common.FileAgentBlock
	err      chan error
	rdrate   *common.TransferRate
	txrate   *common.TransferRate
	progress *common.OnelineProgress
	startTs  time.Time
}

func (svr *FileAgentSvr) Init(ip string, port int, fpath string) error {
	fp, err := os.Open(fpath)
	if err != nil {
		return err
	}
	finfo, err := fp.Stat()
	if err != nil {
		return err
	}
	if !finfo.Mode().IsRegular() {
		return fmt.Errorf("%s not a regular file", fpath)
	}
	svr.fsize = finfo.Size()
	addr := &net.TCPAddr{
		IP:   net.ParseIP(ip),
		Port: port,
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return fmt.Errorf("Fail to listen on %s, %v", ip, err)
	}
	svr.ip = ip
	svr.port = port
	svr.listener = listener
	svr.fpath = fpath
	svr.fp = fp
	svr.blocks = make(chan *common.FileAgentBlock, common.FILE_AGENT_THREADS)
	svr.err = make(chan error, common.FILE_AGENT_THREADS+1) // +1 for read thread
	svr.rdrate = &common.TransferRate{Holder: "RD", Total: svr.fsize}
	svr.txrate = &common.TransferRate{Holder: "TX", Total: svr.fsize}
	svr.progress = new(common.OnelineProgress).Init()
	return nil
}

func (svr *FileAgentSvr) Run(ctx context.Context) error {
	var err error
	log.Info("FileAgent server start running")
	svr.checksum, err = common.CRC64OfFile(svr.fpath)
	if err != nil {
		return err
	}
	t1 := time.Now()
	svr.startTs = t1
	err = svr.send(ctx)
	t2 := time.Now()
	if err == nil {
		fmt.Printf("FileAgent finished transfer in %v\n", t2.Sub(t1))
	}
	log.Info("FileAgent server closed, err %v", err)
	return err
}

func (svr *FileAgentSvr) send(ctx context.Context) error {
	var err error
	ctx, svr.cancel = context.WithCancel(ctx)
	defer svr.cancel()
	go svr.read(ctx)
	go svr.monitor()
	defer svr.listener.Close()
	abort := false
	endts := time.Now().Add(common.FILE_AGENT_SVR_WAIT_TIMEOUT)
	for !abort {
		deadline := time.Now().Add(common.FILE_AGENT_SVR_ACCEPT_TIMEOUT)
		err = svr.listener.SetDeadline(deadline)
		if err != nil {
			break
		}
		log.Info("FileAgent server wait for connection")
		conn, err := svr.listener.AcceptTCP()
		if err == nil {
			svr.newConn(ctx, conn)
			svr.connCnt++
		}
		if svr.connCnt == common.FILE_AGENT_THREADS {
			break
		}
		if time.Now().After(endts) {
			break
		}
		select {
		case <-ctx.Done():
			abort = true
		default:
			// do nothing
		}
	}
	log.Info("FileAgent server wait for send done...")
	svr.wg.Wait()
	svr.fp.Close()
	svr.progress.Finish()
	err = svr.geterr()
	return err
}

func (svr *FileAgentSvr) geterr() error {
	if len(svr.err) > 0 {
		return <-svr.err
	} else {
		return nil
	}
}

func (svr *FileAgentSvr) monitor() {
	update := func() {
		rd, tx := svr.rdrate.String(), svr.txrate.String()
		duration := common.PrettyDuration(time.Now().Sub(svr.startTs))
		eta := svr.txrate.ETA()
		svr.progress.Update("%s, %s, %s, %s", rd, tx, duration, eta)
	}
	abort := false
	ticker := time.NewTicker(common.FILE_AGENT_PROGRESS_INTERVAL)
	for !abort {
		select {
		case <-ticker.C:
			update()
		case <-svr.progress.Exit():
			abort = true
		}
	}
	update()
	fmt.Println()
	svr.progress.NotifyDone()
	log.Info("FileAgent server monitor exit")
}

func (svr *FileAgentSvr) read(ctx context.Context) {
	log.Info("FileAgent server, start reading...")
	chunk := make([]byte, common.FILE_AGENT_CHUNK_SIZE)
	var seq int64
	abort := false
	svr.readMeta()
	for !abort {
		n, err := svr.fp.Read(chunk)
		if err == io.EOF {
			break
		} else if err != nil {
			svr.err <- err
			break
		}
		log.Debug("FileAgent server read %d bytes", n)
		svr.rdrate.Add(n)
		blocks := svr.splitChunk(seq, chunk, n)
		seq += int64(len(blocks))
		for _, block := range blocks {
			log.Debug("FileAgent svr, insert block %d", block.Seq)
			select {
			case svr.blocks <- block:
				//do nothing
			case <-ctx.Done():
				abort = true
				svr.err <- common.ABORT_ERR
			}
			if abort {
				log.Info("FileAgent server abort reading")
				break
			}
		}
	}
	close(svr.blocks)
	log.Info("FileAgent server, reading finished")
}

func (svr *FileAgentSvr) readMeta() {
	size := common.SEQ_SIZE + common.INT64_BYTES + common.CHECKSUM_BYTES
	block := &common.FileAgentBlock{
		Seq: common.SEQ_META,
		Buf: make([]byte, size),
	}
	i := 0
	seqbuf := common.Int64ToByte(block.Seq)
	copybuf(seqbuf, block.Buf[i:])
	i += common.SEQ_SIZE
	sizebuf := common.Int64ToByte(svr.fsize)
	copybuf(sizebuf, block.Buf[i:])
	i += common.INT64_BYTES
	copybuf([]byte(svr.checksum), block.Buf[i:])
	svr.blocks <- block
}

func (svr *FileAgentSvr) splitChunk(seq int64, chunk []byte, n int) []*common.FileAgentBlock {
	blocks := make([]*common.FileAgentBlock, 0)
	for i, j := 0, 0; i < n; i += common.FILE_AGENT_BLOCK_SIZE {
		size := common.FILE_AGENT_BLOCK_SIZE
		if i+size >= n {
			size = n - i
		}
		block := &common.FileAgentBlock{
			Seq: seq + int64(j),
			Buf: make([]byte, common.SEQ_SIZE+size),
		}
		seqbuf := common.Int64ToByte(block.Seq)
		copybuf(seqbuf, block.Buf)
		copybuf(chunk[i:i+size], block.Buf[common.SEQ_SIZE:])
		log.Debug("block buf %d, %c", len(block.Buf), block.Buf)
		blocks = append(blocks, block)
		j++
	}
	return blocks
}

func (svr *FileAgentSvr) newConn(ctx context.Context, conn *net.TCPConn) {
	log.Info("FileAgent server, new connection from %v", conn.RemoteAddr())
	tx := &FileAgentTx{
		name:   fmt.Sprintf("%v", conn.RemoteAddr()),
		conn:   conn,
		blocks: svr.blocks,
		cancel: svr.cancel,
		err:    svr.err,
		rate:   svr.txrate,
	}
	svr.wg.Add(1)
	go func() {
		defer svr.wg.Done()
		tx.run(ctx)
	}()
}

type FileAgentTx struct {
	name      string
	conn      *net.TCPConn
	blocks    <-chan *common.FileAgentBlock
	cancel    context.CancelFunc
	totalSize int
	err       chan<- error
	rate      *common.TransferRate
}

func (tx *FileAgentTx) run(ctx context.Context) {
	log.Info("FileAgent TX %s start running", tx.name)
	err := tx.sendBlocks(ctx)
	if err != nil {
		log.Info("FileAgent TX %s err, %v", err)
		tx.cancel()
	}
	tx.conn.Close()
	log.Info("FileAgent TX %s exit, total send %d", tx.name, tx.totalSize)
}

func (tx *FileAgentTx) sendBlocks(ctx context.Context) error {
	for block := range tx.blocks {
		log.Debug("FileAgent TX send out block %d to %s", block.Seq, tx.name)
		if err := tx.send(block); err != nil {
			tx.err <- err
			return err
		}
		select {
		case <-ctx.Done():
			tx.err <- common.ABORT_ERR
			log.Info("FileAgent TX aborted")
			return nil
		default:
			// do nothing
		}
	}
	return nil
}

func (tx *FileAgentTx) send(block *common.FileAgentBlock) error {
	deadline := time.Now().Add(common.FILE_AGENT_RW_TIMEOUT)
	if err := tx.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	m, err := tx.conn.Write(block.Buf)
	if err != nil {
		return err
	}
	if m != len(block.Buf) {
		return fmt.Errorf("Write %d, expect %d", m, len(block.Buf))
	}
	tx.totalSize += m
	if block.Seq >= 0 {
		tx.rate.Add(m - common.SEQ_SIZE)
	}
	return nil
}

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
	log.Info("=====Start server=====")
	return nil
}

func svrMain(ip string, port int, fpath string) {
	svr := new(FileAgentSvr)
	err := svr.Init(ip, port, fpath)
	if err != nil {
		panic(err)
	}
	if err := svr.Run(context.Background()); err != nil {
		panic(err)
	}
}

func svrLiteMain(ip string, port int, fpath string) {
	svr := new(FileAgentSvrLite)
	err := svr.Init(ip, port, fpath)
	if err != nil {
		panic(err)
	}
	if err := svr.Run(); err != nil {
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
		panic("Usage: server SERVER_IP SERVER_PORT LOACL_FILE_PATH")
	}
	ip := args[0]
	fpath := args[2]
	port64, err := strconv.ParseInt(args[1], 10, 32)
	if err != nil {
		panic(err)
	}
	port := int(port64)
	if lite {
		svrLiteMain(ip, port, fpath)
	} else {
		svrMain(ip, port, fpath)
	}
}
