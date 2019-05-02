package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"time"
)

const (
	defaultInfluxURL = "http://USER:PASS@HOST:8086/write?db=MY_DB"
)

var (
	elog *log.Logger
	dlog *log.Logger
	flog *log.Logger
)

type submitter struct {
	ch       chan io.Reader
	endpoint string
	debug    bool
	client   *http.Client
}

func newSubmitter(nworkers, nbuf int, endpoint string, client *http.Client, debug bool) *submitter {
	s := &submitter{
		ch:       make(chan io.Reader, nbuf),
		client:   client,
		endpoint: endpoint,
		debug:    debug,
	}
	for i := 0; i < nworkers; i++ {
		go s.run()
	}
	return s
}

func (s *submitter) run() {
	for r := range s.ch {
		if err := s.send(r); err != nil {
			elog.Printf("could not submit batch: %v", err)
		}
	}
}

func (s *submitter) submit(r io.Reader) {
	s.ch <- r
}

func (s *submitter) send(r io.Reader) error {
	var debugBuf []byte
	req, err := http.NewRequest("POST", s.endpoint, r)
	if err != nil {
		return fmt.Errorf("cannot create request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	if s.debug {
		debugBuf, err = httputil.DumpRequest(req, true)
		if err != nil {
			elog.Printf("could not dump POST request for debugging: %v", err)
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("cannot POST data: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if s.debug {
			dlog.Printf("failed POST request:\n\n%s\n", debugBuf)
			debugBuf, err = httputil.DumpResponse(resp, true)
			if err != nil {
				elog.Printf("could not dump influx reponse for debugging: %v", err)
			} else {
				dlog.Printf("failed POST reponse:\n\n%s\n\n", debugBuf)
			}
		}
		return fmt.Errorf("expected status 2xx, got %s", resp.Status)
	}
	if _, err := io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("cannot read and discard data: %v", err)
	}
	return nil
}

type collector interface {
	collect(<-chan string)
}

type batchCollector struct {
	nbatch    int
	batchi    int // current position in batch slice
	tbatch    time.Duration
	submitter *submitter
	batch     []string
}

func newBatchCollector(nbatch int, tbatch time.Duration, sub *submitter) *batchCollector {
	return &batchCollector{
		nbatch:    nbatch,
		tbatch:    tbatch,
		submitter: sub,
		batch:     make([]string, nbatch),
	}
}

func (b *batchCollector) collect(ch <-chan string) {
	var skipTick bool // avoid flushing because of full and then timeout
	tick := time.Tick(b.tbatch)
	for {
		select {
		case res := <-ch:
			if b.batchi >= b.nbatch {
				b.flush()
				skipTick = true
			}
			b.batch[b.batchi] = res
			b.batchi++
		case <-tick:
			if skipTick {
				skipTick = false
				continue
			}
			if b.batchi == 0 {
				continue
			}
			b.flush()
		}
	}
}

func (b *batchCollector) flush() {
	var buf bytes.Buffer
	if err := b.writeTo(&buf); err != nil {
		elog.Printf("flushing data: cannot write to buffer: %v", err)
		return
	}
	b.submitter.submit(&buf)
}

func (b *batchCollector) writeTo(w io.Writer) error {
	for i := 0; i < b.batchi; i++ {
		if _, err := fmt.Fprintln(w, b.batch[i]); err != nil {
			return fmt.Errorf("cannot write batch line: %v", err)
		}
		b.batch[i] = ""
	}
	b.batchi = 0
	return nil
}

type printCollector struct {
	w io.Writer
}

func (p printCollector) collect(ch <-chan string) {
	for r := range ch {
		fmt.Fprintln(p.w, r)
	}
}

type results struct {
	sinks []chan string
}

func newResults(cols []collector) (*results, error) {
	if len(cols) == 0 {
		return nil, errors.New("no collectors specified")
	}
	r := &results{
		sinks: make([]chan string, len(cols)),
	}
	for i := range cols {
		ch := make(chan string)
		r.sinks[i] = ch
		go cols[i].collect(ch)
	}
	return r, nil
}

func (r *results) collect(ch <-chan string) {
	for res := range ch {
		for i := range r.sinks {
			r.sinks[i] <- res
		}
	}
}

func drainPipes(rs *results, stdout, stderr io.Reader) {
	ch := make(chan string)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			fmt.Println(sc.Text())
		}
		if err := sc.Err(); err != nil {
			elog.Printf("reading stderr: %v", err)
		}
	}()
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			ch <- sc.Text()
		}
		if err := sc.Err(); err != nil {
			flog.Fatalf("fatal: reading stdout: %v", err)
		}
		close(ch)
	}()
	rs.collect(ch)
}

type cmd struct {
	name string
	args []string
}

func (c *cmd) execCollect(rs *results) error {
	cmd := exec.Command(c.name, c.args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("cannot get stderr for command: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("fatal: cannot get stdout for command: %v", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("fatal: cannot start command: %v", err)
	}
	drainPipes(rs, stdout, stderr)
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("child exited with failure code, aborting (%v)", err)
		}
		elog.Printf("error waiting for command: %v", err)
	}
	return nil
}

type cmds []cmd

func cmdsFromArgs(args []string) cmds {
	cmds := cmds(make([]cmd, 0))
	c := cmd{}
	for i := range args {
		if c.name == "" {
			c.name = args[i]
			continue
		}
		if args[i] == ";" {
			cmds = append(cmds, c)
			c = cmd{}
			continue
		}
		c.args = append(c.args, args[i])
	}
	if c.name != "" {
		cmds = append(cmds, c)
	}
	return cmds
}

func (c cmds) run(rs *results) {
	runOne := func(c *cmd) {
		for {
			if err := c.execCollect(rs); err != nil {
				flog.Fatalf("executing subprocess: %v", err)
			}
		}
	}
	for i := range c {
		go runOne(&c[i])
	}
	select {}
}

func configure() (cmds, *results, error) {
	verbose := flag.Bool("verbose", false, "Print measurements to stdout")
	debug := flag.Bool("debug", false, "Print failed requests to stdout")
	influxdb := flag.String("endpoint", defaultInfluxURL, "Address of InfluxDB write endpoint")
	nbatch := flag.Int("nbatch", 100, "Max number of measurements to cache")
	tbatch := flag.Duration("batch-time", 1*time.Minute, "Max duration betweek flushes of InfluxDB cache")
	nworkers := 1 // number of HTTP submitting workers
	nbuf := 0     // buffer for workers channel
	flag.Parse()

	cmds := cmdsFromArgs(flag.Args())
	if len(cmds) == 0 {
		return nil, nil, errors.New("specify one or more commands to execute, separated by semicolon")
	}
	client := &http.Client{}
	submitter := newSubmitter(nworkers, nbuf, *influxdb, client, *debug)
	var cs []collector
	if *influxdb != "" && *influxdb != defaultInfluxURL {
		cs = append(cs, newBatchCollector(*nbatch, *tbatch, submitter))
	}
	if *verbose {
		cs = append(cs, printCollector{os.Stdout})
	}
	rs, err := newResults(cs)
	if err != nil {
		return nil, nil, fmt.Errorf("%v: use either -endpoint or -verbose", err)
	}
	return cmdsFromArgs(flag.Args()), rs, nil
}

func main() {
	elog = log.New(os.Stderr, "error - ", log.LstdFlags)
	dlog = log.New(os.Stdout, "debug - ", log.LstdFlags)
	flog = log.New(os.Stderr, "fatal - ", log.LstdFlags)
	cmds, rs, err := configure()
	if err != nil {
		flog.Fatalf("configuration error: %v", err)
	}
	cmds.run(rs)
}
