package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	defaultInfluxURL  = "http://USER:PASS@HOST:8086/write?db=MY_DB"
	templateInfluxURL = "http://localhost:8086/write?db=test"
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

func drainPipes(rs *results, prefix string, stdout, stderr io.Reader) {
	ch := make(chan string)
	send := func(line string) {
		ch <- line
	}
	if prefix != "" {
		send = func(line string) {
			if strings.HasPrefix(line, prefix) {
				ch <- strings.TrimSpace(line[len(prefix):])
				return
			}
			fmt.Println(line)
		}
	}
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			fmt.Fprintf(os.Stderr, "%s\n", sc.Text())
		}
		if err := sc.Err(); err != nil {
			elog.Printf("reading stderr: %v", err)
		}
	}()
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			send(sc.Text())
		}
		if err := sc.Err(); err != nil {
			elog.Printf("fatal: reading stdout: %v", err)
		}
		close(ch)
	}()
	rs.collect(ch)
}

type cmd struct {
	name   string
	prefix string
	args   []string
}

func (c *cmd) execCollect(rs *results, id int) error {
	dlog.Printf("executing #%d: %s %v", id, c.name, c.args)
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
	drainPipes(rs, c.prefix, stdout, stderr)
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("child exited with failure code, aborting (%v)", err)
		}
		elog.Printf("error waiting for command: %v", err)
	}
	return nil
}

type cmds []cmd

func cmdsFromArgs(mkcmd func() cmd, nosplit bool, args []string) cmds {
	cmds := cmds(make([]cmd, 0))
	c := mkcmd()
	for i := range args {
		if c.name == "" {
			c.name = args[i]
			continue
		}
		if args[i] == ";" {
			if !nosplit {
				cmds = append(cmds, c)
				c = mkcmd()
				continue
			}
		}
		c.args = append(c.args, args[i])
	}
	if c.name != "" {
		cmds = append(cmds, c)
	}
	return cmds
}

func (c cmds) run(rs *results, fatal bool) {
	runOne := func(c *cmd, id int) {
		for {
			if err := c.execCollect(rs, id); err != nil {
				elog.Printf("executing subprocess #%d: %v", id, err)
				if fatal {
					elog.Fatalf("terminating all on subprocess failure")
				}
			}
		}
	}
	if len(c) == 1 {
		runOne(&c[0], 0)
		return
	}
	for i := range c {
		go runOne(&c[i], i)
	}
	select {}
}

func influxEndpoint(rawurl, user, pass, host, dbname string, ssl bool) (string, error) {
	if rawurl == "" {
		rawurl = templateInfluxURL
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		return "", fmt.Errorf("cannot parse influx endpoint URL: %v", err)
	}
	if ssl {
		u.Scheme = "https"
	}
	if host != "" {
		u.Host = host
	}
	if dbname != "" {
		q := u.Query()
		q.Set("db", dbname)
		u.RawQuery = q.Encode()
	}
	if user != "" {
		if pass != "" {
			u.User = url.UserPassword(user, pass)
		} else {
			u.User = url.User(user)
		}
	}
	return u.String(), nil
}

func makeHttpClient(insecure bool) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
	}
}

func prefixEnv(prefix string, getenv func(string) string) func(*flag.Flag) {
	prefix = prefix + "_"
	return func(f *flag.Flag) {
		key := prefix + strings.Replace(strings.ToUpper(f.Name), "-", "_", -1)
		val := getenv(key)
		if val == "" {
			return
		}
		if err := f.Value.Set(val); err != nil {
			elog.Fatalf("cannot set flag from environment variable %s: %v", key, err)
		}
	}
}

func start() error {
	verbose := flag.Bool("verbose", false, "Print measurements to stdout")
	debug := flag.Bool("debug", false, "Print failed requests to stdout")
	insecure := flag.Bool("insecure", false, "Ignore TLS validation")
	nosplit := flag.Bool("nosplit", false, "Do not split the commands by semicolon")
	ssl := flag.Bool("ssl", false, "Use TLS/SSL to connect to endpoint")
	influxdb := flag.String("endpoint", defaultInfluxURL, "Address of InfluxDB write endpoint; if not specified defaults to verbose mode")
	user := flag.String("user", "", "Username for authentication")
	pass := flag.String("password", "", "Password for authentication")
	host := flag.String("host", "", "Hostname of InfluxDB (overrides endpoint)")
	dbname := flag.String("dbname", "", "Database name of InfluxDB (overrides endpoint)")
	prefix := flag.String("prefix", "", "Only parse lines with this prefix, write back everything else")
	nbatch := flag.Int("nbatch", 100, "Max number of measurements to cache")
	tbatch := flag.Duration("batch-time", 1*time.Minute, "Max duration betweek flushes of InfluxDB cache")
	fatal := flag.Bool("fatal", false, "Subprocess errors are fatal errors")

	flag.VisitAll(prefixEnv("INFLUXIN", os.Getenv))
	flag.Parse()

	nworkers := 1 // number of HTTP submitting workers
	nbuf := 0     // buffer for workers channel

	dlog = log.New(ioutil.Discard, "", 0)
	if *debug {
		dlog = log.New(os.Stdout, "debug - ", log.LstdFlags)
	}

	var (
		endpoint string
		err      error
	)
	if *influxdb != defaultInfluxURL {
		endpoint, err = influxEndpoint(*influxdb, *user, *pass, *host, *dbname, *ssl)
		if err != nil {
			return fmt.Errorf("invalid influx endpoint configuration: %v", err)
		}
	} else {
		// without an endpoint, default to verbose
		*verbose = true
	}

	mkcmd := func() cmd {
		return cmd{prefix: *prefix}
	}
	cmds := cmdsFromArgs(mkcmd, *nosplit, flag.Args())
	if len(cmds) == 0 {
		return errors.New("specify one or more commands to execute, separated by semicolon")
	}
	var cs []collector
	if endpoint != "" {
		client := makeHttpClient(*insecure)
		submitter := newSubmitter(nworkers, nbuf, endpoint, client, *debug)
		cs = append(cs, newBatchCollector(*nbatch, *tbatch, submitter))
	}
	if *verbose {
		cs = append(cs, printCollector{os.Stdout})
	}
	rs, err := newResults(cs)
	if err != nil {
		return fmt.Errorf("%v: use either -endpoint or -verbose", err)
	}
	cmds.run(rs, *fatal)
	return nil
}

func main() {
	elog = log.New(os.Stderr, "error - ", log.LstdFlags)
	flog = log.New(os.Stderr, "fatal - ", log.LstdFlags)
	if err := start(); err != nil {
		flog.Fatalf("configuration error: %v", err)
	}
}
