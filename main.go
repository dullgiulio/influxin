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
	"net/url"
	"os"
	"os/exec"
	"time"
)

const (
	defaultInfluxURL = "http://HOST:8086/write?db=MY_DB"
)

type collector interface {
	collect(<-chan string)
}

type batchCollector struct {
	endpoint string
	nbatch   int
	batchi   int // current position in batch slice
	tbatch   time.Duration
	batch    []string
	client   *http.Client
}

func newBatchCollector(endp string, nbatch int, tbatch time.Duration, client *http.Client) *batchCollector {
	return &batchCollector{
		endpoint: endp,
		nbatch:   nbatch,
		tbatch:   tbatch,
		client:   client,
		batch:    make([]string, nbatch),
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
			b.flush()
		}
	}
}

func (b *batchCollector) flush() {
	var buf bytes.Buffer
	for i := 0; i < b.batchi; i++ {
		fmt.Fprintln(&buf, b.batch[i])
		b.batch[i] = ""
	}
	b.batchi = 0
	resp, err := b.client.Post(b.endpoint, "text/plain", &buf)
	if err != nil {
		log.Printf("influxin: error posting data: %s", err)
		return
	}
	if resp.StatusCode != 204 {
		log.Printf("influxin: error posting data: expected status 204, got %s", resp.Status)
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

func proxyAwareHTTPClient() (*http.Client, error) {
	proxyurl, ok := os.LookupEnv("HTTP_PROXY")
	if !ok {
		return http.DefaultClient, nil
	}
	purl, err := url.Parse(proxyurl)
	if err != nil {
		return nil, fmt.Errorf("invalid URL in environment variable HTTP_PROXY: %s", err)
	}
	tr := &http.Transport{
		Proxy: http.ProxyURL(purl),
	}
	return &http.Client{Transport: tr}, nil
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
			log.Printf("influxin: error reading stderr: %v", err)
		}
	}()
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			ch <- sc.Text()
		}
		if err := sc.Err(); err != nil {
			log.Fatalf("influxin: error reading stdout: %v", err)
		}
		close(ch)
	}()
	rs.collect(ch)
}

type cmd struct {
	name string
	args []string
}

func (c *cmd) execCollect(rs *results) {
	cmd := exec.Command(c.name, c.args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatalf("influxin: cannot get stderr for command: %s", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("influxin: cannot get stdout for command: %s", err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatalf("influxin: cannot start command: %s", err)
	}
	drainPipes(rs, stdout, stderr)
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			log.Fatalf("influxin: child exited with failure code, aborting (%s)", err)
		}
		log.Printf("influxin: error waiting for command: %s", err)
	}
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
	for i := range c {
		go func(c *cmd) {
			for {
				c.execCollect(rs)
			}
		}(&c[i])
	}
	select {}
}

func configure() (cmds, *results, error) {
	verbose := flag.Bool("verbose", false, "Print measurements to stdout")
	influxdb := flag.String("influxdb", defaultInfluxURL, "Address of InfluxDB write endpoint")
	nbatch := flag.Int("influx-nbatch", 100, "Max number of measurements to cache")
	tbatch := flag.Duration("influx-batch-time", 1*time.Minute, "Max duration betweek flushes of InfluxDB cache")
	flag.Parse()

	cmds := cmdsFromArgs(flag.Args())
	if len(cmds) == 0 {
		return nil, nil, errors.New("specify one or more commands to execute, separated by semicolon")
	}
	var cs []collector
	if *influxdb != "" && *influxdb != defaultInfluxURL {
		client, err := proxyAwareHTTPClient()
		if err != nil {
			return nil, nil, err
		}
		cs = append(cs, newBatchCollector(*influxdb, *nbatch, *tbatch, client))
	}
	if *verbose {
		cs = append(cs, printCollector{os.Stdout})
	}
	rs, err := newResults(cs)
	if err != nil {
		return nil, nil, fmt.Errorf("%v: use either -influxdb or -verbose", err)
	}
	return cmdsFromArgs(flag.Args()), rs, nil
}

func main() {
	cmds, rs, err := configure()
	if err != nil {
		log.Fatalf("influxin: fatal: %v", err)
	}
	cmds.run(rs)
}
