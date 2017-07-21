package main

import (
	"bufio"
	"bytes"
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
		log.Printf("influxdb: error posting data: %s", err)
		return
	}
	if resp.StatusCode != 204 {
		log.Printf("influxdb: error posting data: expected status 204, got %s", resp.Status)
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

func ProxyAwareHttpClient() (*http.Client, error) {
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
	ch    chan string
}

func newResults(cols []collector) *results {
	r := &results{
		sinks: make([]chan string, len(cols)),
		ch:    make(chan string),
	}
	for i := range cols {
		ch := make(chan string)
		r.sinks[i] = ch
		go cols[i].collect(ch)
	}
	return r
}

func (r *results) collect() {
	for res := range r.ch {
		for i := range r.sinks {
			r.sinks[i] <- res
		}
	}
}

func collect(cs []collector, stdout, stderr io.Reader) {
	rs := newResults(cs)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			log.Printf("stderr: %s", sc.Text())
		}
		if err := sc.Err(); err != nil {
			log.Fatal("error reading stderr: %s", err)
		}
	}()
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			rs.ch <- sc.Text()
		}
		if err := sc.Err(); err != nil {
			log.Fatal("error reading stdout: %s", err)
		}
		close(rs.ch)
	}()
	rs.collect()
}

func execCollect(cs []collector, cmdName string, cmdArgs []string) {
	cmd := exec.Command(cmdName, cmdArgs...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatalf("cannot get stderr for command: %s", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("cannot get stdout for command: %s", err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatalf("cannot start command: %s", err)
	}
	collect(cs, stdout, stderr)
	if err := cmd.Wait(); err != nil {
		log.Fatalf("error waiting for command: %s", err)
	}
}

func main() {
	verbose := flag.Bool("verbose", false, "Print measurements to stdout")
	influxdb := flag.String("influxdb", defaultInfluxURL, "Address of InfluxDB write endpoint")
	nbatch := flag.Int("influx-nbatch", 20, "Max number of measurements to cache")
	tbatch := flag.Duration("influx-batch-time", 10*time.Second, "Max duration betweek flushes of InfluxDB cache")
	flag.Parse()

	args := flag.Args()
	cmdName := args[0]
	cmdArgs := args[1:len(args)]

	var collectors []collector
	if *influxdb != "" && *influxdb != defaultInfluxURL {
		client, err := ProxyAwareHttpClient()
		if err != nil {
			log.Fatalf("fatal: %s", err)
		}
		collectors = append(collectors, newBatchCollector(*influxdb, *nbatch, *tbatch, client))
	}
	if *verbose {
		collectors = append(collectors, printCollector{os.Stdout})
	}
	for {
		execCollect(collectors, cmdName, cmdArgs)
	}
}
