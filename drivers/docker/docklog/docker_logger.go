package docklog

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	hclog "github.com/hashicorp/go-hclog"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad/client/lib/fifo"
	"golang.org/x/net/context"
)

// DockerLogger is a small utility to forward logs from a docker container to a target
// destination
type DockerLogger interface {
	Start(*StartOpts) error
	Stop() error
}

// StartOpts are the options needed to start docker log monitoring
type StartOpts struct {
	// Endpoint sets the docker client endpoint, defaults to environment if not set
	Endpoint string

	// ContainerID of the container to monitor logs for
	ContainerID string
	TTY         bool

	// Stdout path to fifo
	Stdout string
	//Stderr path to fifo
	Stderr string

	// StartTime is the Unix time that the docker logger should fetch logs beginning
	// from
	StartTime int64

	// GracePeriod is the time in which we can attempt to collect logs from a stopped
	// container, if none have been read yet.
	GracePeriod time.Duration

	// TLS settings for docker client
	TLSCert string
	TLSKey  string
	TLSCA   string
}

// NewDockerLogger returns an implementation of the DockerLogger interface
func NewDockerLogger(logger hclog.Logger) DockerLogger {
	return &dockerLogger{
		logger: logger,
		doneCh: make(chan interface{}),
	}
}

// dockerLogger implements the DockerLogger interface
type dockerLogger struct {
	logger      hclog.Logger
	containerID string

	stdout  io.WriteCloser
	stderr  io.WriteCloser
	stdLock sync.Mutex

	// containerDoneCtx is called when the container dies, indicating that there will be no
	// more logs to be read.
	containerDoneCtx context.CancelFunc

	// read indicates whether we have read anything from the logs.  This is manipulated
	// using the sync package via multiple goroutines.
	read int64

	doneCh chan interface{}
	// readDelay is used in testing to delay reads, simulating race conditions between
	// container exits and reading
	readDelay *time.Duration
}

// Start log monitoring
func (d *dockerLogger) Start(opts *StartOpts) error {
	d.containerID = opts.ContainerID

	client, err := d.getDockerClient(opts)
	if err != nil {
		return fmt.Errorf("failed to open docker client: %v", err)
	}

	// Set up a ctx which is called when the container quits.
	containerDoneCtx, cancel := context.WithCancel(context.Background())
	d.containerDoneCtx = cancel

	// Set up a ctx which will be cancelled when we stop reading logs.  This
	// grace period allows us to collect logs from stopped containers if none
	// have yet been read.
	ctx, cancelStreams := context.WithCancel(context.Background())
	go func() {
		<-containerDoneCtx.Done()

		// Wait until we've read from the logs to exit.
		timeout := time.After(opts.GracePeriod)
		for {
			select {
			case <-time.After(time.Second):
				if d.read > 0 {
					cancelStreams()
					return
				}
			case <-timeout:
				cancelStreams()
				return
			}
		}

	}()

	go func() {
		defer close(d.doneCh)
		defer d.cleanup()

		if d.readDelay != nil {
			// Allows us to simulate reading from stopped containers in testing.
			<-time.After(*d.readDelay)
		}

		stdout, stderr, err := d.openStreams(ctx, opts)
		if err != nil {
			d.logger.Error("log streaming ended with terminal error", "error", err)
			return
		}

		sinceTime := time.Unix(opts.StartTime, 0)
		backoff := 0.0

		for {
			logOpts := docker.LogsOptions{
				Context:      ctx,
				Container:    opts.ContainerID,
				OutputStream: stdout,
				ErrorStream:  stderr,
				Since:        sinceTime.Unix(),
				Follow:       true,
				Stdout:       true,
				Stderr:       true,

				Timestamps: false,

				// When running in TTY, we must use a raw terminal.
				// If not, we set RawTerminal to false to allow docker client
				// to interpret special stdout/stderr messages
				RawTerminal: opts.TTY,
			}

			err := client.Logs(logOpts)

			// If we've been reading logs and the container is done we can safely break
			// the loop.
			//
			// NOTE: This isn't guaranteed to be closed when the docker container exits;
			// the log stream will likely have stopped following before the nomad task
			// signals to the log driver that the container has exited.
			//
			// Therefore, we're likely to loop again and read the logs once more.  If
			// the loop retries _in the same second that the container exited_, we will
			// receive duplicate logs for the last second of the container.
			if containerDoneCtx.Err() != nil && d.read > 2 {
				return
			} else if ctx.Err() != nil {
				// If context is terminated then we can safely break the loop
				return
			} else if err == nil {
				backoff = 0.0
			} else if isLoggingTerminalError(err) {
				d.logger.Error("log streaming ended with terminal error", "error", err)
				return
			} else if err != nil {
				backoff = nextBackoff(backoff)
				d.logger.Error("log streaming ended with error", "error", err, "retry_in", backoff)

				time.Sleep(time.Duration(backoff) * time.Second)
			}

			// As explained above, we may be here even if the container has exited as
			// containerDoneCtx isn't guaranteed to be instant.  This means we will loop
			// and return dupe logs for the last second of the container's life.
			sinceTime = time.Now()

			_, err = client.InspectContainerWithOptions(docker.InspectContainerOptions{
				ID: opts.ContainerID,
			})
			if err != nil {
				_, notFoundOk := err.(*docker.NoSuchContainer)
				if !notFoundOk {
					return
				}
			}
		}
	}()
	return nil

}

// openStreams open logger stdout/stderr; should be called in a background goroutine to avoid locking up
// process to avoid locking goroutine process
func (d *dockerLogger) openStreams(ctx context.Context, opts *StartOpts) (stdout, stderr io.WriteCloser, err error) {
	d.stdLock.Lock()
	stdoutF, stderrF := d.stdout, d.stderr
	d.stdLock.Unlock()

	if stdoutF != nil && stderrF != nil {
		return stdoutF, stderrF, nil
	}

	// opening a fifo may block indefinitely until a reader end opens, so
	// we preform open() without holding the stdLock, so Stop and interleave.
	// This a defensive measure - logmon (the reader end) should be up and
	// started before dockerLogger is started
	if stdoutF == nil {
		stdoutF, err = fifo.OpenWriter(opts.Stdout)
		if err != nil {
			return nil, nil, err
		}
	}

	if stderrF == nil {
		stderrF, err = fifo.OpenWriter(opts.Stderr)
		if err != nil {
			return nil, nil, err
		}
	}

	d.stdLock.Lock()
	d.stdout, d.stderr = stdoutF, stderrF
	d.stdLock.Unlock()

	return d.streamCopier(stdoutF), d.streamCopier(stderrF), nil
}

// streamCopier copies into the given writer and sets a flag indicating that
// we have read some logs.
func (d *dockerLogger) streamCopier(to io.WriteCloser) io.WriteCloser {
	return &copier{read: &d.read, writer: to, logger: d.logger}
}

type copier struct {
	logger hclog.Logger
	read   *int64
	writer io.WriteCloser

	lastRead []byte
}

func (c *copier) Write(p []byte) (n int, err error) {
	// XXX: There are race conditions between nomad and reading docker outputs:
	//
	// When a container exits we may not have read all of the logs.  How do we know
	// when we've read all of the logs?
	//
	// Ideally we'd use an incremental JSON parser, assuming that all output to stdoud
	// must be in JSON format.  An incremental parser would return whether the currently
	// read JSON is valid;  if it's valid we can assume that we've read everything.
	//
	// It would be great to create a sream parser which discards all data except for
	// the internal JSON-decoding state, eg. which position / data structure we're in,
	// and then we could capture whether the state is valid.
	if len(c.lastRead) > 0 {
		// We may be reading duplicate logs as per the comment on line 156;
		// the client may request logs from a killed container twice in the same
		// second.
		//
		// This does not fix everythign:  Write() is called for each log line
		// within the same second.  This may mean that > 1 log line is repeated,
		// therefore lastRead will never prevet dupes for > 1 repeated log.
		//
		// To fix, for our use case we can:
		//
		// 1. Discard log streaming and copy the entire log file from the docker's
		//    log output to the writer once the container has closed
		// 2. Request timestamps, and store all timestamps we've seen in the last second
		//    in a buffer
		if len(c.lastRead) == len(p) && bytes.Equal(c.lastRead, p) {
			return len(p), nil
		}
	}

	atomic.AddInt64(c.read, int64(len(p)))
	c.lastRead = make([]byte, len(p))
	copy(c.lastRead, p)

	return c.writer.Write(p)
}

func (c *copier) Close() error {
	return c.writer.Close()
}

// Stop log monitoring
func (d *dockerLogger) Stop() error {
	if d.containerDoneCtx != nil {
		d.containerDoneCtx()
	}
	return nil
}

func (d *dockerLogger) cleanup() error {
	d.logger.Debug("cleaning up", "container_id", d.containerID)

	d.stdLock.Lock()
	stdout, stderr := d.stdout, d.stderr
	d.stdLock.Unlock()

	if stdout != nil {
		stdout.Close()
	}
	if stderr != nil {
		stderr.Close()
	}
	return nil
}

func (d *dockerLogger) getDockerClient(opts *StartOpts) (*docker.Client, error) {
	var err error
	var merr multierror.Error
	var newClient *docker.Client

	// Default to using whatever is configured in docker.endpoint. If this is
	// not specified we'll fall back on NewClientFromEnv which reads config from
	// the DOCKER_* environment variables DOCKER_HOST, DOCKER_TLS_VERIFY, and
	// DOCKER_CERT_PATH. This allows us to lock down the config in production
	// but also accept the standard ENV configs for dev and test.
	if opts.Endpoint != "" {
		if opts.TLSCert+opts.TLSKey+opts.TLSCA != "" {
			d.logger.Debug("using TLS client connection to docker", "endpoint", opts.Endpoint)
			newClient, err = docker.NewTLSClient(opts.Endpoint, opts.TLSCert, opts.TLSKey, opts.TLSCA)
			if err != nil {
				merr.Errors = append(merr.Errors, err)
			}
		} else {
			d.logger.Debug("using plaintext client connection to docker", "endpoint", opts.Endpoint)
			newClient, err = docker.NewClient(opts.Endpoint)
			if err != nil {
				merr.Errors = append(merr.Errors, err)
			}
		}
	} else {
		d.logger.Debug("using client connection initialized from environment")
		newClient, err = docker.NewClientFromEnv()
		if err != nil {
			merr.Errors = append(merr.Errors, err)
		}
	}

	return newClient, merr.ErrorOrNil()
}

func isLoggingTerminalError(err error) bool {
	if err == nil {
		return false
	}

	if apiErr, ok := err.(*docker.Error); ok {
		switch apiErr.Status {
		case 501:
			return true
		}
	}

	terminals := []string{
		"configured logging driver does not support reading",
	}

	for _, c := range terminals {
		if strings.Contains(err.Error(), c) {
			return true
		}
	}

	return false
}

// nextBackoff returns the next backoff period in seconds given current backoff
func nextBackoff(backoff float64) float64 {
	if backoff < 0.5 {
		backoff = 0.5
	}

	backoff = backoff * 1.15 * (1.0 + rand.Float64())
	if backoff > 120 {
		backoff = 120
	} else if backoff < 0.5 {
		backoff = 0.5
	}

	return backoff
}
