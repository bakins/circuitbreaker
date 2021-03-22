package circuitbreaker

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asecurityteam/rolling"
)

var (
	// ErrTooManyRequests is returned when the Breaker state is StateHalfOpen and the requests count is over the  MaxRequests
	ErrTooManyRequests = errors.New("too many requests")
	// ErrOpenState is returned when the Breaker state is StateOpen
	ErrOpenState = errors.New("circuit breaker is open")
)

// State of a Breaker.
type State int

// Breaker states
const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

// String returns a string representation of the Breaker state
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

// ReadyToTrip is called whenever a request fails in the closed state.
type ReadyToTrip func(Counts) bool

// OnStateChange is called whenever the state of the Breaker changes.
type OnStateChange func(from State, to State)

// Options configure the Breaker.
type Options struct {
	readyToTrip   ReadyToTrip
	onStateChange OnStateChange
	window        time.Duration
	timeout       time.Duration
	maxRequests   uint64
}

// Option sets Breaker options
type Option func(*Options)

// WithMaxRequests sets maximum number of requests allowed to pass through
// when the Breaker is half-open.
// Default is 1
func WithMaxRequests(max uint64) Option {
	return func(o *Options) {
		o.maxRequests = max
	}
}

// WithWindow sets the rolling time window for counting successes and failures.
// Default is 60 seconds. Must be at least one second.
func WithWindow(window time.Duration) Option {
	return func(o *Options) {
		o.window = window
	}
}

// WithTimeout sets the s the period of the open state,
// after which the state of the Breaker becomes half-open.
// Default is 10 seconds. Must be at least one second.
func WithTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.timeout = timeout
	}
}

// Counts holds the numbers of requests and their successes/failures.
// Counts are kept in rolling window.
type Counts struct {
	Requests             uint64
	TotalSuccesses       uint64
	TotalFailures        uint64
	ConsecutiveSuccesses uint64
	ConsecutiveFailures  uint64
}

// DefaultReadyToTrip is the default function called by WithReadyToTrip.
// It returns true if ConsecutiveFailures is greater than 5
func DefaultReadyToTrip(counts Counts) bool {
	return counts.ConsecutiveFailures > 5
}

// WithReadyToTrip sets a function to call whenever a request fails in the closed state.
// If this function returns true, the Breaker will be placed into the open state.
// The default is DefaultReadyToTrip.
func WithReadyToTrip(readyToTrip ReadyToTrip) Option {
	return func(o *Options) {
		o.readyToTrip = readyToTrip
	}
}

// WithOnStateChange sets a function that is called whenever the state of the Breaker changes.
// There is no default.
func WithOnStateChange(onStateChange OnStateChange) Option {
	return func(o *Options) {
		o.onStateChange = onStateChange
	}
}

// Breaker is a circuit breaker that uses rolling time windows.
type Breaker struct {
	lastStateChange      time.Time
	requests             *timePolicy
	totalSuccesses       *timePolicy
	totalFailures        *timePolicy
	options              Options
	currentState         State
	consecutiveSuccesses uint64
	consecutiveFailures  uint64
	lock                 sync.Mutex
}

// New creates a Breaker
func New(options ...Option) (*Breaker, error) {
	opts := Options{}

	for _, o := range options {
		o(&opts)
	}

	if opts.maxRequests <= 0 {
		opts.maxRequests = 1
	}

	if opts.window < time.Second {
		opts.window = time.Second
	}

	if opts.timeout < time.Second {
		opts.timeout = time.Second
	}

	if opts.readyToTrip == nil {
		opts.readyToTrip = DefaultReadyToTrip
	}

	if opts.onStateChange == nil {
		opts.onStateChange = func(from State, to State) {}
	}

	// one bucket per second.  Should this be configurable?
	numBuckets := opts.window / time.Second

	b := &Breaker{
		options:         opts,
		requests:        newTimePolicy(rolling.NewWindow(int(numBuckets)), time.Second),
		totalSuccesses:  newTimePolicy(rolling.NewWindow(int(numBuckets)), time.Second),
		totalFailures:   newTimePolicy(rolling.NewWindow(int(numBuckets)), time.Second),
		currentState:    StateClosed,
		lastStateChange: timeNow(),
	}

	return b, nil
}

// State returns the current state .
func (b *Breaker) State() State {
	b.lock.Lock()
	defer b.lock.Unlock()

	state := b.currentState

	if state == StateOpen {
		now := timeNow()
		if b.lastStateChange.Add(b.options.timeout).Before(now) {
			b.switchState(StateOpen, StateHalfOpen)
			return b.currentState
		}
	}

	return state
}

// Allow checks if a new request can proceed. It returns a callback that should be used to register
// the success or failure in a separate step. If the circuit breaker doesn't allow requests, it returns an error.
func (b *Breaker) Allow() (func(bool), error) {
	s := b.State()

	switch s {
	case StateOpen:
		return nil, ErrOpenState
	case StateHalfOpen:
		requests := uint64(b.requests.Reduce(rolling.Sum))
		if requests > b.options.maxRequests {
			return nil, ErrTooManyRequests
		}
	}

	b.requests.Append(1.0)

	return b.allowResult, nil
}

// to help testing
var timeNow = time.Now

// must be called with lock
func (b *Breaker) switchState(from State, to State) {
	if from == to {
		return
	}

	b.lastStateChange = timeNow()

	b.currentState = to

	b.options.onStateChange(from, to)
}

func (b *Breaker) setState(state State) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.switchState(b.currentState, state)
}

func (b *Breaker) allowResult(success bool) {
	state := b.State()

	if success {
		b.onSuccess()
		switch state {
		case StateClosed, StateOpen:
			return
		case StateHalfOpen:
			consecutiveSuccesses := atomic.LoadUint64(&b.consecutiveSuccesses)
			if consecutiveSuccesses >= b.options.maxRequests {
				b.setState(StateClosed)
			}
		}

		return
	}

	b.onFailure()

	switch state {
	case StateClosed:
		counts := Counts{
			Requests:             uint64(b.requests.Reduce(rolling.Sum)),
			TotalSuccesses:       uint64(b.totalSuccesses.Reduce(rolling.Sum)),
			TotalFailures:        uint64(b.totalFailures.Reduce(rolling.Sum)),
			ConsecutiveSuccesses: atomic.LoadUint64(&b.consecutiveSuccesses),
			ConsecutiveFailures:  atomic.LoadUint64(&b.consecutiveFailures),
		}

		if b.options.readyToTrip(counts) {
			b.setState(StateOpen)
		}
	case StateHalfOpen:
		b.setState(StateOpen)
	}
}

func (b *Breaker) onSuccess() {
	b.totalSuccesses.Append(1.0)
	atomic.AddUint64(&b.consecutiveSuccesses, 1)
	atomic.StoreUint64(&b.consecutiveFailures, 0)
}

func (b *Breaker) onFailure() {
	b.totalFailures.Append(1.0)
	atomic.AddUint64(&b.consecutiveFailures, 1)
	atomic.StoreUint64(&b.consecutiveSuccesses, 0)
}

type timePolicy struct {
	policy *rolling.TimePolicy
	lock   sync.Mutex
}

func newTimePolicy(window rolling.Window, bucketDuration time.Duration) *timePolicy {
	return &timePolicy{
		policy: rolling.NewTimePolicy(window, bucketDuration),
	}
}

func (p *timePolicy) Append(value float64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.policy.Append(value)
}

func (p *timePolicy) Reduce(f func(rolling.Window) float64) float64 {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.policy.Reduce(f)
}
