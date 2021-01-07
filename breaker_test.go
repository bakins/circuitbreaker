package circuitbreaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	b, err := New()
	require.NoError(t, err)

	require.Equal(t, StateClosed, b.State())

	_, err = b.Allow()
	require.NoError(t, err)
}

func TestFailure(t *testing.T) {
	readyToTrip := func(c Counts) bool {
		require.Equal(t, uint64(1), c.ConsecutiveFailures)
		require.Equal(t, uint64(1), c.TotalFailures)
		require.Equal(t, uint64(0), c.ConsecutiveSuccesses)
		require.Equal(t, uint64(0), c.TotalSuccesses)

		return true
	}

	b, err := New(WithReadyToTrip(readyToTrip))
	require.NoError(t, err)

	require.Equal(t, StateClosed, b.State())

	cb, err := b.Allow()
	require.NoError(t, err)

	require.Equal(t, StateClosed, b.State())

	cb(false)

	require.Equal(t, StateOpen, b.State())

	cb, err = b.Allow()
	require.Equal(t, ErrOpenState, err)
	require.Nil(t, cb)
}

type testClock struct {
	now time.Time
}

func (t *testClock) Now() time.Time {
	return t.now
}

func TestHalfOpen(t *testing.T) {
	current := timeNow

	defer func() {
		timeNow = current
	}()

	c := &testClock{
		now: time.Now(),
	}

	timeNow = c.Now

	readyToTrip := func(c Counts) bool {
		return true
	}

	b, err := New(WithReadyToTrip(readyToTrip), WithTimeout(time.Second))
	require.NoError(t, err)

	require.Equal(t, StateClosed, b.State())

	cb, err := b.Allow()
	require.NoError(t, err)

	require.Equal(t, StateClosed, b.State())

	cb(false)

	require.Equal(t, StateOpen, b.State())

	cb, err = b.Allow()
	require.Equal(t, ErrOpenState, err)
	require.Nil(t, cb)

	c.now = c.now.Add(time.Minute)

	require.Equal(t, StateHalfOpen, b.State())

	cb, err = b.Allow()
	require.NoError(t, err)
	require.Equal(t, StateHalfOpen, b.State())
	require.NotEmpty(t, cb)

	cb, err = b.Allow()
	require.Equal(t, ErrTooManyRequests, err)
	require.Nil(t, cb)
	require.Equal(t, StateHalfOpen, b.State())

	// for time window
	time.Sleep(time.Second * 2)

	require.Equal(t, StateHalfOpen, b.State())

	cb, err = b.Allow()
	require.NoError(t, err)
	require.NotEmpty(t, cb)

	require.Equal(t, StateHalfOpen, b.State())

	cb(true)

	require.Equal(t, StateClosed, b.State())
}
