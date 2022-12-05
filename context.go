package stream

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type Context interface {
	context.Context
	recover()
	close(err error) error
}

var ErrDone = errors.New("context is done")

type streamCtx struct {
	parent context.Context

	done     chan struct{}
	doneMark uint32
	doneOnce sync.Once

	err error
}

func NewBackgroundContext() Context {
	return NewContext(context.Background())
}
func NewContext(parent context.Context) Context {
	ctx := &streamCtx{parent: parent, done: make(chan struct{})}
	if parent.Done() != nil {
		go ctx.wait()
	}
	return ctx
}

func (c *streamCtx) Done() <-chan struct{}             { return c.done }
func (c *streamCtx) Deadline() (time.Time, bool)       { return c.parent.Deadline() }
func (c *streamCtx) Value(key interface{}) interface{} { return c.parent.Value(key) }

func (c *streamCtx) Err() error {
	if atomic.LoadUint32(&c.doneMark) != 0 {
		return c.err
	}

	select {
	case <-c.parent.Done():
		return c.close(c.parent.Err())
	default:
		return nil
	}
}

func (c *streamCtx) close(err error) error {
	c.doneOnce.Do(func() {
		if err == ErrDone {
			err = nil
		}
		c.err = err
		atomic.StoreUint32(&c.doneMark, 1)
		close(c.done)
	})
	return c.err
}

func (c *streamCtx) wait() {
	select {
	case <-c.parent.Done():
		c.close(c.parent.Err())
	case <-c.done:
	}
}

func (c *streamCtx) recover() {
	r := recover()
	if r != nil {
		if r != ErrDone {
			debug.PrintStack()
		}
		err := fmt.Errorf("recovered from %v", r)
		if e, ok := r.(error); ok {
			err = e
		}
		c.close(err)
	}
}
