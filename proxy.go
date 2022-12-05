package stream

import (
	"sync"
)

func NewEmitter(ctx Context, out InChan) Emitter {
	return NewProxy(ctx, nil, out)
}
func NewReceiver(ctx Context, in OutChan) Receiver { return NewProxy(ctx, in, nil) }
func NewProxy(ctx Context, in OutChan, out InChan) Proxy {
	return &channelproxy{ctx: ctx, out: out, in: in, done: make(chan struct{})}
}

type channelproxy struct {
	ctx     Context
	out     InChan
	in      OutChan
	once    sync.Once
	done    chan struct{}
	current T
}

func (c *channelproxy) Context() Context { return c.ctx }

func (c *channelproxy) Emit(data T) bool {
	select {
	case <-c.ctx.Done():
		return false
	case <-c.done:
		return false
	case c.out.In() <- data:
		return true
	}
}

func (r *channelproxy) Recv(v *T) (ok bool) {
	if ok = r.Next(); ok {
		*v = r.current
	}
	return
}

func (r *channelproxy) Next() (ok bool) {
	select {
	case <-r.ctx.Done():
	case <-r.done:
	case r.current, ok = <-r.in.Out():
	}
	return
}

func (r *channelproxy) Get() T {
	return r.current
}

func (r *channelproxy) Break() {
	r.once.Do(func() {
		close(r.done)
	})
}
func (r *channelproxy) Fail(err error) {
	r.once.Do(func() {
		close(r.done)
		r.ctx.close(err)
	})
}
