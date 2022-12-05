package stream

type Producer interface {
	Produce(ctx Context) OutChan
}
type ProducerFunc func(ctx Context) OutChan

func (fn ProducerFunc) Produce(ctx Context) OutChan { return fn(ctx) }

type Generator interface {
	Generate(ctx Context, out InChan)
}

type Operator interface {
	Operate(ctx Context, in OutChan, out InChan)
}

type Consumer interface {
	Consume(ctx Context, in OutChan)
}

type Runner interface {
	Run(ctx Context, in OutChan) OutChan
}

type Runnable func(ctx Context, in OutChan) OutChan

func (fn Runnable) Run(ctx Context, in OutChan) OutChan { return fn(ctx, in) }

type Transform func(Runner) Runner

type ReceiverFunc func(r Receiver)

func (fn ReceiverFunc) Consume(ctx Context, in OutChan) { fn(NewReceiver(ctx, in)) }

type ProxyFunc func(p Proxy)

func (fn ProxyFunc) Operate(ctx Context, in OutChan, out InChan) { fn(NewProxy(ctx, in, out)) }

type EmitterFunc func(e Emitter)

func (fn EmitterFunc) Generate(ctx Context, out InChan) { fn(NewEmitter(ctx, out)) }

type GeneratorFunc func(ctx Context, out InChan)

func (fn GeneratorFunc) Generate(ctx Context, out InChan) { fn(ctx, out) }

type OperatorFunc func(ctx Context, in OutChan, out InChan)

func (fn OperatorFunc) Operate(ctx Context, in OutChan, out InChan) { fn(ctx, in, out) }

type ConsumerFunc func(ctx Context, in OutChan)

func (fn ConsumerFunc) Consume(ctx Context, in OutChan) { fn(ctx, in) }

type Driver interface {
	Context() Context
	Break()
	Fail(err error)
}

type Emitter interface {
	Driver
	Emit(data T) bool
}

type Receiver interface {
	Driver
	Recv(v *T) bool
	Next() bool
	Get() T
}
type Proxy interface {
	Emitter
	Receiver
}

type T interface{}
type Readable <-chan T
type Writable chan<- T

type Closable interface {
	Close()
}
type Channel interface {
	InChan
	OutChan
}
type InChan interface {
	In() Writable
	Closable
}

type OutChan interface {
	Out() Readable
}

type Chan struct {
	ch chan T
}

func NewChannel() Chan                   { return Chan{ch: make(chan T, 128)} }
func (c Chan) ToPair() (InChan, OutChan) { return c, c }
func (c Chan) In() Writable              { return c.ch }
func (c Chan) Out() Readable             { return c.ch }
func (c Chan) Len() int                  { return len(c.ch) }
func (c Chan) Cap() int                  { return cap(c.ch) }
func (c Chan) Close()                    { close(c.ch) }

type KeyValue struct {
	Key   interface{}
	Value interface{}
}
