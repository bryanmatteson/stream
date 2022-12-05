package stream

import (
	"context"
	"errors"
	"reflect"
)

type FlowFunc func(Flow) Flow

type Stream struct {
	ops    Flow
	source Producer
	sink   Consumer
}

func From(source interface{}) *Stream {
	switch src := source.(type) {
	case Generator:
		return &Stream{source: sourcer(src)}
	case Producer:
		return &Stream{source: src}
	}

	var gen Generator
	switch src := reflect.ValueOf(source); src.Kind() {
	case reflect.Slice, reflect.Array:
		gen = EmitterFunc(func(e Emitter) {
			for i := 0; i < src.Len() && e.Emit(src.Index(i).Interface()); i++ {
			}
		})

	case reflect.Map:
		iter := src.MapRange()
		gen = EmitterFunc(func(e Emitter) {
			for iter.Next() && e.Emit(KeyValue{Key: iter.Key(), Value: iter.Value()}) {
			}
		})

	case reflect.Chan:
		gen = EmitterFunc(func(e Emitter) {
			for val, open := src.Recv(); open && e.Emit(val.Interface()); val, open = src.Recv() {
			}
		})

	case reflect.String:
		str := source.(string)
		gen = EmitterFunc(func(e Emitter) {
			for _, ch := range str {
				if !e.Emit(ch) {
					return
				}
			}
		})
	}

	if gen == nil {
		panic(ErrInvalidSource)
	}

	return &Stream{source: sourcer(gen)}
}

func FromRange(from, to int) *Stream {
	return From(EmitterFunc(func(e Emitter) {
		for from <= to && e.Emit(from) {
			from++
		}
	}))
}

func (s *Stream) Flow(fn FlowFunc) *Stream {
	var ops Flow
	s.ops = append(s.ops, fn(ops)...)
	return s
}

func (s *Stream) CollectInto(target interface{}) *Stream {
	return s.Sink(
		ReceiverFunc(func(r Receiver) {
			ptr := reflect.ValueOf(target)
			if ptr.Kind() != reflect.Ptr || ptr.Elem().Kind() != reflect.Slice {
				r.Fail(errors.New("no such slice: element must be a pointer to a slice"))
				return
			}

			dest := ptr.Elem()
			for r.Next() {
				dest.Set(reflect.Append(dest, reflect.ValueOf(r.Get())))
			}
		}),
	)
}

func (s *Stream) ConsumeEach(fn ForEachFunc) *Stream {
	return s.Sink(
		ReceiverFunc(func(r Receiver) {
			for r.Next() {
				fn(r.Get())
			}
		}),
	)
}

func (s *Stream) ConsumeEachT(fn GenericFunc) *Stream {
	return s.ConsumeEach(convertForEachFunc(fn))
}

func (s *Stream) Sink(sink Consumer) *Stream { s.sink = sink; return s }

func (s *Stream) Exec(parent context.Context) error {
	ctx := NewContext(parent)
	ch := s.source.Produce(ctx)
	ch = s.ops.Run(ctx, ch)
	s.sink.Consume(ctx, ch)
	return ctx.Err()
}

func (s *Stream) WhereT(fn interface{}) *Stream   { s.ops = s.ops.WhereT(fn); return s }
func (s *Stream) MapT(fn interface{}) *Stream     { s.ops = s.ops.MapT(fn); return s }
func (s *Stream) EachT(fn interface{}) *Stream    { s.ops = s.ops.EachT(fn); return s }
func (s *Stream) TakeT(fn interface{}) *Stream    { s.ops = s.ops.TakeT(fn); return s }
func (s *Stream) DropT(fn interface{}) *Stream    { s.ops = s.ops.DropT(fn); return s }
func (s *Stream) ProcessT(fn interface{}) *Stream { s.ops = s.ops.ProcessT(fn); return s }
func (s *Stream) Each(fn ForEachFunc) *Stream     { s.ops = s.ops.Each(fn); return s }
func (s *Stream) Process(fn OnDataFunc) *Stream   { s.ops = s.ops.Process(fn); return s }
func (s *Stream) Take(fn PredicateFunc) *Stream   { s.ops = s.ops.Take(fn); return s }
func (s *Stream) Where(fn PredicateFunc) *Stream  { s.ops = s.ops.Where(fn); return s }
func (s *Stream) Map(fn MapFunc) *Stream          { s.ops = s.ops.Map(fn); return s }
func (s *Stream) Drop(fn PredicateFunc) *Stream   { s.ops = s.ops.Drop(fn); return s }
func (s *Stream) TakeFirstN(n int) *Stream        { s.ops = s.ops.TakeFirstN(n); return s }
func (s *Stream) DropFirstN(n int) *Stream        { s.ops = s.ops.DropFirstN(n); return s }
func (s *Stream) Flatten() *Stream                { s.ops = s.ops.Flatten(); return s }
