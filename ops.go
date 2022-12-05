package stream

import (
	"reflect"
	"sync"
)

func sourcer(g Generator) Producer {
	return ProducerFunc(func(ctx Context) OutChan {
		ch := NewChannel()
		go func() {
			defer ch.Close()
			g.Generate(ctx, ch)
		}()
		return ch
	})
}

func runner(op Operator) Runner {
	return Runnable(func(ctx Context, in OutChan) OutChan {
		ch := NewChannel()
		go func() {
			defer ch.Close()
			op.Operate(ctx, in, ch)
		}()
		return ch
	})
}

type Flow []Runner

func (b Flow) Run(ctx Context, in OutChan) OutChan {
	for _, op := range b {
		in = op.Run(ctx, in)
	}
	return in
}

func (b *Flow) Add(op Operator) Flow { *b = append(*b, runner(op)); return *b }

func (b Flow) WhereT(fn interface{}) Flow   { return b.Where(convertPredicateFunc(fn)) }
func (b Flow) MapT(fn interface{}) Flow     { return b.Map(convertMapFunc(fn)) }
func (b Flow) EachT(fn interface{}) Flow    { return b.Each(convertForEachFunc(fn)) }
func (b Flow) TakeT(fn interface{}) Flow    { return b.Take(convertPredicateFunc(fn)) }
func (b Flow) DropT(fn interface{}) Flow    { return b.Drop(convertPredicateFunc(fn)) }
func (b Flow) ProcessT(fn interface{}) Flow { return b.Process(convertOnDataFunc(fn)) }

func (b Flow) Each(fn ForEachFunc) Flow {
	return b.Add(ProxyFunc(func(p Proxy) {
		for p.Next() && p.Emit(p.Get()) {
			fn(p.Get())
		}
	}))
}

func (b Flow) Process(fn OnDataFunc) Flow {
	return b.Add(ProxyFunc(func(p Proxy) {
		for p.Next() {
			fn(p.Get(), p)
		}
	}))
}

func (b Flow) Take(fn PredicateFunc) Flow {
	return b.Where(fn)
}

func (b Flow) Where(fn PredicateFunc) Flow {
	return b.Add(ProxyFunc(func(p Proxy) {
		for p.Next() {
			if !fn(p.Get()) {
				continue
			}
			if !p.Emit(p.Get()) {
				break
			}
		}
	}))
}

func (b Flow) Map(fn MapFunc) Flow {
	return b.Add(ProxyFunc(func(p Proxy) {
		for p.Next() && p.Emit(fn(p.Get())) {
		}
	}))
}

func (b Flow) Drop(fn PredicateFunc) Flow {
	return b.Where(func(data T) bool { return !fn(data) })
}

func (b Flow) TakeFirstN(n int) Flow {
	return b.Add(ProxyFunc(func(p Proxy) {
		taken := 0
		for p.Next() && taken <= n && p.Emit(p.Get()) {
			taken++
		}
	}))
}

func (b Flow) DropFirstN(n int) Flow {
	return b.Add(ProxyFunc(func(p Proxy) {
		dropped := 0
		for dropped < n && p.Next() {
			dropped++
		}
		for p.Next() && p.Emit(p.Get()) {
		}
	}))
}

func (b Flow) Flatten() Flow {
	return b.Add(ProxyFunc(func(p Proxy) {
		for p.Next() {
			dv := reflect.Indirect(reflect.ValueOf(p.Get()))
			switch dv.Kind() {
			case reflect.Slice:
				for i := 0; i < dv.Len() && p.Emit(dv.Index(i).Interface()); i++ {
				}
			default:
				p.Emit(dv.Interface())
			}
		}
	}))
}

func (b Flow) Transform(trans Transform) Flow { return Flow{trans(b)} }

func (b Flow) Parallel(n int) Flow {
	return b.Transform(func(r Runner) Runner {
		return Runnable(func(ctx Context, in OutChan) OutChan {
			outputs := make([]OutChan, n)
			for i := range outputs {
				outputs[i] = r.Run(ctx, in)
			}

			var wg sync.WaitGroup
			ch := NewChannel()
			for i := range outputs {
				wg.Add(1)
				go func(out OutChan) {
					defer wg.Done()
					for data := range out.Out() {
						ch.In() <- data
					}
				}(outputs[i])
			}

			go func() {
				defer ch.Close()
				wg.Wait()
			}()

			return ch
		})
	})
}
