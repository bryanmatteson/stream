package stream

import (
	"errors"
	"reflect"
)

var (
	ErrInvalidEachFunc      = errors.New("element is not a valid each function: must be func(T)")
	ErrInvalidMapFunc       = errors.New("element is not a valid map function: must be func(T) T")
	ErrInvalidOnDataFunc    = errors.New("element is not a valid map function: must be func(T, Emitter)")
	ErrInvalidPredicateFunc = errors.New("element is not a valid predicate function: must be func(T) bool")
)

type OnDataFunc func(data T, e Emitter)
type MapFunc func(data T) T
type PredicateFunc func(data T) bool
type ForEachFunc func(data T)
type GenericFunc interface{}

func convertOnDataFunc(source GenericFunc) OnDataFunc {
	if fn, ok := source.(OnDataFunc); ok {
		return fn
	}

	typ := reflect.TypeOf(source)
	if !(typ.Kind() == reflect.Func && typ.NumIn() == 2 && typ.NumOut() == 0 && typ.In(1).Implements(reflect.TypeOf(new(Emitter)).Elem())) {
		panic(ErrInvalidOnDataFunc)
	}

	src := reflect.ValueOf(source)
	return func(data T, e Emitter) {
		src.Call([]reflect.Value{reflect.ValueOf(data), reflect.ValueOf(e)})
	}
}

func convertPredicateFunc(source GenericFunc) PredicateFunc {
	fn := reflect.ValueOf(source)
	typ := fn.Type()

	pred := reflect.TypeOf(new(PredicateFunc)).Elem()
	if typ.ConvertibleTo(pred) {
		filter := fn.Convert(pred).Interface().(PredicateFunc)
		return filter
	}

	if !(fn.Kind() == reflect.Func && typ.NumIn() == 1 && typ.NumOut() == 1 && typ.Out(0).Kind() == reflect.Bool) {
		panic(ErrInvalidPredicateFunc)
	}

	src := reflect.ValueOf(source)
	return func(data T) bool { return src.Call([]reflect.Value{reflect.ValueOf(data)})[0].Bool() }
}

func convertForEachFunc(source GenericFunc) ForEachFunc {
	if fn, ok := source.(ForEachFunc); ok {
		return fn
	}
	fntyp := reflect.TypeOf(source)
	if !(fntyp.Kind() == reflect.Func && fntyp.NumIn() == 1 && fntyp.NumOut() == 0) {
		panic(ErrInvalidEachFunc)
	}

	src := reflect.ValueOf(source)
	return func(data T) { src.Call([]reflect.Value{reflect.ValueOf(data)}) }
}

func convertMapFunc(source GenericFunc) MapFunc {
	if fn, ok := source.(MapFunc); ok {
		return fn
	}

	typ := reflect.TypeOf(source)
	if !(typ.Kind() == reflect.Func && typ.NumIn() == 1 && typ.NumOut() == 1) {
		panic(ErrInvalidMapFunc)
	}

	src := reflect.ValueOf(source)
	return func(data T) T { return src.Call([]reflect.Value{reflect.ValueOf(data)})[0].Interface() }
}
