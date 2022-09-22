package coordinator

import "sync"

type CriticalResource[T any] struct {
	resource T
	mux sync.Mutex
}

func (cr *CriticalResource[T]) Set(resource T) {
	cr.mux.Lock()
	cr.resource = resource
	cr.mux.Unlock()
}

func (cr *CriticalResource[T]) Get() T {
	cr.mux.Lock()
	ret := cr.resource
	cr.mux.Unlock()
	return ret
}

type CriticalList[T any] struct {
	CriticalResource[[]T]
}

func (cl *CriticalList[T]) GetAt(i int) T {
	cl.mux.Lock()
	ret := cl.resource[i]
	cl.mux.Unlock()
	return ret
}

func (cl *CriticalList[T]) SetAt(i int, value T) {
	cl.mux.Lock()
	cl.resource[i] = value
	cl.mux.Unlock()
}

func (cl *CriticalList[T]) Append(value T) int {
	cl.mux.Lock()
	ret := len(cl.resource)
	cl.resource = append(cl.resource, value)
	cl.mux.Unlock()
	return ret
}

func (cl *CriticalList[T]) Len() int {
	cl.mux.Lock()
	ret := len(cl.resource)
	cl.mux.Unlock()
	return ret
}
