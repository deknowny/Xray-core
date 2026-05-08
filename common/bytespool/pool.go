package bytespool

import "sync"

func createAllocFunc(size int32) func() interface{} {
	return func() interface{} {
		return make([]byte, size)
	}
}

// The following parameters controls the size of buffer pools.
// There are numPools pools. Starting from 2k size, the size of each pool is sizeMulti of the previous one.
// Package buf is guaranteed to not use buffers larger than the largest pool.
// Other packets may use larger buffers.
const (
	numPools  = 4
	sizeMulti = 4
)

var (
	pool     [numPools]sync.Pool
	poolSize [numPools]int32
)

func init() {
	size := int32(2048)
	for i := 0; i < numPools; i++ {
		pool[i] = sync.Pool{
			New: createAllocFunc(size),
		}
		poolSize[i] = size
		size *= sizeMulti
	}
}

// GetPool returns a sync.Pool that generates bytes array with at least the given size.
// It may return nil if no such pool exists.
//
// xray:api:stable
func GetPool(size int32) *sync.Pool {
	if idx := poolIndex(size); idx >= 0 {
		return &pool[idx]
	}
	return nil
}

// Alloc returns a byte slice with at least the given size. Minimum size of returned slice is 2048.
//
// xray:api:stable
func Alloc(size int32) []byte {
	idx := poolIndex(size)
	if idx >= 0 {
		b := pool[idx].Get().([]byte)
		capacity := int32(cap(b))
		if capacity > poolSize[numPools-1] {
			recordAlloc(-1, capacity)
		} else {
			recordAlloc(idx, capacity)
		}
		return b
	}
	recordAlloc(-1, size)
	return make([]byte, size)
}

// Free puts a byte slice into the internal pool.
//
// xray:api:stable
func Free(b []byte) {
	size := int32(cap(b))
	b = b[0:cap(b)]
	if size > poolSize[numPools-1] {
		recordFree(-1, size)
		pool[numPools-1].Put(b)
		return
	}
	for i := numPools - 1; i >= 0; i-- {
		if size >= poolSize[i] {
			recordFree(i, size)
			pool[i].Put(b)
			return
		}
	}
}

func poolIndex(size int32) int {
	for idx, ps := range poolSize {
		if size <= ps {
			return idx
		}
	}
	return -1
}
