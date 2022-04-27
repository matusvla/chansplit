/*
Package chansplit provides a tool for the distribution of the data sent to one channel to multiple output channels.
If ensures that all the output channel consumers receive messages as soon as possible and that they do not block
each other. This is achieved using internal buffers for storing the channel values that cannot be received by the
consumers right away.

The core component of this package is the Splitter structure which is initialized using an existing input channel.
It provides method GetOutputCh that is used to obtain an output channel and can be called multiple times.

Closing of the output channels is triggered automatically by closing the input channel. All the messages that are
currently in the internal buffer of the Splitter are sent to every individual output channel before this channel is closed.
To wait until all the output channels have been successfully closed we can call the Wait method of the Splitter.

Known shortcomings

It is not beneficial to use the solution provided by this package in all cases when there is a need for forwarding
the data to multiple channels. In some of these cases it might be easier to run multiple instances of the producer instead.
The typical use-case for this package would be when the operations carried out by the message producer to get the payload
sent to the channel take a significant amount of computational time or memory, and therefore we do not want to duplicate them.

When the consumers take long time to process a message and a producer sends the messages to the channel with a high
frequency the memory used by the Splitter will gradually grow as its internal buffers will store all the unprocessed
messages. There is currently no mechanism that would suppress this behavior or limit the memory usage.
*/
package chansplit

import (
	"container/list"
	"sync"
)

// Splitter provides a way how to distribute messages sent onto one channel to multiple output channels.
// These channels are completely independent, and they do not block each other.
// Message sent onto the input channel never blocks the caller.
// If the output channels' consumers are occupied when a new incoming message is received, the message is stored in an internal buffer.
// When the input channel is closed, it closes all the output channels as well, but only after all the messages currently in the buffer have been consumed.
type Splitter[T any] struct {
	buffer      []int
	subscribers []*subscriber[T]
	wg          sync.WaitGroup
}

// New creates a new Splitter from the provided input channel
func New[T any](inCh <-chan T) *Splitter[T] {
	splitter := Splitter[T]{}
	splitter.wg.Add(1)
	go splitter.splitCh(inCh) // this go routine is terminated by close(inCh)
	return &splitter
}

// GetOutputCh returns a new output channel connected to the receiver's input channel
func (a *Splitter[T]) GetOutputCh() <-chan T {
	ch := make(chan T)
	listener := subscriber[T]{
		c:        ch,
		newValCh: make(chan struct{}),
	}
	go listener.listen() // this go func is terminated by listener.close(), which is called when decomposing the whole Splitter

	a.subscribers = append(a.subscribers, &listener)
	return ch
}

// Wait blocks until all load has been processed and all the output channels have been closed
func (a *Splitter[T]) Wait() {
	a.wg.Wait()
}

func (a *Splitter[T]) splitCh(ch <-chan T) {
	defer a.wg.Done()
	var subscriberWg sync.WaitGroup
	for val := range ch {
		for _, p := range a.subscribers {
			subscriberWg.Add(1)
			/*
				todo the number of go routines is not limited at the moment.
				If we wanted to do splitting to a high number of output channels we should introduce a worker pool instead
			*/
			go func(p *subscriber[T]) {
				defer subscriberWg.Done()
				p.send(val)
			}(p)
		}
		subscriberWg.Wait()
	}
	for _, p := range a.subscribers {
		p.close()
	}
}

type subscriber[T any] struct {
	c         chan T
	fifoQueue list.List
	newValCh  chan struct{}
	wg        sync.WaitGroup
	mu        sync.Mutex
}

func (p *subscriber[T]) send(val T) {
	p.mu.Lock()
	toProcessCount := p.fifoQueue.Len()
	p.fifoQueue.PushBack(val)
	p.mu.Unlock()

	if toProcessCount == 0 {
		p.newValCh <- struct{}{}
	}
}

func (p *subscriber[T]) close() {
	close(p.newValCh)
	p.wg.Wait()
	close(p.c)
}

func (p *subscriber[T]) listen() {
	p.wg.Add(1)
	defer p.wg.Done()
	for range p.newValCh {
		for {
			p.mu.Lock()
			toProcessCount := p.fifoQueue.Len()
			p.mu.Unlock()
			if toProcessCount == 0 {
				break
			}
			el := p.fifoQueue.Front()
			p.c <- el.Value.(T)
			p.mu.Lock()
			p.fifoQueue.Remove(el)
			p.mu.Unlock()
		}
	}
}
