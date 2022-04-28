package chansplit

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSplitter(t *testing.T) {
	sendCh := make(chan int)
	channelSplitter := New(sendCh)
	receiveCh1 := channelSplitter.GetOutputCh()
	receiveCh2 := channelSplitter.GetOutputCh()

	go func() {
		i := 0
		for num := range receiveCh1 {
			assert.Equal(t, i, num)
			fmt.Println(num)
			time.Sleep(20 * time.Microsecond)
			i++
		}
	}()

	go func() {
		i := 0
		for num := range receiveCh2 {
			assert.Equal(t, i, num)
			fmt.Println(num)
			time.Sleep(200 * time.Microsecond)
			i++
		}
	}()

	for i := 0; i < 1000; i++ {
		sendCh <- i
	}
	time.Sleep(time.Second)
	for i := 1000; i < 2000; i++ {
		sendCh <- i
	}
	close(sendCh)
	channelSplitter.Wait()
}

func BenchmarkSplitter_SingleOutput(b *testing.B) {
	sendCh := make(chan int)
	channelSplitter := New(sendCh)
	receiveCh1 := channelSplitter.GetOutputCh()
	for i := 0; i < b.N; i++ {
		go func() {
			sendCh <- i
		}()
		<-receiveCh1
	}
}

func BenchmarkSplitter_MultipleOutputs(b *testing.B) {
	sendCh := make(chan int)
	channelSplitter := New(sendCh)
	receiveCh1 := channelSplitter.GetOutputCh()
	receiveCh2 := channelSplitter.GetOutputCh()
	receiveCh3 := channelSplitter.GetOutputCh()
	for i := 0; i < b.N; i++ {
		go func() {
			sendCh <- i
		}()
		<-receiveCh1
		<-receiveCh2
		<-receiveCh3
	}
}

func BenchmarkDefaultChannel(b *testing.B) {
	sendCh := make(chan int)
	for i := 0; i < b.N; i++ {
		go func() {
			sendCh <- i
		}()
		<-sendCh
	}
}

func Example() {
	// Split one input channel into two output channels and wait for them to finish the processing before finishing.
	ch := make(chan int)
	s := New(ch)

	outCh1 := s.GetOutputCh()
	outCh2 := s.GetOutputCh()

	// Start two consuming go routines, one for each output channel.
	go func() {
		for num := range outCh1 {
			fmt.Printf("Consumer 1 got message: %d\n", num)
			time.Sleep(10 * time.Millisecond) // simulate some work
		}
	}()
	go func() {
		for num := range outCh2 {
			fmt.Printf("Consumer 2 got message: %d\n", num)
			time.Sleep(100 * time.Millisecond) // simulate some work
		}
	}()

	ch <- 1
	ch <- 2

	// Closing the input channel closes all the output channels as well after all the messages have been received.
	close(ch)

	// Wait until all the output channels have been successfully closed. Beware that this does not wait for the
	// work of the consuming go routine to be finished. To ensure this, we would have to use a sync.WaitGroup
	s.Wait()

	// Output:
	// Consumer 1 got message: 1
	// Consumer 2 got message: 1 - the order of the first two lines is non-deterministic
	// Consumer 1 got message: 2
	// Consumer 2 got message: 2 - this line might not be printed due to the missing sync.WaitGroup
}
