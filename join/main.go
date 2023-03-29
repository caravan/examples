package main

import (
	"fmt"
	"math/rand"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/build"
)

func main() {
	// Create new topics with permanent retention
	left := essentials.NewTopic[stream.Event]()
	right := essentials.NewTopic[stream.Event]()
	out := essentials.NewTopic[stream.Event]()
	msg := message.Of[stream.Event]()

	s, _ := build.
		TopicSource(left).
		Filter(func(e stream.Event) bool {
			// Filter out numbers greater than or equal to 200
			return e.(int) < 200
		}).
		Join(
			build.
				TopicSource(right).
				Filter(func(e stream.Event) bool {
					// Filter out numbers less than or equal to 100
					return e.(int) > 100
				}),
			func(l stream.Event, r stream.Event) bool {
				// Only join if the left is even, and the right is odd
				return l.(int)%2 == 0 && r.(int)%2 == 1
			},
			func(l stream.Event, r stream.Event) stream.Event {
				// Join by multiplying the numbers
				return l.(int) * r.(int)
			},
		).
		TopicSink(out).
		Stream()
	_ = s.Start()

	go func() {
		// Start sending stuff to the topic
		lp := left.NewProducer()
		rp := right.NewProducer()
		for i := 0; i < 10000; i++ {
			msg.Send(lp, rand.Intn(1000))
			msg.Send(rp, rand.Intn(1000))
		}
		lp.Close()
		rp.Close()
	}()

	c := out.NewConsumer()
	for i := 0; i < 10; i++ {
		// Display the first ten that come out
		fmt.Println(msg.MustReceive(c))
	}
	c.Close()
}
