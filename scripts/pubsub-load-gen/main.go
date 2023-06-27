package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	p := flag.String("payload", "", "message payload")
	c := flag.Int("count", 300, "message count")
	d := flag.Duration("delay", time.Second, "delay")
	t := flag.String("topic", "impression_event", "pubsub topic name")
	s := flag.String("subscription", "impression_event_testing", "(optional) subscription name to create")
	project := flag.String("project", "", "GCP Project ID")
	flag.Parse()

	if *p == "" {
		log.Println("ERR: payload not specified. Use flag -payload to specify payload")
		os.Exit(1)
	}

	log.Printf("INF: creating pubsub client with project=%s ...", *project)
	client, err := pubsub.NewClient(context.Background(), *project)
	if err != nil {
		log.Printf("ERR: unable to create pubsub client: %v", err)
		os.Exit(1)
	}
	defer client.Close()
	log.Println("INF: pubsub client created!")

	log.Printf("INF: creating topic name=%s ...", *t)
	topic, err := client.CreateTopic(context.Background(), *t)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			log.Printf("WRN: attempted to create topic name=%s but it already existed", *t)
			topic = client.Topic(*t)
		} else {
			log.Printf("ERR: unable to create topic: %v", err)
			os.Exit(1)
		}
	}
	defer topic.Stop()
	log.Printf("INF: topic name=%s created!", *t)

	if *s != "" {
		_, err := client.CreateSubscription(context.Background(), *s, pubsub.SubscriptionConfig{
			Topic: topic,
		})
		if status.Code(err) == codes.AlreadyExists {
			log.Printf("WRN: attempted to create subscription name=%s topic=%s but it already existed", *s, *t)
		} else {
			log.Printf("ERR: unable to create subscription: %v", err)
			os.Exit(1)
		}
	}

	log.Printf("INF: begin sending messages with payload=%s", *p)
	payload := []byte(*p)
	for i := 0; i < *c; i++ {
		_ = topic.Publish(context.Background(), &pubsub.Message{
			ID:   uuid.NewV4().String(),
			Data: payload,
		})
		log.Println("INF: success sending message!")
		time.Sleep(*d)
	}
}
