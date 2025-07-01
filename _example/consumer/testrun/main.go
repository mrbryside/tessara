package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mrbryside/tessara"
)

type Handler struct{}

func (h Handler) Perform(pm tessara.PerformMessage) error {
	randomSleep := time.Duration(rand.Intn(600)) * time.Millisecond
	randError := rand.Intn(100)
	if string(pm.Key) == "error ja" {
		return fmt.Errorf("error from error ja")
	}
	if randError < 10 {
		return fmt.Errorf("error from random")
	}
	time.Sleep(randomSleep)
	return nil
}

func (h Handler) Fallback(pm tessara.PerformMessage, err error) {
	fmt.Println("handle error!")
	fmt.Println(err)
	// log.Panic("panic because error ja: err:", err.Error())
}

type MyErrorHandler struct{}

func (mh MyErrorHandler) HandleCommitGiveUp(topic string, partition int32) {
	log.Println("this my error give up topic: ", topic, "partition:", partition)
}

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server running on :2112/metrics")
		log.Fatal(http.ListenAndServe(":2112", nil))
	}()

	brokers := []string{"host.docker.internal:9092"}
	topic := "example-topic"
	groupID := "example-group"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := tessara.NewConsumerConfig(brokers, topic, groupID).
		WithSASL("kafkaUser", "kafkaPassword").
		WithOffsetInitialOldest().
		WithBufferSize(256).
		WithSubqueue(5).
		WithKeyDistributeMode().
		WithCommitInterval(3*time.Second).
		WithCommitGiveUpInterval(10*time.Second).
		WithCommitGiveUpTime(40*time.Second).
		WithRetry(5, 1.5)

	tessara.
		NewConsumer(cfg, Handler{}).
		WithErrorHandler(MyErrorHandler{}).
		StartConsume(ctx)

}
