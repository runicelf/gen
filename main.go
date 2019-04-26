package main

import (
	"fmt"
	"github.com/go-redis/redis"
	uuid "github.com/satori/go.uuid"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	lastIndex               = "lastIndex"
	messageQueue            = "messageQueue"
	messagePartition        = "messagePartition"
	generatorHeartbeatQueue = "generatorHeartbeatQueue"
	electionQueue           = "electionQueue"
	queueLen                = 1000
)

var appUUID = uuid.Must(uuid.NewV4()).String()

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := client.Ping().Result()
	if err != nil {
		panic(err)
	}

	handleMessages(client)
}

func handleMessages(client *redis.Client) {
	messageQueue := subscribe(client, messageQueue)
	generatorHeartbeat := subscribe(client, generatorHeartbeatQueue)
	election := subscribe(client, electionQueue)

	go listen(generatorHeartbeat, client)

	isFirst := true

	for {
		select {
		case appId := <-election:
			if isFirst {
				if appId.Payload == appUUID {
					generateMessages(client)
				}
				isFirst = false
				go switchAfterTimeout(&isFirst)
			}

		case msg := <-messageQueue:
			splittedInfo := strings.SplitN(msg.Payload, ":", 2)
			i, payload := splittedInfo[0], splittedInfo[1]

			if client.HSetNX(messagePartition, i, 1).Val() {
				if payload == "0" {
					fmt.Println(i, "error")
				} else {
					fmt.Println(i, payload)
				}
			}
		}
	}
}

func subscribe(client *redis.Client, pattern string) <-chan *redis.Message {
	pubsub := client.Subscribe(pattern)
	_, err := pubsub.Receive()
	if err != nil {
		panic(err)
	}
	return pubsub.Channel()
}

func switchAfterTimeout(flag *bool) {
	<-time.After(5 * time.Second)
	*flag = true
}

func listen(generatorHeartbeat <-chan *redis.Message, client *redis.Client) {
	ticker := time.NewTicker(1 * time.Second)
	lastHeartbeat := time.Now()

	for {
		select {
		case <-ticker.C:
			if time.Since(lastHeartbeat) > 5*time.Second {
				err := client.Publish(electionQueue, appUUID).Err()
				if err != nil {
					panic(err)
				}
				lastHeartbeat = time.Now()
			}
		case <-generatorHeartbeat:
			lastHeartbeat = time.Now()
		}
	}
}

func generateMessages(client *redis.Client) {
	ticker := time.NewTicker(1 * time.Second)

	go beat(client)

	var i int

	if client.Exists(lastIndex).Val() == 0 {
		client.Set(lastIndex, i, 0)
	} else {
		var err error
		i, err = client.Get(lastIndex).Int()
		if err != nil {
			panic(err)
		}
	}

	for range ticker.C {
		err := client.HDel(messagePartition, strconv.Itoa(i)).Err()
		if err != nil {
			panic(err)
		}

		random := rand.Intn(19)
		err = client.Publish(messageQueue, fmt.Sprintf("%d:%d", i, random)).Err()
		if err != nil {
			panic(err)
		}

		err = client.Set(lastIndex, i, 0).Err()
		if err != nil {
			panic(err)
		}

		i++
		if i >= queueLen {
			i = 0
		}
	}
}

func beat(client *redis.Client) {
	ticker := time.NewTicker(1 * time.Second)

	for range ticker.C {
		err := client.Publish(generatorHeartbeatQueue, 1).Err()
		if err != nil {
			panic(err)
		}
	}
}
