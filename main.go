package main

import (
	"fmt"
	"github.com/go-redis/redis"
	uuid "github.com/satori/go.uuid"
	"math/rand"
	"os"
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
	errorList               = "errorList"
	queueLen                = 1000
	getErrorModKey          = "getErrors"
)

var appUUID = uuid.Must(uuid.NewV4()).String()

func main() {
	args := os.Args[1:]

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := client.Ping().Result()
	if err != nil {
		panic(err)
	}

	if len(args) >= 1 && args[0] == getErrorModKey {
		for {
			outerErr := client.LPop(errorList).Val()
			fmt.Println(outerErr)
			if outerErr == "" {
				return
			}
		}
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
				random := rand.Intn(19)
				if random == 0 {
					client.LPush(errorList, payload)
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
	ticker := time.NewTicker(500 * time.Millisecond)

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

		randomUUID := uuid.Must(uuid.NewV4()).String()
		err = client.Publish(messageQueue, fmt.Sprintf("%d:%s", i, randomUUID)).Err()
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
