package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jakebailey/irc"
	flags "github.com/jessevdk/go-flags"
	"github.com/joho/godotenv"
	yaml "gopkg.in/yaml.v2"
)

const twitchIRC = "irc.chat.twitch.tv:6697"

var (
	errEmptyNick       = errors.New("empty nick")
	errEmptyPass       = errors.New("empty pass")
	errNonOauthPass    = errors.New("pass did not start with oauth")
	errBadTopics       = errors.New("pub and sub topics are the same or empty")
	errBadQOS          = errors.New("invalid QOS")
	errChannelsNoTopic = errors.New("channels provided without publish topic")
	errEmptyChannel    = errors.New("empty channel name")
)

var args = struct {
	MQTTBroker string `long:"mqtt-broker" env:"MQTT_BROKER" required:"true"`
	ConfigPath string `long:"config" env:"CONFIG"`
	Debug      bool   `long:"debug" env:"DEBUG" description:"enables debug logging"`
}{
	ConfigPath: "config.yaml",
}

type Config struct {
	Connections []*Connection
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if err := godotenv.Load(); err != nil {
		if !os.IsNotExist(err) {
			log.Fatal(err)
		}
	}

	if _, err := flags.Parse(&args); err != nil {
		os.Exit(1)
	}

	b, err := ioutil.ReadFile(args.ConfigPath)
	if err != nil {
		log.Fatal(err)
	}

	var config Config
	if err := yaml.Unmarshal(b, &config); err != nil {
		log.Fatal(err)
	}

	exit := false
	for i, c := range config.Connections {
		if err := c.validate(); err != nil {
			log.Println(i, err)
			exit = true
		}
	}

	if exit {
		os.Exit(1)
	}

	cOpts := mqtt.NewClientOptions()
	cOpts.SetClientID(fmt.Sprintf("%d%d", time.Now().UnixNano(), rand.Intn(10)))
	cOpts.SetCleanSession(false)
	cOpts.AddBroker(args.MQTTBroker)
	client := mqtt.NewClient(cOpts)

	if t := client.Connect(); t.Wait() && t.Error() != nil {
		log.Fatal(t.Error())
	}
	defer client.Disconnect(0)

	stop := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(len(config.Connections))

	for _, c := range config.Connections {
		go c.run(wg, stop, client)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	close(stop)
	wg.Wait()
}

type Connection struct {
	Nick string
	Pass string

	Publish struct {
		Topic    string
		QOS      byte
		Channels []string
	}

	Subscribe struct {
		Topic string
		QOS   byte
	}
}

func (c *Connection) validate() error {
	if c.Nick == "" {
		return errEmptyNick
	}

	if c.Pass == "" {
		return errEmptyPass
	}

	if !strings.HasPrefix(c.Pass, "oauth:") {
		return errNonOauthPass
	}

	if c.Publish.Topic == c.Subscribe.Topic {
		return errBadTopics
	}

	if len(c.Publish.Channels) > 0 && c.Publish.Topic == "" {
		return errChannelsNoTopic
	}

	if c.Publish.QOS > 2 || c.Subscribe.QOS > 2 {
		return errBadQOS
	}

	for _, s := range c.Publish.Channels {
		if s == "" {
			return errEmptyChannel
		}
	}

	return nil
}

func (c *Connection) run(wg *sync.WaitGroup, stop <-chan struct{}, client mqtt.Client) {
	defer wg.Done()
	var mu sync.Mutex

	conn, err := createIRCConn(c.Nick, c.Pass)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	if err := join(conn, c.Publish.Channels...); err != nil {
		log.Fatal(err)
	}

	go func() {
		<-stop
		mu.Lock()
		defer mu.Unlock()
		if err := quit(conn); err != nil {
			log.Fatal(err)
		}
	}()

	if c.Subscribe.Topic != "" {
		log.Printf("subscribing to %s at QOS %d", c.Subscribe.Topic, c.Subscribe.QOS)

		if t := client.Subscribe(c.Subscribe.Topic, c.Subscribe.QOS, func(c mqtt.Client, mq mqtt.Message) {
			var msg struct {
				Channel string
				Message string
			}

			if err := json.Unmarshal(mq.Payload(), &msg); err != nil {
				log.Println(err)
				return
			}

			if msg.Channel == "" {
				log.Println("empty channel")
				return
			}

			if msg.Channel[0] != '#' {
				msg.Channel = "#" + msg.Channel
			}

			if msg.Message == "" {
				log.Println("empty message")
				return
			}

			m := &irc.Message{
				Command:  "PRIVMSG",
				Params:   []string{msg.Channel},
				Trailing: msg.Message,
			}

			if args.Debug {
				log.Println("<", m.String())
			}

			mu.Lock()
			defer mu.Unlock()

			if err := conn.Encode(m); err != nil {
				log.Println(err)
			}
		}); t.Wait() && t.Error() != nil {
			log.Fatal(t.Error())
		}
	}

	if c.Publish.Topic != "" {
		log.Printf("publishing to %s at QOS %d", c.Publish.Topic, c.Publish.QOS)
	}

	for {
		var m irc.Message
		if err := conn.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		if args.Debug {
			log.Println(">", m.Raw)
		} else {
			switch m.Command {
			case "PRIVMSG", "NOTICE", "USERNOTICE", "PING", "CLEARCHAT", "HOSTTARGET":
				// Do nothing.
			default:
				log.Println(">", m.Raw)
			}
		}

		if m.Command == "PING" {
			m.Command = "PONG"
			if err := conn.Encode(&m); err != nil {
				log.Println(err)
			}
			continue
		}

		if c.Publish.Topic != "" {
			b, err := json.Marshal(m)
			if err != nil {
				log.Println(err)
				continue
			}

			t := client.Publish(c.Publish.Topic, c.Publish.QOS, false, b)
			if err := t.Error(); err != nil {
				log.Println(err)
			}
		}

		if m.Command == "RECONNECT" {
			log.Println("server sent RECONNECT, restarting process")
			time.Sleep(time.Second)
			restartProcess()
		}
	}
}

func createIRCConn(nick, pass string) (irc.Conn, error) {
	tconn, err := tls.Dial("tcp", twitchIRC, nil)
	if err != nil {
		return nil, err
	}
	conn := irc.NewBaseConn(tconn)

	if err := login(conn, nick, pass); err != nil {
		return nil, err
	}

	if err := capReq(conn,
		"twitch.tv/tags",
		"twitch.tv/commands",
	); err != nil {
		return nil, err
	}

	return conn, nil
}

func login(conn irc.Encoder, nick, pass string) error {
	err := conn.Encode(&irc.Message{
		Command: "PASS",
		Params:  []string{pass},
	})
	if err != nil {
		return err
	}

	return conn.Encode(&irc.Message{
		Command: "NICK",
		Params:  []string{nick},
	})
}

func capReq(conn irc.Encoder, caps ...string) error {
	if len(caps) == 0 {
		return nil
	}

	return conn.Encode(&irc.Message{
		Command:  "CAP",
		Params:   []string{"REQ"},
		Trailing: strings.Join(caps, " "),
	})
}

func join(conn irc.Encoder, channels ...string) error {
	if len(channels) == 0 {
		return nil
	}

	for i, s := range channels {
		if s[0] != '#' {
			channels[i] = "#" + s
		}
	}

	return conn.Encode(&irc.Message{
		Command: "JOIN",
		Params:  []string{strings.Join(channels, ",")},
	})
}

func quit(conn irc.Encoder) error {
	return conn.Encode(&irc.Message{
		Command: "QUIT",
	})
}

var (
	argv0 = os.Args[0]
	argv  = os.Args
	envv  = os.Environ()
)

func restartProcess() {
	log.Fatal(syscall.Exec(argv0, argv, envv))
}
