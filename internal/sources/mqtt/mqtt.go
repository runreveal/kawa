package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/types"
	"golang.org/x/exp/slog"
)

type mqttMsg struct {
	Payload   string `json:"payload"`
	Topic     string `json:"topic"`
	Duplicate bool   `json:"duplicate"`
	MessageID uint16 `json:"messageID"`
	QOS       byte   `json:"qos"`
	Retained  bool   `json:"retained"`
}

type mqtt struct {
	msgC chan msgAck

	broker   string
	clientID string
	topic    string

	userName string
	password string

	qos      byte
	retained bool

	batchSize int
}

type msgAck struct {
	msg kawa.Message[types.Event]
	ack func()
}

type Option func(*mqtt)

func WithBroker(broker string) Option {
	return func(m *mqtt) {
		m.broker = broker
	}
}

func WithClientID(clientID string) Option {
	return func(m *mqtt) {
		m.clientID = clientID
	}
}

func WithTopic(topic string) Option {
	return func(m *mqtt) {
		m.topic = topic
	}
}

func WithQOS(qos byte) Option {
	return func(m *mqtt) {
		m.qos = qos
	}
}

func WithRetained(retained bool) Option {
	return func(m *mqtt) {
		m.retained = retained
	}
}

func WithBatchSize(batchSize int) Option {
	return func(m *mqtt) {
		m.batchSize = batchSize
	}
}

func WithUserName(userName string) Option {
	return func(m *mqtt) {
		m.userName = userName
	}
}

func WithPassword(password string) Option {
	return func(m *mqtt) {
		m.password = password
	}
}

func New(opts ...Option) *mqtt {
	ret := &mqtt{
		msgC: make(chan msgAck),
	}

	for _, o := range opts {
		o(ret)
	}
	if ret.topic == "" {
		ret.topic = "#"
	}
	if ret.qos == 0 {
		ret.qos = 1
	}

	return ret
}

func (m *mqtt) Run(ctx context.Context) error {
	return m.recvLoop(ctx)
}

func (m *mqtt) recvLoop(ctx context.Context) error {
	// Open file to check and save high watermark
	opts := MQTT.NewClientOptions().AddBroker(m.broker).
		SetClientID(m.clientID).SetUsername(m.userName).SetPassword(m.password)
	//SetAutoReconnect(true).SetConnectRetry(true)

	client := MQTT.NewClient(opts)

	newMessage := func(client MQTT.Client, message MQTT.Message) {
		rawMsg := mqttMsg{
			Payload:   string(message.Payload()),
			Topic:     message.Topic(),
			Duplicate: message.Duplicate(),
			MessageID: message.MessageID(),
			QOS:       message.Qos(),
			Retained:  message.Retained(),
		}

		rawMsgBts, err := json.Marshal(rawMsg)
		if err != nil {
			slog.Error(fmt.Sprintf("unmarshaling %+v", err))
		}

		select {
		case m.msgC <- msgAck{
			msg: kawa.Message[types.Event]{
				Value: types.Event{
					Timestamp:  time.Now().UTC(),
					SourceType: "mqtt",
					RawLog:     rawMsgBts,
				},
				Key:   fmt.Sprintf("%d", rawMsg.MessageID),
				Topic: rawMsg.Topic,
			},
			ack: message.Ack,
		}:
		case <-ctx.Done():
			return
		}
	}

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt connect error: %s", token.Error())
	}

	token := client.Subscribe(m.topic, m.qos, newMessage)
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("mqtt subscribe error: %s", token.Error())
	}

	defer client.Unsubscribe(m.topic)
	defer client.Disconnect(250)

	for {
		select {
		case <-time.After(60 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *mqtt) Recv(ctx context.Context) (kawa.Message[types.Event], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[types.Event]{}, nil, ctx.Err()
	case pass := <-s.msgC:
		return pass.msg, pass.ack, nil
	}
}