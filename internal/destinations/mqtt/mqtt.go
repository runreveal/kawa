package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/types"
	batch "github.com/runreveal/kawa/x/batcher"
)

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
		if topic == "" {
			m.topic = "#"
		} else {
			m.topic = topic
		}
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
		if batchSize > 0 {
			m.batchSize = batchSize
		} else {
			m.batchSize = 100
		}

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

type mqtt struct {
	batcher *batch.Destination[types.Event]
	client  MQTT.Client

	broker   string
	clientID string
	topic    string

	userName string
	password string

	qos      byte
	retained bool

	batchSize int
}

func New(opts ...Option) *mqtt {
	ret := &mqtt{}
	for _, o := range opts {
		o(ret)
	}

	ret.batcher = batch.NewDestination[types.Event](ret,
		batch.FlushLength(ret.batchSize),
		batch.FlushFrequency(5*time.Second),
	)
	return ret
}

func (m *mqtt) Run(ctx context.Context) error {
	if m.broker == "" {
		return errors.New("missing broker")
	}
	if m.clientID == "" {
		return errors.New("missing clientID")
	}

	opts := MQTT.NewClientOptions().AddBroker(m.broker).
		SetClientID(m.clientID).SetUsername(m.userName).SetPassword(m.password)
	client := MQTT.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	m.client = client

	return m.batcher.Run(ctx)
}

func (m *mqtt) Send(ctx context.Context, ack func(), msgs ...kawa.Message[types.Event]) error {
	return m.batcher.Send(ctx, ack, msgs...)
}

// Flush sends the given messages of type kawa.Message[type.Event] to an MQTT topic
func (m *mqtt) Flush(ctx context.Context, msgs []kawa.Message[types.Event]) error {

	for _, msg := range msgs {
		jsonData, err := json.Marshal(msg.Value)
		if err != nil {
			return err
		}

		token := m.client.Publish(m.topic, m.qos, m.retained, jsonData)
		token.Wait()
		if token.Error() != nil {
			return token.Error()
		}
	}
	return nil
}
