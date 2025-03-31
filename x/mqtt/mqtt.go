// Package mqtt provides [MQTT] integrations for kawa.
//
// [MQTT]: https://mqtt.org/
package mqtt

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/runreveal/kawa"
)

// Option is an argument to [NewSource] or [NewDestination].
type Option func(*options)

type options struct {
	broker   string
	clientID string
	topic    string

	userName string
	password string

	qos       byte
	retained  bool
	keepAlive time.Duration
}

// WithBroker specifies the broker URI to connect to.
func WithBroker(broker string) Option {
	return func(opts *options) {
		opts.broker = broker
	}
}

// WithClientID specifies client ID to advertise when connecting to the MQTT broker.
// This string must be no more than 23 bytes in length.
func WithClientID(clientID string) Option {
	return func(opts *options) {
		opts.clientID = clientID
	}
}

// WithTopic specifies the topic to publish or subscribe to.
// By default, this is "#".
func WithTopic(topic string) Option {
	return func(opts *options) {
		if topic == "" {
			opts.topic = "#"
		} else {
			opts.topic = topic
		}
	}
}

// WithKeepAlive sets the amount of time before sending a PING message to the broker.
func WithKeepAlive(keepAlive time.Duration) Option {
	return func(opts *options) {
		opts.keepAlive = keepAlive
	}
}

// WithQOS sets the quality-of-service (QoS) option for publishing or subscribing.
func WithQOS(qos byte) Option {
	return func(opts *options) {
		opts.qos = qos
	}
}

// WithRetained sets whether messages sent to the topic will have the retained bit set.
// WithRetained is only applicable for [NewDestination].
func WithRetained(retained bool) Option {
	return func(opts *options) {
		opts.retained = retained
	}
}

// WithUserName sets the username to authenticate to the broker with.
func WithUserName(userName string) Option {
	return func(opts *options) {
		opts.userName = userName
	}
}

// WithPassword sets the password to authenticate to the broker with.
func WithPassword(password string) Option {
	return func(opts *options) {
		opts.password = password
	}
}

// Destination is a [kawa.Destination] that publishes to an MQTT topic.
type Destination struct {
	client MQTT.Client
	cfg    options
	errc   chan error
}

type msgAck struct {
	msg kawa.Message[[]byte]
	ack func()
}

func loadOpts(opts []Option) options {
	cfg := options{
		topic:    "#",
		retained: false,
		qos:      1,
	}

	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// NewDestination returns a new [Destination] with the given options.
// NewDestination returns an error if the caller does not provide [WithBroker] or [WithClientID].
func NewDestination(opts ...Option) (*Destination, error) {
	cfg := loadOpts(opts)
	ret := &Destination{
		cfg:  cfg,
		errc: make(chan error),
	}

	connLost := func(client MQTT.Client, err error) {
		ret.errc <- err
	}

	var err error
	ret.client, err = clientConnect(cfg, connLost)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func clientConnect(opts options, onLost MQTT.ConnectionLostHandler) (MQTT.Client, error) {
	if opts.broker == "" {
		return nil, errors.New("mqtt: missing broker")
	}
	if opts.clientID == "" {
		return nil, errors.New("mqtt: missing clientID")
	}

	clientOpts := MQTT.NewClientOptions().
		AddBroker(opts.broker).
		SetClientID(opts.clientID).
		SetConnectionLostHandler(onLost).
		SetKeepAlive(opts.keepAlive)

	if opts.userName != "" {
		clientOpts = clientOpts.SetUsername(opts.userName)
	}
	if opts.password != "" {
		clientOpts = clientOpts.SetPassword(opts.password)
	}

	fmt.Printf("Opts: %+v\n", clientOpts)
	client := MQTT.NewClient(clientOpts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("mqtt connect error: %s", token.Error())
	}

	return client, nil
}

// Run waits until ctx.Done() is closed or the MQTT connection is lost,
// then disconnects after a grace period.
func (dest *Destination) Run(ctx context.Context) error {
	var err error
	select {
	case err = <-dest.errc:
	case <-ctx.Done():
		err = ctx.Err()
	}
	dest.client.Disconnect(1000)
	return err
}

// Send publishes each message in the batch to the configured topic.
func (dest *Destination) Send(ctx context.Context, ack func(), msgs ...kawa.Message[[]byte]) error {
	for _, msg := range msgs {
		token := dest.client.Publish(dest.cfg.topic, dest.cfg.qos, dest.cfg.retained, string(msg.Value))
		token.Wait()
		if token.Error() != nil {
			return token.Error()
		}
	}
	kawa.Ack(ack)
	return nil
}

// Source is a [kawa.Source] that subscribes to an MQTT topic.
type Source struct {
	msgC   chan msgAck
	cfg    options
	errc   chan error
	client MQTT.Client
}

// NewSource returns a new [Source] with the given options.
// NewSource returns an error if the caller does not provide [WithBroker] or [WithClientID].
//
// The caller is responsible for calling [*Source.Run]
// or else [*Source.Recv] will block indefinitely.
func NewSource(opts ...Option) (*Source, error) {
	cfg := loadOpts(opts)

	ret := &Source{
		msgC: make(chan msgAck),
		cfg:  cfg,
		errc: make(chan error),
	}

	connLost := func(client MQTT.Client, err error) {
		ret.errc <- err
	}

	var err error
	ret.client, err = clientConnect(cfg, connLost)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// Run receives messages until ctx.Done() is closed.
func (src *Source) Run(ctx context.Context) error {
	return src.recvLoop(ctx)
}

func (src *Source) recvLoop(ctx context.Context) error {
	newMessage := func(client MQTT.Client, message MQTT.Message) {
		select {
		case src.msgC <- msgAck{
			msg: kawa.Message[[]byte]{
				Value: message.Payload(),
				Key:   strconv.FormatUint(uint64(message.MessageID()), 10),
				Topic: message.Topic(),
			},
			ack: message.Ack,
		}:
		case <-ctx.Done():
			return
		}
	}

	token := src.client.Subscribe(src.cfg.topic, src.cfg.qos, newMessage)
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("mqtt subscribe error: %s", token.Error())
	}

	defer func() {
		src.client.Unsubscribe(src.cfg.topic)
		src.client.Disconnect(250)
	}()

	select {
	case err := <-src.errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Recv waits for the next message from the topic.
//
// Note that Recv will block indefinitely unless [*Source.Run] is active.
func (src *Source) Recv(ctx context.Context) (kawa.Message[[]byte], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[[]byte]{}, nil, ctx.Err()
	case pass := <-src.msgC:
		return pass.msg, pass.ack, nil
	}
}
