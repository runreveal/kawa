# kawa

<!-- The space below the div closing tag is necessary -->
<div align=center>
  <div>
    <img src="docs/static/kawa.png" width="300px">
    &nbsp;&nbsp;
  </div>
  <div>
    <a href="https://pkg.go.dev/github.com/runreveal/kawa"><img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/runreveal/kawa.svg"/></a>
    <a href="https://github.com/runreveal/kawa/actions/workflows/ci.yml"><img alt="GoFrame CI" src="https://github.com/runreveal/kawa/actions/workflows/ci.yml/badge.svg"/></a>
    <a href="https://goreportcard.com/report/github.com/runreveal/kawa"><img alt="Go Report Card" src="https://goreportcard.com/badge/github.com/runreveal/kawa"/></a>
    <a href="https://github.com/runreveal/kawa"><img alt="License" src="https://img.shields.io/github/license/runreveal/kawa.svg?style=flat"/></a>
  </div>
</div>

---

kawa ("Kaa-Wah") is an opinionated framework for scalable, reliable stream processing.

kawad ("Kaa-Wah-Dee") is a daemon for collecting system logs and metrics.

# Installation

## Kawad

Find the package for your OS and architecture on the releases page. Download
that file to the machine, and install somewhere visible on your $path.

    curl -L https://github.com/runreveal/kawa/releases/download/v0.0.2/kawa-linux-amd64.tar.gz | sudo tar --directory /usr/local/bin -xz

Copy an example config from the examples/ directory, then run it!  There is
also an example for deploying as a systemd service.  Additionally, we'll have
kubernetes examples soon.

## Kawa

Add the library to your project as you would any other Go library:

```go
go get -u github.com/runreveal/kawa
```

# Design and Rationale

See https://blog.runreveal.com/kawa-the-event-processor-for-the-grug-brained-developer/

# Roadmap

- Ensure that consumers of kawa aren't subject to all the dependencies of the
  kawa program.
- Related: consider breaking apart the library from the daemon.
- Event Routing and/or Multiple Processors in kawa program
- Dynamic Sources (e.g. Kafka Consumer Groups)

# Disclaimer

This is nascent software, subject to breaking changes as we reach a good
working set of APIs, interfaces and data models.  Please try it out and help
shape the direction of the project by giving us feedback!

# Getting started using Kawad

An example use case might be shipping your nginx logs to s3. Save the following
config.json, and fill in the config file.

```
{
  "sources": [
    {
      "type": "syslog",
      "addr": "0.0.0.0:5514",
      "contentType": "application/json; rrtype=nginx-json",
    },
  ],
  "destinations": [
    {
      "type": "s3",
      "bucketName": "{{YOUR-S3-BUCKET-NAME}}",
      "bucketRegion": "us-east-2",
    },
  ],
}
```

Next, add the following line to your nginx server config.

```
server {
    access_log syslog:server=127.0.0.1:5514;
    # ... other config ...
}
```

Run it!

```
$ kawa run --config config.json
```

# Development & Extension

The source and destination interfaces are designed for simplicity of use and
implementation.  It should be easy to use the sources and destinations in an
abstract manner without knowing the underlying implementation, and it should
be relatively easy to implement sources and destinations of various types.

The library provides multiple abstractions suitable for different purposes.

The easiest way to get started is by using the polling/batch implementations
for sources/destinations, respectively, since they require less overhead in
terms of accounting for offset tracking to ensure at-least-once reliable
processing.

Extensions under the `x` package provide either generic or `[]byte` based
sources, destinations and utility functions which aren't part of the core
functionality of kawa.  They're provided for re-use in other applications.

To use them, import them into your program and apply the proper serialization
techniques relevant to your application.  See examples of this in practice in
the `cmd/kawad/internal` package, where we use it for system logs.

## Configure and Run Design Pattern

The "Configure and Run" pattern is a pattern discovered while writing this
framework that works nicely with other patterns and libraries in Go.

The general idea is as follows.  Each struct maintaining long running goroutines
can be made easy to reason about and operate by splitting it's configuration and
runtime into two separate stages.

The first stage, "Configure", is simply the initialization of the struct.  Most
often, this is the New function for the struct, with required arguments passed
in first, and options passed in as a variadic functional options slice
afterwards.  Occasionally, this may involve also implementing a translation
layer for serialization of the struct from JSON or some other serialization
format (see cmd/kawad/config.go for an example of this pattern).

The next stage, "Run", involves implementing the simple Runner interface:

```golang
type Runner interface {
	Run(ctx context.Context) error
}
```

Implementing this interface consistently across all instances of structs that
have long-running processes means that we can easily implement cancellation
across a broad number of distinct types via a common context variable.

It also means that any goroutine can trigger a shutdown by returning an error
from the Run routine.

While not absolutely required, following this pattern will enable the source or
destination to be seamlessly integrated into the daemon.

## Implementing Sources

Sources are things that you read message-oriented data from.  At the most basic
level, it's a collection of bytes that together represent some discrete event.

### Polling Source

We recommend implementing polling sources when querying an API, or whenever
it's easiest to implement a function to periodically get called.  The following
is the interface which needs to be satisfied to implement a polling source.

```golang
type Poller[T any] interface {
	Poll(context.Context, int) ([]kawa.Message[T], func(), error)
}
```

### Streaming Source

We recommend implementing streaming sources when the source either implements
it's own batching semantics (like Kafka), or when message latency is more
important than message volume.

## Implementing Destinations

Destinations are things that you write message-oriented data to.

### Batch Destination

Implementing a batch destination is the easiest way to process messages as a
batch being written to some persistent storage.  It handles timeouts, batch size,
and parallel writes at the configuration level so destinations only have to implement
a single method "Flush".

```golang
type Flusher[T any] interface {
	Flush(context.Context, []kawa.Message[T]) error
}
```

### Streaming Destination

We recommend implementing streaming destinations when the destination either
implements it's own batching semantics (like Kafka), or when message latency is
more important than message volume.

# Supported sources

 - syslog
 - scanner
 - journald

# Supported destinations

 - s3 / r2
 - printer
 - runreveal

# Configuring the Daemon

## Source Configuration

### syslog

With the syslog config, and address and content type can be set.

```
{
    "type":"syslog",
    "addr":"0.0.0.0:5514",
}
```

### journald

Journald has no configuration, just set the type and kawa will read from journald.
```
{
    "type":"journald"
}
```

### scanner

Read from stdin. Useful for testing or doing something you probably shouldn't.

```
{
    "type":"scanner",
}
```


## Destination Configuration

### RunReveal

WebhookURL is the only config argument and it is required.

```
{
    "type":"runreveal",
    "webhookURL": "https://api.runreveal.com/....."
}
```

### S3

The s3 destination is compatible with s3 and other s3 compatible interfaces. By default the s3 destination will pull credentials from the standard places the aws sdk looks, but they can optionally be set in the configuration.

customEndpoint must be set for custom destinations, and in that case bucketRegion probably will not be set. bucketName is the only required argument.

For high volume or low volume, the batchSize can be tweaked but is set to 100 by default.

```
{
    "type":"s3",
    "bucketName":"my-cool-log-bucket",
    "bucketRegion":"us-east-2",
    "batchSize":1000,
}
```

### Printer

Printer will print the results to stdout. Useful for testing and development.
```
{
    "type":"printer",
}
```

## Source / Destination Wishlist

 - Kafka
 - redis
 - NATS
 - amqp
 - pubsub
 - Kinesis
 - memcache?
 - zmq?
 - NSQ?
