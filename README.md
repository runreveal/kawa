# kawa

<!-- The space below the div is necessary -->
<div align=center>

<img src="docs/static/kawa.png" width="300px">

[![Go Reference](https://pkg.go.dev/badge/github.com/runreveal/kawa.svg)](https://pkg.go.dev/github.com/runreveal/kawa)
[![GoFrame CI](https://github.com/runreveal/kawa/actions/workflows/ci.yml/badge.svg)](https://github.com/runreveal/kawa/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/runreveal/kawa)](https://goreportcard.com/report/github.com/runreveal/kawa)
[![License](https://img.shields.io/github/license/runreveal/kawa.svg?style=flat)](https://github.com/runreveal/kawa)
</div>

kawa is an opinionated framework for scalable, reliable stream processing.

kawa also provides a daemon for collecting system logs and metrics.

# Installation

Find the package for your OS and architecture on the releases page. Download
that file to the machine, and install somewhere visible on your $path.

    curl -L https://github.com/runreveal/kawa/releases/download/v0.0.2/kawa-linux-amd64.tar.gz | sudo tar --directory /usr/local/bin -xz

Copy an example config from the examples/ directory, then run it!  There is
also an example for deploying as a systemd service.  Additionally, we'll have
kubernetes examples soon.

# Design and Rationale

See https://blog.runreveal.com/insert-blog-here

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

# Getting started

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


# Supported sources
 - syslog
 - scanner
 - journald

# Supported destinations
 - s3 / r2
 - printer
 - runreveal

# Source Configuration
## syslog
With the syslog config, and address and content type can be set.
```
{
    "type":"syslog",
    "addr":"0.0.0.0:5514",
}
```

## journald
Journald has no configuration, just set the type and kawa will read from journald.
```
{
    "type":"journald"
}
```

## scanner
Read from stdin. Useful for testing or doing something you probably shouldn't.
```
{
    "type":"scanner",
}
```


# Destination Configuration
## RunReveal
WebhookURL is the only config argument and it is required.
```
{
    "type":"runreveal",
    "webhookURL": "https://api.runreveal.com/....."
}
```

## S3
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

## Printer
Printer will print the results to stdout. Useful for testing and development.
```
{
    "type":"printer",
}
```

# Source / Destination Wishlist
 - Kafka
 - redis
 - NATS
 - amqp
 - pubsub
 - Kinesis
 - memcache?
 - zmq?
 - NSQ?
