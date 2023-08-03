# kawa

<!-- The space below the div is necessary -->
<div align=center>

![Kawa](docs/static/kawa.png)

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

# Source Wishlist

Kafka
redis
NATS
amqp
pubsub
Kinesis
