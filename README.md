# kawa

kawa is an opinionated framework for scalable, reliable stream processing.

kawa also provides a daemon for collecting system logs and metrics.

# Installation

Find the package for your OS and architecture on the releases page. Download
that file to the machine, and install somewhere visible on your $path.

curl -L https://github.com/runreveal/kawa/releases/download/v0.0.2/kawa-linux-amd64.tar.gz | sudo tar --directory /usr/local/bin -xz

Copy an example config from the examples/ directory, then run it!  There is
also an example for deploying as a systemd service.  Additionally, we'll have
kubernetes examples soon.

# Disclaimer

This is nascent software, subject to breaking changes as we reach a good
working set of APIs, interfaces and data models.  Please try it out and help
shape the direction of the project by giving us feedback!

# TODO

- Ensure that consumers of kawa aren't subject to all the dependencies of kawad.
- Consider breaking apart the library from the daemon.

# Source Wishlist

Kafka
redis
NATS
amqp
pubsub
Kinesis
memcache?
zmq?
NSQ?
