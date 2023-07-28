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

# Motivation

I've spent the better part of the last 10 years of my career building highly
scalable event processing systems.  In that time, I've been thinking about what
my ideal system would look like, given that I came in after the initial systems
were developed.  What I found when building these systems and evaluating others
is that achiving the trifecta of simplicity, performance, and reliability is
incredibly difficult.

This framework strives to achieve all three.  I believe that plugins should be
easy to implement and I believe that we should not have to sacrafice delivery
guarantees or performance for simplicity.

# Core Concepts

## Do one thing at a time

A key decision when building this framework was to handle one event at a time.

This decision results in simplified interfaces for sources and destinations,
better delivery guarantees, flexibility in tuning paramters, and cleaner error
handling.

How is this reflected in the code?  Sources implement a `Recv` function which
returns exactly 1 event.  The handler then takes that event and processes it,
returning 0 or more events to be sent to one or more destinations.

Why is this important?  Imagine for a moment that you're handling mutliple
events in a batch.  Now imagine that you have to impelement a mechanism for
acknowledging the success or failure of processing those messages.  How would
you do it?  If an error is encountered, do you fail that event or fail the
batch?  How do you handle side-effects?

This concept is not novel.  This concept was formed as a quality control
strategy in lean manufaturing and there it is called the "one piece flow"
principle.  Toyota discovered that they could handle mistakes, errors, or
exceptions not previously considered when creating or modifying a process by
identifying an issue as soon as it arises, handle it immediately and fix the
process for the following vehicles minimizing the number of vehicles impacted
by the mistake or exception.

With event processing, the concept is the same.  We may encounter errors or
exceptions processing any arbitrary set of events.  Maybe some events are
invalid, maybe we want to ensure transactional guarantees scoped to an
individual event, or maybe some transient network or system errors make forward
progress temporarily unavailable.  All of these issues raise questions about
how to properly handle them in the context of a batch, whereas in the scope of
an individual event the programmer can make a better informed decision about
how to handle the particular issue.

## Batch at the boundries

I lied, we still ❤️ batching.  The former concept applies to the handling of
events after they're in the system.  But you'd be right to call us crazy for
throwing out batches all together.  So how do we reconcile batch processing
with the "one event at a time" principle?  By batching at the boundries.

Clearly, we'd be wasting cycles if we were to go and fetch a single message at
a time from some remote source, or delivered one event at a time to
destinations.

Computers are fast, light is slow.  [Grace
Hopper](https://www.youtube.com/watch?v=gYqF6-h9Cvg) has perhaps one of the
best lectures on the nature of relativistic speeds in the propegation of
electricity across mediums.  Because of this, any activity which leaves the
processor, or the computer itself, leads to high latency relative to what you
can do in a processor.

So we batch.  We're very particular about where and how we batch.  We batch at
the boundries.  We batch where we need to, which is often when we're leaving
the scope of the computer system we're currently running our code on, or at
perhaps when we're leaving the process.

For this framework, the batching is best handled in the sources and
destinations.  By doing this, we enable a very clear separation of concerns
between the transport of data across system boundries, and the actual work
being done to the data being processed.

You can see this pattern repeated frequently in computer science: TCP windowing
batches packets to be sent across networks.  Disk i/o is often buffered and
batch-written to disk. Message queues like Kafka adopt this paradigm.  Going
back to the manufaturing example, materials are delivered in batches, and
products are shipped out in batches despite the manufacturing line often
producing one item at a time.

The first two concepts comprise the first-principles approach to building a new
stream processing framework in Go.

## Configure and Run pattern (the CAR pattern)

A pattern we discovered that came about while developing this framework is
something we're calling the "configure and run" pattern.

Simply put, when implementing sources and destinations we first configure them,
instantiating them with their configuration and options returning a struct 
which satisfies a `Run(ctx) error` interface.

Often in Go programs, you'll see these two operations paired together into a
single method with a signature similar to something like `New(ctx, options)
struct, error`.  However, after extensive time building Go programs, I found
that it serves a very good purpose to separate these.

We want to separate the configuration of an entity from the running of that
entity to clearly separate its configuration from the lifecycle management of
whatever process it's providing.  An example to illstrate is useful here.

Say we have a service which consumes messages from a queue. Let's assume for a
moment that we want to write a new function which has a signature like:
`New(ctx, cfg) (consumer, error)`.  Let's say that this function returns a
consumer ready to be read and can be stopped by calling `consumer.Close()`.

A lot of questions arise: What's the context passed into New for? I've got a
consumer that I can get messages from, but now I've received a shutdown
request, how do I stop processing and shutdown cleanly?  What if I'm in the
middle of `consumer.Consume()` in a loop somewhere? Where do I launch my
goroutines to manage the lifecycle?

It becomes much easier to reason about these concerns when separating the
configuration from the running of a process.  By separating them, we can now
configure everything before doing any work.  We can optionally verify the
configuration of the whole system before starting by calling `Run` on each
process we've configured.  When it's time to terminate the processes, we simply
cancel the context passed into their respective Run functions.


## Handle errors as soon as reasonably possible

Error handling is difficult.  Go makes it a bit easier by forcing programmers
to consider the error (or explicitly ignore it by assigning it to underscore),
but it's still hard to know when or how to handle errors.

Based on our experience, we found (like many before us) that handling errors as
close as possible to where they're raised is the best way to build reliable
systems.  The reason is simple: the closer you are to something, the easier it
is to see it clearly and make an informed decision based on the current context
of the program.

Based on this, if a Source, Destination or Handler returns an error, a
Processor will quit.  Who are we to tell you how to handle those errors? Error
handling can't ever be prescriptive because every program has different goals.

Likewise, retries should be handled by the source, destination, or handlers.
The logic for handling is not easily generalizable, so therefore we don't try.


# Comparison to other frameworks

Kawa is not just for logs, although that's our first target.  The framework is
designed to be a general purpose stream processing framework similar to the
likes of Spark, Bethos or Dataflow.  We aim to make moving data around easy.

When comparing against other frameworks, we'll evaluate what they're good at
versus what we can do as well as what the experience of using these frameworks
either as a plugin author or end consumer (e.g. a sysadmin collecting logs).

## Vector

Written in Rust.  Vector is an ambitious project worth exploring!  Personally,
I've found it hard to implement plugins in Vector.

## Logstash

Written in Java / JRuby.  Pretty cool, but now you have the JVM in your stack.

## FluentBit

Written in C.  Steep learning curve, but highly efficient.

## Benthos

Benthos is probably the closest like-for-like framework for building abstract
stream processing jobs against many systems.

## OpenTelemetry

Well then you'd be using open telemetry.  Good luck with that.

Written in Go.  It's designed by committees of contributors from competing
organizations with their own motivations, leading to crazy interfaces and
usability issues all around.

# Disclaimer

This is alpha software, subject to breaking changes as we reach a good working
set of APIs, interfaces and data models.  Please try it out and help shape the
direction of the project by giving us feedback!

# TODO

- Ensure that consumers of kawa aren't subject to all the dependencies of kawad.
- Consider breaking apart the library from the daemon.

# Source/Destination Wishlist

Kafka
redis
NATS
amqp
pubsub
Kinesis
memcache?
zmq?
NSQ?
