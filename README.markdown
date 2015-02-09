# A data vault for metrics

> <span style="font-family: 'Times New Roman',serif; font-style: italic;
> font-size: 22px;">&ldquo;I may not agree with your methodology, but I'll
> store to capacity the data you apply it to.&rdquo;</span>
>
>  <span style="padding-left:100px">&nbsp;</span> —Vaultaire, 1770 (apocryphal)

# Basis for analytics

Most systems that store server metrics are lossy: they average data
out as they compact them in order to save space. If you're planning on
doing serious analytics or predictive modelling, however, you can't be
throwing away data points. We wanted a place to put metrics that wasn't
lossy, allows arbitrary range querying, and would scale out without
bothering anybody.

Vaultaire is a data vault for system metrics, backed onto Ceph. We use it
internally to store systems metrics from Nagios, OpenStack and pmacct for
problem diagnosis, abnormality detection, metering/billing, forecasting
and capacity planning. We're looking forward to letting clients write to
it as well.

# Building

## System dependencies

 - [librados](http://ceph.com)
 - [zeromq 4+](http://zeromq.org/)
 - [GHC 7.8.3+](https://www.haskell.org/ghc/)
 - [cabal](https://www.haskell.org/cabal/)

## Common packages

 - [vaultaire-common](https://github.com/anchor/vaultaire-common)
 - [marquise](https://github.com/anchor/marquise)

## Build

```
cabal sandbox init
cabal sandbox add-source ../vaultaire-common
cabal sandbox add-source ../marquise
cabal install
```

You'll need to add the sandbox's `bin` directory to your `$PATH`, or
else replace the `vault` command with a fully-qualified path.

# Running

You need to run four components: the `broker`, plus one or more of each
of the `writer`, `reader` and `contents` daemons. These can be on the
same or different hosts, but they must all point to the same broker (if
you want to make the broker highly-available, use a TCP load-balancer).

The defaults assume your Rados pool is called `vaultaire`, and you're
connecting with the username `vaultaire`; if this isn't the case,
override with `-p` and `-u` respectively. In either case, you'll need to
have the environment variable `CEPH_KEYRING` pointing to the correct
place.

```
vault -d broker # Start broker in debug mode
vault -d writer -b localhost # Start writer in debug mode
vault -d reader -b localhost # Start reader in debug mode
vault -d contents -b localhost # Start contents daemon in debug mode
```

# Ecosystem

Vaultaire on its own only provides a binary interface over ZeroMQ. To
fill this gap, there's a standard ecosystem of tools to provide various
interfaces:

 - [Chevalier](https://github.com/anchor/chevalier) - datasource search
   engine backed to Elasticsearch.
 - [Sieste](https://github.com/anchor/sieste) - RESTful web interface to
   datapoints and metadata (talks to Vaultaire and Chevalier).
 - [Marquise](https://github.com/anchor/marquise) - client library for
   reading and writing to Vaultaire, and the `marquised` daemon for
   transmitting queued datapoints.
 - [libmarquise](https://github.com/anchor/libmarquise) - C client
   library for writing datapoints, for situations where using the
   Haskell package is unsuitable.
 - [vaultaire-query](https://github.com/anchor/vaultaire-query) -
   analytics-focused query DSL.
 - [Machiavelli](https://github.com/anchor/machiavelli) - graphing
   engine with support for using Sieste as a backend.

# Design

Vaultaire is a fault-tolerant, distributed, scalable system. A few key
architectural decisions enable this:

 * Data points are immutable. Once you've written a metric you don't go
   changing it. If a business decision is made to value that data
   differently, then that's a job for analytics later.
 
 * Writes operations are idempotent. Under the hood we _append_ every
   time we write, and do de-duplication when we read. If you aren't
   trying to to update or sort in time order when saving points, the on
   disk data structures become very simple indeed. And with idempotent
   operations, it means that should a client not receive an
   acknowledgment of write it can re-send that point.
 
 * No state is held by daemons. Vaultaire has quite a number of moving
   parts, but all of them can be multiply provisioned and none of them
   carry any state, so a single failure does not take down the system.
   Scaling under load can be done by adding more daemons horizontally.
   The only place state is held is in the Ceph cluster (which, by
   definition, is consistent and redundant).

# Implementation

This is the second major release of Vaultaire. "v2" writes directly to our
production Ceph cluster (via [librados][]). The server daemons ([vaultaire][])
are written in Haskell, with client libraries available in Haskell
([marquise][]) and C ([libmarquise][]). There's a search index ([chevalier][])
backed by ElasticSearch, and a visualization pipeline where RESTful access to
interpolated data points ([sieste][]) is provided by Haskell to a beautiful
Ruby/JavaScript graphing front-end ([machiavelli][]) that allows you to
correlate and view data streams. The [vaultaire-query][] library
provides a Haskell-based DSL for analytics applications.

[librados]: https://ceph.com/docs/master/architecture/
[vaultaire]: https://github.com/anchor/vaultaire
[marquise]: https://github.com/anchor/marquise
[libmarquise]: https://github.com/anchor/libmarquise
[chevalier]: https://github.com/anchor/chevalier
[sieste]: https://github.com/anchor/sieste
[machiavelli]: http://anchor.github.io/machiavelli/
[vaultaire-query]: http://github.com/anchor/vaultaire-query
