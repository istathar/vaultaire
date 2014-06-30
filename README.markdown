A data vault for metrics
========================

> <span style="font-family: 'Times New Roman',serif; font-style: italic;
> font-size: 22px;">&ldquo;I may not agree with your methodology, but I'll
> store to capacity the data you apply it to.&rdquo;</span>
>
>  <span style="padding-left:100px">&nbsp;</span> —Vaultaire, 1770 (apocryphal)

Basis for analytics
-------------------

Most systems that store server metrics are lossy: they average data out as they
compact them in order to save space. If you're planning on doing serious
analytics or predictive modelling, however, you can't be throwing away data
points. We wanted a place to put metrics that wasn't lossy, allows arbitrary
range querying, and would scale out without bothering anybody.

Vaultaire is a data vault for system metrics, backed onto Ceph. We use it
internally to store system metrics and to derive IP traffic usage for billing.
We're looking forward to letting clients write to it as well.

Design
------

Vaultaire is a fault-tolerant, distributed, scalable system. A few key
architectural decisions enable this:

 * Data points are immutable. Once you've written a metric you don't go changing
it. If a business decision is made to value that data differently, then that's a
job for analytics later.
 
 * Writes operations are idempotent. Under the hood we _append_ every time we
write, and do de-duplication when we read. If you aren't trying to to update or
sort in time order when saving points, the on disk data structures become very
simple indeed. And with idempotent operations, it means that should a client not
receive an acknowledgment of write it can re-send that point.
 
 * No state is held by daemons. Vaultaire has quite a number of moving parts,
but all of them can be multiply provisioned and none of them carry any state, so
a single failure does not take down the system. Scaling under load can be done
by adding more daemons horizontally. The only place state is held is in the Ceph
cluster (which, by definition, is consistent and redundant).

Implementation
--------------

This is the second major release of Vaultaire. "v2" writes directly to our
production Ceph cluster (via [librados][]). The server daemons ([vaultaire][])
are written in Haskell, with client libraries available in Haskell
([marquise][]) and C ([libmarquise][]). There's a search index ([chevalier][])
backed by ElasticSearch, and a visualization pipeline where RESTful access to
interpolated data points ([sieste][]) is provided by Haskell to a beautiful
Ruby/JavaScript graphing front-end ([machiavelli][]) that allows you to
correlate and view data streams.




[librados]: https://ceph.com/docs/master/architecture/
[vaultaire]: https://github.com/anchor/vaultaire
[marquise]: https://github.com/anchor/marquise
[libmarquise]: https://github.com/anchor/libmarquise
[chevalier]: https://github.com/anchor/chevalier
[sieste]: https://github.com/anchor/sieste
[machiavelli]: http://anchor.github.io/machiavelli/


