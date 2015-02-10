# 2.6.1

## User interface
To disable profiling, run ``vault --no-profiling``. By default,
profiling is enabled with a period of 1s and an accuracy bound of 2048
telemetric stats per second.

# 2.6.0

## User interface

To enable profiling when running `vault`, use the flag `'--profiling'`.
Optionally, you can also specify:

  * The profiling period with `-period <number of milliseconds>`.
  * The name of the daemon, with `-n <name>`, for easy reading of
    telemetric reports.

## Internal changes

* The broker ``String`` is replaced by a ``URI`` type from the
  ``network-uri`` package. It does validation and allows easy
  manipulation of URI strings.
* Most functions that previously take a broker string, a Ceph user, a
  Ceph pool and a signal now take those things in a ``DaemonArgs`` sum
  type, which also includes a profiling interface. Use the smart
  constructor ``daemonArgs`` to create this sum type.
