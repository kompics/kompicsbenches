Running the Benchmarks
======================

Parameter Space
---------------

The experiments have two options for parameter spaces:
	
1. The "full" parameter space is targeted at machines with 30-50 CPU cores, and is the default setting.
2. The "testing" parameter space is targeted as machines with 2-8 CPU cores, and can be selected by passing `--testing true` to `./bench.sc` in one of the execution modes listed below.

Config
------

- Check the `javaOpts` in `benchmarks.sc` and make sure the the `-Xmx` matches available memory on your system, so that the experiments do not start paging. Divide the memory between the number of nodes you are planning to run in `fakeRemote`!
- If running `remote`, set `masterAddr` in `bench.sc` to an IP address (and port) that all nodes in `nodes.conf` can reach.

Local Execution
---------------

Run `./bench.sc local` to only run the benchmarks that execute within a single process.

Local Distributed Execution
---------------------------

Run `./bench.sc fakeRemote` to run all benchmarks on the local machine, by starting a configurable number (`--withClients`) of clients in temporary folders.

Remote Distributed Execution
----------------------------

Write all client nodes into `nodes.conf`. Make sure the master can SSH into them without a password, and they can communicate over TCP on all ports.
Then run `./bench.sc remote` to run all benchmarks over the configured set of nodes.

Other Options
-------------

- A subset of implementations `X,Y,Z` can be selected by adding `--impls X,Y,Z` to the `./bench.sc` command.
- A subset of benchmarks `X,Y,Z` can be selected by adding `--benchmarks X,Y,Z` to the `./bench.sc` command.

Plotting
--------

Results can be automatically plotted by running `./plot.sc`. It requires GNUPlot to be installed.
