<center>
![GridGain Logo](http://www.gridgain.com/images/logo/logo_mid.png "GridGain Logo")
</center>

<div style="height: 40px"></div>

## Table Of Contents
* Building the Client Library
* Running the Client Examples
* Configuring Logging

## Building the Client Library

To build the C++ Client library, please follow the instructions in `GRIDGAIN_HOME/modules/clients/cpp/readme.md` file.

## Running the Client Examples

First, build the examples by going into the `GRIDGAIN_HOME/examples/cpp` directory and executing the following commands:

    ./configure
    make

On Windows you can use the provided Visual Studio solution (in the `vsproject/` directory) to compile the examples code.
<br/><br/>
Before you can run the examples, you must ensure that the org.gridgain.examples.client package are available on the GridGain node's classpath. One way to achieve that is to buildthe jar containing the examples code and drop it to the `GRIDGAIN_HOME/libs/ext` directory. When the `ggstart` script runs it automatically picks up the jars from that directory.
<br/><br/>
Now you can start a GridGain node as follows:

    $GRIDGAIN_HOME/bin/ggstart.{sh,bat} examples/config/example-cache.xml

Alternatively you can run an instance of `GridClientExampleNodeStartup` or `GridClientCacheExampleNodeStartup` Java class, which will start up a GridGain node with proper configuration. You can do this from your favourite IDE.
<br/><br/>
Once the GridGain node is up and running you can run the examples:

    ./gridgain-compute-example
    ./gridgain-data-example

If you just compiled the client library make sure that you ran `ldconfig` to update library cache before running the example.
<br/><br/>
You can also start multiple nodes.
<br/><br/>
Feel free to modify the example source code to change communication protocol from TCP (default) to HTTP, use multiple hosts and try other options.

## Configuring Logging

You can control the client's log level by setting the `GRIDGAIN_CPP_CLIENT_LOG_LEVEL` environment variable to one of the following values:

1. (error),
2. (warning),
3. (info), and
4. (debug).

By default, the log level is 3. The client sends all its log messages to stdout.
