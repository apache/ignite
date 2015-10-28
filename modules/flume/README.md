#FlumeNG sink and Ignite streamer

## Setting up and running

1. Extend FlumeStreamer with you transform() method implementation
2. Build it and copy to ${FLUME_HOME}/plugins.d/ignite-sink/lib
3. Copy other Ignite-related jar files to ${FLUME_HOME}/plugins.d/ignite-sink/libext to have them as shown below

```
plugins.d/
`-- ignite
    |-- lib
    |   `-- ignite-flume-example-x.x.x.jar <-- your jar
    `-- libext
        |-- cache-api-1.0.0.jar
        |-- ignite-core-x.x.x.jar
        |-- ignite-flume-x.x.x.jar
        |-- ignite-spring-x.x.x.jar
        |-- spring-aop-4.1.0.RELEASE.jar
        |-- spring-beans-4.1.0.RELEASE.jar
        |-- spring-context-4.1.0.RELEASE.jar
        |-- spring-core-4.1.0.RELEASE.jar
        `-- spring-expression-4.1.0.RELEASE.jar
```

4. Specify Ignite configuration XML file's location, cache name and your FlumeStreamer's implementation class

```
# Describe the sink
a1.sinks.k1.type = org.apache.ignite.stream.flume.IgniteSink
a1.sinks.k1.igniteCfg = /some-path/ignite.xml
a1.sinks.k1.igniteCacheName = testCache
a1.sinks.k1.igniteStreamerType = org.apache.ignite.stream.flume.MySampleStreamer
```

Now you are ready to run a Flume agent.
