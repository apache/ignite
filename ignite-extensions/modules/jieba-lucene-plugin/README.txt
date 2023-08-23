Apache Ignite Jieba Luncene Tokenize Module
-----------------------------------

IgniteSink, which can be found in 'ignite-flume-ext', and its dependencies have to be included in the agent's classpath,
as described in the following subsection, before starting the Flume agent.

## Setting up and running

1. Create a transformer by implementing EventTransformer interface.
2. Create 'ignite' directory inside plugins.d directory which is located in ${FLUME_HOME}. If the plugins.d directory is not there, create it.
3. Build it and copy to ${FLUME_HOME}/plugins.d/ignite-sink/lib.
4. Copy other Ignite-related jar files from Apache Ignite distribution to ${FLUME_HOME}/plugins.d/ignite-sink/libext to have them as shown below.

