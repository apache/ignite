/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.stream.storm;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 * Apache Storm streamer implemented as a Storm bolt.
 * Obtaining data from other bolts and spouts is done by the specified tuple field.
 */
public class StormStreamer<K, V> extends StreamAdapter<Tuple, K, V> implements IRichBolt {
    /** Default flush frequency. */
    private static final long DFLT_FLUSH_FREQ = 10000L;

    /** Default Ignite tuple field. */
    private static final String DFLT_TUPLE_FIELD = "ignite";

    /** Logger. */
    private IgniteLogger log;

    /** Field by which tuple data is obtained in topology. */
    private String igniteTupleField = DFLT_TUPLE_FIELD;

    /** Automatic flush frequency. */
    private long autoFlushFrequency = DFLT_FLUSH_FREQ;

    /** Enables overwriting existing values in cache. */
    private boolean allowOverwrite = false;

    /** Flag for stopped state. */
    private static volatile boolean stopped = true;

    /** Storm output collector. */
    private OutputCollector collector;

    /** Ignite grid configuration file. */
    private static String igniteConfigFile;

    /** Cache name. */
    private static String cacheName;

    /**
     * Gets Ignite tuple field, by which tuple data is obtained in topology.
     *
     * @return Tuple field.
     */
    public String getIgniteTupleField() {
        return igniteTupleField;
    }

    /**
     * Names Ignite tuple field, by which tuple data is obtained in topology.
     *
     * @param igniteTupleField Name of tuple field.
     */
    public void setIgniteTupleField(String igniteTupleField) {
        this.igniteTupleField = igniteTupleField;
    }

    /**
     * Gets the cache name.
     *
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Sets the cache name.
     *
     * @param cacheName Cache name.
     */
    public void setCacheName(String cacheName) {
        StormStreamer.cacheName = cacheName;
    }

    /**
     * Gets Ignite configuration file.
     *
     * @return Configuration file.
     */
    public String getIgniteConfigFile() {
        return igniteConfigFile;
    }

    /**
     * Specifies Ignite configuration file.
     *
     * @param igniteConfigFile Ignite config file.
     */
    public void setIgniteConfigFile(String igniteConfigFile) {
        StormStreamer.igniteConfigFile = igniteConfigFile;
    }

    /**
     * Obtains data flush frequency.
     *
     * @return Flush frequency.
     */
    public long getAutoFlushFrequency() {
        return autoFlushFrequency;
    }

    /**
     * Specifies data flush frequency into the grid.
     *
     * @param autoFlushFrequency Flush frequency.
     */
    public void setAutoFlushFrequency(long autoFlushFrequency) {
        this.autoFlushFrequency = autoFlushFrequency;
    }

    /**
     * Obtains flag for enabling overwriting existing values in cache.
     *
     * @return True if overwriting is allowed, false otherwise.
     */
    public boolean getAllowOverwrite() {
        return allowOverwrite;
    }

    /**
     * Enables overwriting existing values in cache.
     *
     * @param allowOverwrite Flag value.
     */
    public void setAllowOverwrite(boolean allowOverwrite) {
        this.allowOverwrite = allowOverwrite;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    @SuppressWarnings("unchecked")
    public void start() throws IgniteException {
        A.notNull(igniteConfigFile, "Ignite config file");
        A.notNull(cacheName, "Cache name");
        A.notNull(igniteTupleField, "Ignite tuple field");

        setIgnite(StreamerContext.getIgnite());

        final IgniteDataStreamer<K, V> dataStreamer = StreamerContext.getStreamer();
        dataStreamer.autoFlushFrequency(autoFlushFrequency);
        dataStreamer.allowOverwrite(allowOverwrite);

        setStreamer(dataStreamer);

        log = getIgnite().log();

        stopped = false;
    }

    /**
     * Stops streamer.
     *
     * @throws IgniteException If failed.
     */
    public void stop() throws IgniteException {
        if (stopped)
            return;

        stopped = true;

        getIgnite().<K, V>dataStreamer(cacheName).close(true);

        getIgnite().close();
    }

    /**
     * Initializes Ignite client instance from a configuration file and declares the output collector of the bolt.
     *
     * @param map Map derived from topology.
     * @param topologyContext Context topology in storm.
     * @param collector Output collector.
     */
    @Override public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        start();

        this.collector = collector;
    }

    /**
     * Transfers data into grid.
     *
     * @param tuple Storm tuple.
     */
    @SuppressWarnings("unchecked")
    @Override public void execute(Tuple tuple) {
        if (stopped)
            return;

        if (!(tuple.getValueByField(igniteTupleField) instanceof Map))
            throw new IgniteException("Map as a streamer input is expected!");

        final Map<K, V> gridVals = (Map<K, V>)tuple.getValueByField(igniteTupleField);

        try {
            if (log.isDebugEnabled())
                log.debug("Tuple (id:" + tuple.getMessageId() + ") from storm: " + gridVals);

            getStreamer().addData(gridVals);

            collector.ack(tuple);
        }
        catch (Exception e) {
            log.error("Error while processing tuple of " + gridVals, e);
            collector.fail(tuple);
        }
    }

    /**
     * Cleans up the streamer when the bolt is going to shutdown.
     */
    @Override public void cleanup() {
        stop();
    }

    /**
     * Normally declares output fields for the stream of the topology.
     * Empty because we have no tuples for any further processing.
     *
     * @param declarer OutputFieldsDeclarer.
     */
    @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No-op.
    }

    /**
     * Not used.
     *
     * @return Configurations.
     */
    @Override public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * Streamer context initializing grid and data streamer instances on demand.
     */
    public static class StreamerContext {
        /** Constructor. */
        private StreamerContext() {
        }

        /** Instance holder. */
        private static class Holder {
            private static final Ignite IGNITE = Ignition.start(igniteConfigFile);

            private static final IgniteDataStreamer STREAMER = IGNITE.dataStreamer(cacheName);
        }

        /**
         * Obtains grid instance.
         *
         * @return Grid instance.
         */
        public static Ignite getIgnite() {
            return Holder.IGNITE;
        }

        /**
         * Obtains data streamer instance.
         *
         * @return Data streamer instance.
         */
        public static IgniteDataStreamer getStreamer() {
            return Holder.STREAMER;
        }
    }
}
