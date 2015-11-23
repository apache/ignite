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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;

/**
 * Server for managing stream Apache Storm. This is a Bolt storm the interact with  Apache Ignite. For a description of
 * the design and the way of use, see the page of the form. How to use streamer inside <a
 * href="https://cwiki.apache.org/confluence/display/IGNITE/Streamers+Implementation+Guidelines"> Streamer
 * Implementations Guidelines</a> For this stream we have the particular conditions of execution that may not reflect
 * the ordinary use of the stream in ignite. Documentation is provided at: <a href="https://cwiki.apache.org/confluence/display/IGNITE/Storm+Integration+Guidelines">Storm
 * Stream in Ignite </a>
 */
public class StormStreamer<T, K, V> extends StreamAdapter<T, K, V> implements IRichBolt {
    /** Logger. */
    private IgniteLogger log;

    /** Executor used to submit storm streams. */
    private ExecutorService executor;

    /** Number of threads to process Storm streams. */
    private int threads = 1;

    /** Stopped. */
    private volatile boolean stopped = true;

    /** the storm output collector */
    private OutputCollector collector;

    /** define a configuration file */
    private String configurationfile = "modules/storm/src/test/resources/example-ignite.xml"; // TODO: should be set at cfg.

    /** define the cache name */
    private String cacheName = "testCache";  // TODO: should be set at cfg.

    /**
     * Gets the cache name.
     * @return cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Sets the cache name.
     *
     * @param cacheName cache name.
     */
    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * Get the configuration file.
     * @return configuration file.
     */
    public String getConfigurationFile() {
        return configurationfile;
    }

    /**
     * Sets the configuration file
     * @param configurationfile
     */
    public void setConfigurationFile(String configurationfile) {
        this.configurationfile = configurationfile;
    }

    /**
     * get the number of threds.
     * @return number of threads.
     */
    public int getThreads() {
        return threads;
    }

    /**
     * get the number of threds.
     * @return number of threads.
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() throws IgniteException {
        if (!stopped)
            throw new IgniteException("Attempted to start an already started Storm  Streamer");

        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.ensure(threads > 0, "threads > 0");

        log = getIgnite().log();

        // Now launch all the consumer threads.
        executor = Executors.newFixedThreadPool(threads);
    }

    /**
     * Stops streamer.
     */
    public void stop() throws IgniteException {
        if (stopped)
            throw new IgniteException("Attempted to stop an already stopped Storm Streamer");

        stopped = true;
        executor.shutdown();
    }

    /**
     * In this point we declare the output collector of the bolt.
     *
     * @param map the map derived from topology.
     * @param topologyContext the context topology in storm.
     * @param collector the output of the collector.
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        if (stopped) {

            setIgnite(Ignition.start(getConfigurationFile()));

            setStreamer(getIgnite().<K, V>dataStreamer(getCacheName()));

            start();
            stopped = false;
        }
        this.collector = collector;
    }

    /**
     * This method generates and map and do a put operations.
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        final Map<K, V> igniteGrid = (Map)tuple.getValueByField("IgniteGrid");

        if (log.isDebugEnabled()) {
            log.debug("received Tuple from Storm " + tuple.getMessageId());
        }

        executor.submit(new Runnable() {
            @Override public void run() {
                for (K k : igniteGrid.keySet()) {
                    try {
                        if (log.isDebugEnabled()) {
                            log.info("get tuple from storm  " + k + ", " + igniteGrid.get(k));
                        }

                        getStreamer().addData(k, igniteGrid.get(k));
                        getStreamer().flush(); //TODO: replace with autoflush.
                    }
                    catch (Exception e) {
                        if (log.isDebugEnabled())
                            log.debug(e.toString());
                    }
                }
            }
        });

        collector.ack(tuple);
    }

    /**
     * Clean-up the streamer when the bolt is going to shut-down.
     */
    @Override
    public void cleanup() {
        stop();
        getIgnite().close();
    }

    /**
     * This may not be necessary, as this is the last node topology before Apache Ignite.
     *
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("IgniteGrid"));
    }

    /**
     * Not used in this case
     *
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
