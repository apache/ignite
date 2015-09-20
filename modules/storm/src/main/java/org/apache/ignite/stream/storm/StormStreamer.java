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
import org.apache.ignite.*;
import org.apache.ignite.stream.*;
import java.util.Map;
import java.util.concurrent.*;

import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Server for managing stream Apache Storm. This is a Bolt storm the interact with  Apache Ignite.
 * For a description of the design and the way of use, see the page of the form.
 * How to use streamer inside
 * <a href="https://cwiki.apache.org/confluence/display/IGNITE/Streamers+Implementation+Guidelines">
 *     Streamer Implementations Guidelines</a>
 * @author Gianfranco Murador
 */
public class StormStreamer<T, K, V> extends StreamAdapter<T, K, V> implements IRichBolt {

    /** Logger. */
    private IgniteLogger log;

    /** Executor used to submit storm streams. */
    private ExecutorService executor;

    /** Topic. */
    private String topic;

    /** Number of threads to process kafka streams. */
    private int threads;

    /** Stopped. */
    private volatile boolean stopped;

    /** the storm output collector */
    private OutputCollector collector;

    /**
     * Starts streamer
     * @throws IgniteException If failed.
     */
    public void start()  throws IgniteException{
        if (stopped)
            throw new IgniteException("Attempted to stop an already stopped Storm  Streamer");
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.notNull(topic, "topic");
        A.ensure(threads > 0, "threads > 0");
        log = getIgnite().log();

    }

    /**
     * Stops streamer.
     */
    public void stop()  throws IgniteException  {
        if (!stopped)
            throw new IgniteException("Attempted to start an already started Storm Streamer");
        stopped = true;
    }
    /**
     * In this point we declare the output collector of the bolt
     * @param map the map derived from topology
     * @param topologyContext the context topology in storm
     * @param collector the output of the collector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * This method generates and map and do a put operations
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
    }

    @Override
    public void cleanup() {
    }

    /**
     * This may not be necessary, as this is the last node topology before Apache Ignite
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("IgniteGrid"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}