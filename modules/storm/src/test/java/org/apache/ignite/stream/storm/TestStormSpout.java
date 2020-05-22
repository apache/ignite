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
import java.util.TreeMap;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

/**
 * Testing Storm spout.
 */
public class TestStormSpout implements IRichSpout {

    /** Field by which tuple data is obtained by the streamer. */
    public static final String IGNITE_TUPLE_FIELD = "test-field";

    /** Number of outgoing tuples. */
    public static final int CNT = 500;

    /** Spout message prefix. */
    private static final String VAL_PREFIX = "v:";

    /** Spout output collector. */
    private SpoutOutputCollector collector;

    /** Map of messages to be sent. */
    private static TreeMap<Integer, String> keyValMap;

    static {
        keyValMap = generateKeyValMap();
    }

    /**
     * Declares the output field for the component.
     *
     * @param outputFieldsDeclarer Declarer to declare fields on.
     */
    @Override public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(IGNITE_TUPLE_FIELD));
    }

    /**
     * {@inheritDoc}
     */
    @Override public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * Requests to emit a tuple to the output collector.
     */
    @Override public void nextTuple() {
        // No-op. Sent via mocked sources.
    }

    /**
     * Generates key,value pair to emit to bolt({@link StormStreamer}).
     *
     * @return Key, value pair.
     */
    private static TreeMap<Integer, String> generateKeyValMap() {
        TreeMap<Integer, String> keyValMap = new TreeMap<>();

        for (int evt = 0; evt < CNT; evt++) {
            String msg = VAL_PREFIX + evt;

            keyValMap.put(evt, msg);
        }

        return keyValMap;
    }

    public static TreeMap<Integer, String> getKeyValMap() {
        return keyValMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void ack(Object msgId) {
    }

    /**
     * {@inheritDoc}
     */
    @Override public void fail(Object msgId) {
    }

    /**
     * {@inheritDoc}
     */
    @Override public void close() {
    }

    /**
     * {@inheritDoc}
     */
    @Override public void activate() {
    }

    /**
     * {@inheritDoc}
     */
    @Override public void deactivate() {
    }

    /**
     * {@inheritDoc}
     */
    @Override public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
