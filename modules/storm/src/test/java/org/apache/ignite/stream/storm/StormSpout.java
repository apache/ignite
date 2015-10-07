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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Defines a testing spout mandatory for Storm
 */
public class StormSpout implements IRichSpout {

    /** Count */
    private static final int CNT = 10;

    /** Spout message value URL. */
    private static final String VALUE = "Value,";

    /* spout output collector */
    private SpoutOutputCollector collector;

    private HashMap<String, String> keyValMap = new HashMap<>();

    /**
     * Declares the output field for the component.
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("IgniteGrid"));
    }

    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the spout with the environment in which the spout executes.
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * When this method is called, Storm is requesting that the Spout emit tuples to the output collector.
     */
    @Override
    public void nextTuple() {
        HashMap<String, String> keyValMap = getKeyValMap();

        collector.emit(new Values(keyValMap));
    }

    /**
     * It generate key,value pair to emit to bolt({@link StormStreamer}).
     * @return Key,value pair
     */
    public HashMap<String, String> getKeyValMap() {
        List<Integer> numbers = new ArrayList<>();

        for (int i = 1; i <= CNT; i++)
            numbers.add(i);

        Collections.shuffle(numbers);

        HashMap<String, String> keyValMap = new HashMap<>();

        for (int evt = 0; evt < CNT; evt++) {
            String ip = Integer.toString(numbers.get(evt));

            String msg = VALUE + ip;

            keyValMap.put(ip, msg);
        }

        return keyValMap;
    }

    /**
     * Storm has determined that the tuple emitted by this spout with the msgId identifier has been fully processed.
     * @param msgId
     */
    @Override
    public void ack(Object msgId) { }

    /**
     * The tuple emitted by this spout with the msgId identifier has failed to be fully processed.
     * Typically, an implementation of this method will put that message back on the queue to be
     * replayed at a later time.
     * @param msgId
     */
    @Override
    public void fail(Object msgId) { }

    /**
     * Called when an ISpout is going to be shutdown.
     */
    @Override
    public void close() { }

    /**
     * Called when a spout has been activated out of a deactivated mode.
     * nextTuple will be called on this spout soon
     */
    @Override
    public void activate() { }

    /**
     * Called when a spout has been deactivated.
     * nextTuple will not be called while a spout is deactivated.
     */
    @Override
    public void deactivate() { }

    /**
     * Declare configuration specific to this component.
     * Only a subset of the "topology.*" configs can be overridden
     * @return Map of String and Object.
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
