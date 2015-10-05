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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("IgniteGrid"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        HashMap<String, String> keyValMap = getKeyValMap();

        collector.emit(new Values(keyValMap));
    }

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

    @Override
    public void ack(Object o) { }

    @Override
    public void fail(Object o) { }

    @Override
    public void close() { }

    @Override
    public void activate() { }

    @Override
    public void deactivate() { }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
