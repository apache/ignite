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

/**
 * @author  Gianfranco Murador
 * Ignite Bolt, We provide a custom Storm Bolt to setup the input. This bolt could be empty.
 */
public abstract class IgniteBolt implements IRichBolt {
    /** the storm output collector */
    private OutputCollector collector;

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

