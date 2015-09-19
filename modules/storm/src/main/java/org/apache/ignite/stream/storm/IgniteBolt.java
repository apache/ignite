package org.apache.ignite.stream.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author  Gianfranco Murador
 * We can provide a middlet-tier bolt to manage different kind of tuple
 */
public class IgniteBolt implements IRichBolt {
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

