package org.apache.ignite.stream.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by chandresh.pancholi on 9/13/15.
 */
public class StormSpout implements IRichSpout {
    /* Count */
    private static int CNT = 100;

    /** Spout message value URL. */
    private static final String VALUE = "Value,";

    /* spout output collector */
    private SpoutOutputCollector collector;

    private HashMap<String,String> keyValMap = new HashMap<>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("igniteGrid"));
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
    public HashMap<String, String> getKeyValMap(){
        List<Integer> numbers = new ArrayList<>();

        for (int i = 1; i <= CNT; i++)
            numbers.add(i);

        Collections.shuffle(numbers);

        HashMap<String,String> keyValMap = new HashMap<>();

        for(int evt = 0; evt<CNT; evt++) {

            String ip = Integer.toString(numbers.get(evt));

            String msg = VALUE + ip;

            keyValMap.put(ip, msg);
        }
        return keyValMap;
    }
    @Override
    public void ack(Object o) {}

    @Override
    public void fail(Object o) {}

    @Override
    public void close() {}

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
