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

    /** Spout message key prefix. */
    private static final String KEY_PREFIX = "192.168.2.";

    /** Spout message value URL. */
    private static final String VALUE_URL = ",www.example.com,";

    /* spout output collector */
    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        // Generate random subnets.
        List<Integer> subnet = new ArrayList<>();

        for (int i = 1; i <= CNT; i++)
            subnet.add(i);

        Collections.shuffle(subnet);
        List<HashMap<String,String>> messages = new ArrayList<>(CNT);
        HashMap<String,String> keyValMap = new HashMap<>();

        for(int evt = 0; evt<CNT; evt++){
            long runtime = System.currentTimeMillis();

            String ip = KEY_PREFIX + subnet.get(evt);

            String msg = runtime + VALUE_URL + ip;

            keyValMap.put(ip,msg);
            collector.emit(new Values(ip, msg));
        }

        messages.add(keyValMap);
        System.out.println(messages);
        collector.emit(new Values(messages));
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }
}
