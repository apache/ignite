package org.apache.ignite.stream.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamAdapter;

import java.util.Map;

/**
 * Created by chandresh.pancholi on 9/12/15.
 */
public class StormStreamer<T,K,V> extends StreamAdapter<T,K,V> implements IRichBolt{

    /* Logger */
    private IgniteLogger log;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        log = getIgnite().log();
        try{
            getStreamer().addData((K)(String) tuple.getValueByField("key"), (V) (String) tuple.getValueByField("value"));
        }catch (Exception e){
            U.warn(log, "Stream data is ignored due to an error ", e);
        }

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("number"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
