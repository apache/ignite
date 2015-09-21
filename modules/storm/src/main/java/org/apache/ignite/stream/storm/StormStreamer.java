package org.apache.ignite.stream.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by chandresh.pancholi on 9/12/15.
 */

public class StormStreamer<T,K,V> extends StreamAdapter<T,K,V> implements IRichBolt {

    /* Logger */
    private IgniteLogger log;

    /* No. of thread to process */
    private int threads;

    /* Storm output collector */
    private OutputCollector collector;
    /**
     * Stopped.
     */
    private volatile boolean stopped = true;

    /**
     * Executor used to submit storm streams.
     */
    private ExecutorService executor;

    public void setThreads(int threads) {
        this.threads = threads;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        if (stopped) {
            start();
        }
        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        if ( log.isDebugEnabled() ) {
            log.debug("received Tuple from Storm " + tuple.getMessageId());
        }
        HashMap<K, V> igniteGrid = (HashMap) tuple.getValueByField("igniteGrid");
        executor.submit(()-> {
            for (K k:igniteGrid.keySet()){
                try {
                    getStreamer().addData(k, igniteGrid.get(k));
                }catch (Exception e){
                    log.error(e.toString());
                }
            }
        });
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        stop();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("igniteGrid"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * Stops streamer.
     */
    public void stop() throws IgniteException {
        if (stopped)
            throw new IgniteException("Attempted to start an already started Storm Streamer");
        stopped = true;
        executor.shutdown();
    }
    /**
     * Starts streamer
     * @throws IgniteException If failed.
     */
    public void start()  throws IgniteException{
        if (!stopped)
            throw new IgniteException("Attempted to stop an already stopped Storm  Streamer");
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.ensure(threads > 0, "threads > 0");
        log = getIgnite().log();

        // Now launch all the consumer threads.
        executor = Executors.newFixedThreadPool(threads);
    }
}
