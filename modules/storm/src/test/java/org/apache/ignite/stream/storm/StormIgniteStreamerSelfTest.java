package org.apache.ignite.stream.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.TimeoutException;

/**
 * Created by chandresh.pancholi on 9/13/15.
 */
public class StormIgniteStreamerSelfTest extends GridCommonAbstractTest {


//    public StormIgniteStreamerSelfTest(){super(true);}

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(null).clear();
    }
    public void testStormStreamer() throws TimeoutException,InterruptedException{
//        StormStreamer<String,String, String > stormStreamer = null;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new StormSpout());
        builder.setBolt("bolt", new StormStreamer<>()).shuffleGrouping("spout");

        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("test", config, builder.createTopology());
//        Utils.sleep(1000);
//        cluster.killTopology("test");
//        cluster.shutdown();
    }
}
