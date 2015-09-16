package org.apache.ignite.stream.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Created by chandresh.pancholi on 9/13/15.
 */
public class StormIgniteStreamerSelfTest extends GridCommonAbstractTest {

    /** Count. */
    private static final int CNT = 100;

    public StormIgniteStreamerSelfTest(){super(true);}

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
        Ignite ignite = grid();

        try (IgniteDataStreamer<String, String> stmr = ignite.dataStreamer(null)) {

            stmr.allowOverwrite(true);
            stmr.autoFlushFrequency(10);

            // Get the cache.
            IgniteCache<String, String> cache = ignite.cache(null);

            //Start storm topology
            startTopology();

            //Get key, value from hash map
            HashMap<String,String> keyValMap = new StormSpout().getKeyValMap();

            final CountDownLatch latch = new CountDownLatch(CNT);

            IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    latch.countDown();

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(null)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);

            latch.await();

            for (Map.Entry<String, String> entry : keyValMap.entrySet())
                assertEquals(entry.getValue(), cache.get(entry.getKey()));
        }
    }

    public void startTopology(){

        //Storm topology builder
        TopologyBuilder builder = new TopologyBuilder();

        //Set storm spout in builder
        builder.setSpout("spout", new StormSpout());

        //Set storm bolt in builder
        builder.setBolt("bolt", new StormStreamer<>()).shuffleGrouping("spout");

        //Storm config for local cluster
        Config config = new Config();

        //Storm local cluster
        LocalCluster localCluster = new LocalCluster();

        //Submit storm topology to local cluster
        localCluster.submitTopology("test", config, builder.createTopology());

        //Topology will run for 10sec
        Utils.sleep(10000);

        //Kil "test" topology
        localCluster.killTopology("test");

        //Local storm cluster shutting down
        localCluster.shutdown();
    }

}
