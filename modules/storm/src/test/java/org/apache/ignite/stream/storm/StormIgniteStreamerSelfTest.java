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
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests {@link StormStreamer}.
 */
public class StormIgniteStreamerSelfTest extends GridCommonAbstractTest {

    StormStreamer<String, String, String> stormStreamer = null;

    /** Count. */
    private static final int CNT = 100;

    public StormIgniteStreamerSelfTest(){super(true);}

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
    }



    /**
     * Defines a common scenario
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void testStormStreamer() throws TimeoutException, InterruptedException {

      try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {

          // Use getOrCreateCache
          IgniteCache<String,String> cache = ignite.getOrCreateCache("igniteCache");
          IgniteDataStreamer<String, String> dataStreamer = ignite.dataStreamer("igniteCache");

          stormStreamer = new StormStreamer<>();

            /*  Set ignite instance */
          stormStreamer.setIgnite(ignite);

            /* Set streamer instance */
          stormStreamer.setStreamer(dataStreamer);

            /* set thread count */
          stormStreamer.setThreads(5);

          //Start storm topology
          startTopology(stormStreamer);

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

    public void startTopology(StormStreamer stormStreamer){
        /* Storm topology builder */
        TopologyBuilder builder = new TopologyBuilder();


        /*Set storm spout in topology builder */
        builder.setSpout("spout", new StormSpout());

        /* Set storm bolt in topology builder */
        builder.setBolt("bolt", stormStreamer).shuffleGrouping("spout");

        /*Storm config for local cluster */
        Config config = new Config();

        /* Storm local cluster */
        LocalCluster localCluster = new LocalCluster();

        /* Submit storm topology to local cluster */
        localCluster.submitTopology("test", config, builder.createTopology());

        /* Topology will run for 10sec */
        Utils.sleep(10000);
    }


    /**
     * Test with the IgniteBolt
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void testStormStreamerIgniteBolt() throws TimeoutException, InterruptedException {

    }
}
