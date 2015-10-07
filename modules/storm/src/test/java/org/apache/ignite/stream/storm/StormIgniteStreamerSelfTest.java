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

import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.tuple.Values;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Tests {@link StormStreamer}.
 */
public class StormIgniteStreamerSelfTest extends GridCommonAbstractTest {

    /** Storm stream object initialization. */
    StormStreamer<String, String, String> stormStreamer = null;

    /** Count. */
    private static final int CNT = 100;

    /** Cache Name */
    private static final String cacheName = "igniteCache";

    public StormIgniteStreamerSelfTest(){super(true);}

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(cacheName).clear();
    }

    /**
     * Test with the bolt Ignite started in bolt.
     * NOTE: the only working solutions for now.
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void testStormStreamerIgniteBolt() throws TimeoutException, InterruptedException {
        stormStreamer = new StormStreamer<>();
        stormStreamer.setThreads(5);

        startSimulatedTopology(stormStreamer);
    }

    /**
     * Note to run this on TC: the time out has to be setted in according
     * to power of the server. In a simple dual core it takes 6 sec.
     * look setMessageTimeoutSecs parameter.
     * @param stormStreamer the storm streamer in Ignite
     */
    public void startSimulatedTopology (final StormStreamer stormStreamer) {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);

        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);

        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
                    @Override
                    public void run(ILocalCluster cluster) throws IOException {
                        /* Storm topology builder. */
                        TopologyBuilder builder = new TopologyBuilder();

                        StormSpout stormSpout = new StormSpout();

                        /*Set storm spout in topology builder. */
                        builder.setSpout("spout", stormSpout);

                        /*Set bolt spout in topology builder. */
                        builder.setBolt("bolt", stormStreamer)
                                .shuffleGrouping("spout");

                        /* Create storm topology. */
                        StormTopology topology = builder.createTopology();

                        MockedSources mockedSources = new MockedSources();

                        //Our spout will be processing this values.
                        mockedSources.addMockData("spout", new Values(stormSpout.getKeyValMap()));

                        // prepare the config
                        Config conf = new Config();
                        conf.setNumWorkers(2);
                        // this parameter is necessary
                        conf.setMessageTimeoutSecs(10000);

                        CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                        completeTopologyParam.setMockedSources(mockedSources);
                        completeTopologyParam.setStormConf(conf);

                        Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);
                        compareStreamCacheData(stormSpout.getKeyValMap());
                    }
                }
        );

    }

    public void compareStreamCacheData(HashMap<String, String> keyValMap){
        Ignite ignite = grid();
        System.out.println(" -=------------------------- ");
        // Get the cache.
        IgniteCache<String, String> cache = ignite.cache(cacheName);
        for (Map.Entry<String, String> entry : keyValMap.entrySet()) {
            System.out.println(" Key === " +entry.getKey() +  " Value ====  " +  cache.get(entry.getKey()));
            assertEquals(entry.getValue(), cache.get(entry.getKey()));
        }
    }

}

