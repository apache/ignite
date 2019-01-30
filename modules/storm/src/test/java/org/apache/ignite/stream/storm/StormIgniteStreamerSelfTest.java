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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests for {@link StormStreamer}.
 */
public class StormIgniteStreamerSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Ignite test configuration file. */
    private static final String GRID_CONF_FILE = "modules/storm/src/test/resources/example-ignite.xml";

    /** Ignite instance. */
    private Ignite ignite;

    /** Parallelization in Storm. */
    private static final int STORM_EXECUTORS = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteConfiguration cfg = loadConfiguration(GRID_CONF_FILE);

        cfg.setClientMode(false);

        ignite = startGrid("igniteServerNode", cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests for the streamer bolt. Ignite started in bolt based on what is specified in the configuration file.
     *
     * @throws TimeoutException
     * @throws InterruptedException
     */
    @Test
    public void testStormStreamerIgniteBolt() throws TimeoutException, InterruptedException {
        final StormStreamer<String, String> stormStreamer = new StormStreamer<>();

        stormStreamer.setAutoFlushFrequency(10L);
        stormStreamer.setAllowOverwrite(true);
        stormStreamer.setCacheName(TEST_CACHE);
        stormStreamer.setIgniteTupleField(TestStormSpout.IGNITE_TUPLE_FIELD);
        stormStreamer.setIgniteConfigFile(GRID_CONF_FILE);

        Config daemonConf = new Config();

        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);

        MkClusterParam mkClusterParam = new MkClusterParam();

        mkClusterParam.setDaemonConf(daemonConf);
        mkClusterParam.setSupervisors(4);

        final CountDownLatch latch = new CountDownLatch(TestStormSpout.CNT);

        IgniteBiPredicate<UUID, CacheEvent> putLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
            @Override public boolean apply(UUID uuid, CacheEvent evt) {
                assert evt != null;

                latch.countDown();

                return true;
            }
        };

        final UUID putLsnrId = ignite.events(ignite.cluster().forCacheNodes(TEST_CACHE))
            .remoteListen(putLsnr, null, EVT_CACHE_OBJECT_PUT);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
                @Override public void run(ILocalCluster cluster) throws IOException, InterruptedException {
                    // Creates a test topology.
                    TopologyBuilder builder = new TopologyBuilder();

                    TestStormSpout testStormSpout = new TestStormSpout();

                    builder.setSpout("test-spout", testStormSpout);
                    builder.setBolt("ignite-bolt", stormStreamer, STORM_EXECUTORS).shuffleGrouping("test-spout");

                    StormTopology topology = builder.createTopology();

                    // Prepares a mock data for the spout.
                    MockedSources mockedSources = new MockedSources();

                    mockedSources.addMockData("test-spout", getMockData());

                    // Prepares the config.
                    Config conf = new Config();

                    conf.setMessageTimeoutSecs(10);

                    IgniteCache<Integer, String> cache = ignite.cache(TEST_CACHE);

                    CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();

                    completeTopologyParam.setTimeoutMs(10000);
                    completeTopologyParam.setMockedSources(mockedSources);
                    completeTopologyParam.setStormConf(conf);

                    // Checks the cache doesn't contain any entries yet.
                    assertEquals(0, cache.size(CachePeekMode.PRIMARY));

                    Testing.completeTopology(cluster, topology, completeTopologyParam);

                    // Checks events successfully processed in 20 seconds.
                    assertTrue(latch.await(10, TimeUnit.SECONDS));

                    ignite.events(ignite.cluster().forCacheNodes(TEST_CACHE)).stopRemoteListen(putLsnrId);

                    // Validates all entries are in the cache.
                    assertEquals(TestStormSpout.CNT, cache.size(CachePeekMode.PRIMARY));

                    for (Map.Entry<Integer, String> entry : TestStormSpout.getKeyValMap().entrySet())
                        assertEquals(entry.getValue(), cache.get(entry.getKey()));
                }
            }
        );
    }

    /**
     * Prepares entry values for test input.
     *
     * @return Array of entry values.
     */
    @NotNull
    private static Values[] getMockData() {
        final int SIZE = 10;

        ArrayList<Values> mockData = new ArrayList<>();

        for (int i = 0; i < TestStormSpout.CNT; i += SIZE)
            mockData.add(new Values(TestStormSpout.getKeyValMap().subMap(i, i + SIZE)));

        return mockData.toArray(new Values[mockData.size()]);
    }
}
