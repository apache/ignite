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

package org.apache.ignite.internal.processors.datastreamer;

import java.io.StringWriter;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for {@code IgniteDataStreamerImpl}.
 */
public class DataStreamerImplSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of keys to load via data streamer. */
    private static final int KEYS_COUNT = 1000;

    /** Next nodes after MAX_CACHE_COUNT start without cache */
    private static final int MAX_CACHE_COUNT = 4;

    /** Started grid counter. */
    private static int cnt;

    /** No nodes filter. */
    private static volatile boolean noNodesFilter;

    /** Indicates whether we need to make the topology stale */
    private static boolean needStaleTop = false;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCommunicationSpi(new StaleTopologyCommunicationSpi());

        if (cnt < MAX_CACHE_COUNT)
            cfg.setCacheConfiguration(cacheConfiguration());

        cnt++;

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullPointerExceptionUponDataStreamerClosing() throws Exception {
        startGrids(5);

        final CyclicBarrier barrier = new CyclicBarrier(2);

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                U.awaitQuiet(barrier);

                G.stopAll(true);

                return null;
            }
        }, 1);

        Ignite g4 = grid(4);

        IgniteDataStreamer<Object, Object> dataLdr = g4.dataStreamer(DEFAULT_CACHE_NAME);

        dataLdr.perNodeBufferSize(32);

        for (int i = 0; i < 100000; i += 2) {
            dataLdr.addData(i, i);
            dataLdr.removeData(i + 1);
        }

        U.awaitQuiet(barrier);

        info("Closing data streamer.");

        try {
            dataLdr.close(true);
        }
        catch (CacheException | IllegalStateException ignore) {
            // This is ok to ignore this exception as test is racy by it's nature -
            // grid is stopping in different thread.
        }
    }

    /**
     * Data streamer should correctly load entries from HashMap in case of grids with more than one node
     * and with GridOptimizedMarshaller that requires serializable.
     *
     * @throws Exception If failed.
     */
    public void testAddDataFromMap() throws Exception {
        cnt = 0;

        startGrids(2);

        Ignite g0 = grid(0);

        IgniteDataStreamer<Integer, String> dataLdr = g0.dataStreamer(DEFAULT_CACHE_NAME);

        Map<Integer, String> map = U.newHashMap(KEYS_COUNT);

        for (int i = 0; i < KEYS_COUNT; i++)
            map.put(i, String.valueOf(i));

        dataLdr.addData(map);

        dataLdr.close();

        Random rnd = new Random();

        IgniteCache<Integer, String> c = g0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEYS_COUNT; i++) {
            Integer k = rnd.nextInt(KEYS_COUNT);

            String v = c.get(k);

            assertEquals(k.toString(), v);
        }
    }

    /**
     * Test logging on {@code DataStreamer.addData()} method when cache have no data nodes
     *
     * @throws Exception If fail.
     */
    public void testNoDataNodesOnClose() throws Exception {
        boolean failed = false;

        cnt = 0;

        noNodesFilter = true;

        try {
            Ignite ignite = startGrid(1);

            try (IgniteDataStreamer<Integer, String> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
                streamer.addData(1, "1");
            }
            catch (CacheException ignored) {
                failed = true;
            }
        }
        finally {
            noNodesFilter = false;

            assertTrue(failed);
        }
    }

    /**
     * Test logging on {@code DataStreamer.addData()} method when cache have no data nodes
     *
     * @throws Exception If fail.
     */
    public void testNoDataNodesOnFlush() throws Exception {
        boolean failed = false;

        cnt = 0;

        noNodesFilter = true;

        try {
            Ignite ignite = startGrid(1);

            IgniteFuture fut = null;

            try (IgniteDataStreamer<Integer, String> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
                fut = streamer.addData(1, "1");

                streamer.flush();
            }
            catch (IllegalStateException ignored) {
                try {
                    fut.get();

                    fail("DataStreamer ignores failed streaming.");
                }
                catch (CacheServerNotFoundException ignored2) {
                    // No-op.
                }

                failed = true;
            }
        }
        finally {
            noNodesFilter = false;

            assertTrue(failed);
        }
    }

    /**
     * Cluster topology mismatch shall result in DataStreamer retrying cache update with the latest topology and
     * no error logged to the console.
     *
     * @throws Exception if failed
     */
    public void testRetryWhenTopologyMismatch() throws Exception {
        final int KEY = 1;
        final String VAL = "1";

        cnt = 0;

        StringWriter logWriter = new StringWriter();
        Appender logAppender = new WriterAppender(new SimpleLayout(), logWriter);

        Logger.getRootLogger().addAppender(logAppender);

        startGrids(MAX_CACHE_COUNT - 1); // cache-enabled nodes

        try (Ignite ignite = startGrid(MAX_CACHE_COUNT);
             IgniteDataStreamer<Integer, String> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {

            needStaleTop = true; // simulate stale topology for the next action

            streamer.addData(KEY, VAL);
        } finally {
            needStaleTop = false;

            logWriter.flush();

            Logger.getRootLogger().removeAppender(logAppender);

            logAppender.close();
        }

        assertFalse(logWriter.toString().contains("DataStreamer will retry data transfer at stable topology"));
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        if (noNodesFilter)
            cacheCfg.setNodeFilter(F.alwaysFalse());

        return cacheCfg;
    }

    /**
     * Simulate stale (not up-to-date) topology
     */
    private static class StaleTopologyCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            // Send stale topology only in the first request to avoid indefinitely getting failures.
            if (needStaleTop) {
                if (msg instanceof GridIoMessage) {
                    GridIoMessage ioMsg = (GridIoMessage)msg;

                    Message appMsg = ioMsg.message();

                    if (appMsg != null && appMsg instanceof DataStreamerRequest) {
                        DataStreamerRequest req = (DataStreamerRequest)appMsg;

                        AffinityTopologyVersion validTop = req.topologyVersion();

                        // Simulate situation when a node did not receive the latest "node joined" topology update causing
                        // topology mismatch
                        AffinityTopologyVersion staleTop = new AffinityTopologyVersion(
                            validTop.topologyVersion() - 1,
                            validTop.minorTopologyVersion());

                        appMsg = new DataStreamerRequest(
                            req.requestId(),
                            req.responseTopicBytes(),
                            req.cacheName(),
                            req.updaterBytes(),
                            req.entries(),
                            req.ignoreDeploymentOwnership(),
                            req.skipStore(),
                            req.keepBinary(),
                            req.deploymentMode(),
                            req.sampleClassName(),
                            req.userVersion(),
                            req.participants(),
                            req.classLoaderId(),
                            req.forceLocalDeployment(),
                            staleTop,
                            -1);

                        msg = new GridIoMessage(
                            GridTestUtils.<Byte>getFieldValue(ioMsg, "plc"),
                            GridTestUtils.getFieldValue(ioMsg, "topic"),
                            GridTestUtils.<Integer>getFieldValue(ioMsg, "topicOrd"),
                            appMsg,
                            GridTestUtils.<Boolean>getFieldValue(ioMsg, "ordered"),
                            ioMsg.timeout(),
                            ioMsg.skipOnTimeout());

                        needStaleTop = false;
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}