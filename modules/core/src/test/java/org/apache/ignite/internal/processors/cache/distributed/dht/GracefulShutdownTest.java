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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Test checks various cluster shutdown and initiated policy.
 */
@WithSystemProperty(key = IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN, value = "false")
public class GracefulShutdownTest extends GridCacheDhtPreloadWaitForBackupsWithPersistenceTest {

    /** Shutdown policy of static configuration. */
    public ShutdownPolicy policy = ShutdownPolicy.GRACEFUL;

    /** Listening test logger. */
    ListeningTestLogger listeningLog;

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        listeningLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setShutdownPolicy(policy);
    }

    /**
     * Check static configuration of shutdown policy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithStaticConfiguredPolicy() throws Exception {
        Ignite ignite0 = startGrid(0);

        assertSame(ignite0.cluster().shutdownPolicy(), ignite0.configuration().getShutdownPolicy());

        ignite0.close();

        policy = ShutdownPolicy.IMMEDIATE;

        ignite0 = startGrid(0);

        assertSame(ignite0.cluster().shutdownPolicy(), ignite0.configuration().getShutdownPolicy());

        assertSame(ignite0.cluster().shutdownPolicy(), ShutdownPolicy.IMMEDIATE);
    }

    /**
     * Test checked exception which is thrown when configuration of nodes different.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTwoNodesWithDifferentConfuguration() throws Exception {
        Ignite ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        assertSame(ignite0.configuration().getShutdownPolicy(), ShutdownPolicy.GRACEFUL);

        policy = ShutdownPolicy.IMMEDIATE;

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(1), IgniteCheckedException.class,
            "Remote node has shutdoun policy different from local local");
    }

    /**
     * Check dynamic configuration of shutdown policy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithDynamicConfiguredPolicy() throws Exception {
        Ignite ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        assertSame(ignite0.cluster().shutdownPolicy(), ignite0.configuration().getShutdownPolicy());

        ShutdownPolicy configuredPolicy = ignite0.cluster().shutdownPolicy();

        ShutdownPolicy policyToChange = null;

        for (ShutdownPolicy policy : ShutdownPolicy.values()) {
            if (policy != ignite0.cluster().shutdownPolicy())
                policyToChange = policy;
        }

        assertNotNull(policyToChange);

        ignite0.cluster().shutdownPolicy(policyToChange);

        forceCheckpoint();

        info("Policy to change: " + policyToChange);

        ignite0.close();

        ignite0 = startGrid(0);

        info("Policy after restart: " + ignite0.cluster().shutdownPolicy());

        assertNotSame(ignite0.cluster().shutdownPolicy(), ignite0.configuration().getShutdownPolicy());

        assertSame(ignite0.cluster().shutdownPolicy(), policyToChange);
    }

    /**
     * Try to stop node when not all backups are matching of ideal assignment.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotIdealOwners() throws Exception {
        backups = 1;

        Ignite ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        for (int i = 1; i <= 3; i++) {
            IgniteCache cache = ignite0.cache("cache" + i);

            assertNotNull(cache);

            try (IgniteDataStreamer streamer = ignite0.dataStreamer("cache" + i)) {
                for (int j = 0; j < 100; j++)
                    streamer.addData(j, j);
            }
        }

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite0);

        spi.blockMessages((node, msg) -> {
            String nodeName = (String)node.attributes().get(ATTR_IGNITE_INSTANCE_NAME);

            if (msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage supplyMsg = (GridDhtPartitionSupplyMessage)msg;

                if (supplyMsg.groupId() != CU.cacheId(GridCacheUtils.UTILITY_CACHE_NAME) &&
                    getTestIgniteInstanceName(1).equals(nodeName))
                    return true;
            }

            return false;
        });

        startGrid(1);
        Ignite ignite2 = startGrid(2);

        resetBaselineTopology();

        spi.waitForBlocked();

        for (CacheGroupContext grp: ((IgniteEx)ignite2).context().cache().cacheGroups()) {
            GridTestUtils.waitForCondition(() ->
                !grp.topology().partitionMap(false).get(((IgniteEx)ignite2).localNode().id()).hasMovingPartitions(), 30_000);
        }

        LogListener lnsr = LogListener.matches("This node is waiting for backups of local partitions for group")
            .build();

        listeningLog.registerListener(lnsr);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            ignite2.close();
        });

        assertTrue(GridTestUtils.waitForCondition(lnsr::check, 30_000));

        assertFalse(fut.isDone());

        spi.stopBlock();

        assertTrue(GridTestUtils.waitForCondition(fut::isDone, 30_000));
    }

    /**
     * Stopping node and start cache which does not allow it.
     *
     * @throws Exception If failed.
     */
    @Test
    public void tesStartCacheWhenNodeStopping() throws Exception {
        backups = 2;

        Ignite ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        for (int i = 1; i <= 3; i++) {
            IgniteCache cache = ignite0.cache("cache" + i);

            assertNotNull(cache);

            try (IgniteDataStreamer streamer = ignite0.dataStreamer("cache" + i)) {
                for (int j = 0; j < 100; j++)
                    streamer.addData(j, j);
            }
        }

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite0);

        spi.blockMessages((node, msg) -> {
            String nodeName = (String)node.attributes().get(ATTR_IGNITE_INSTANCE_NAME);

            if (msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage supplyMsg = (GridDhtPartitionSupplyMessage)msg;

                if (supplyMsg.groupId() != CU.cacheId(GridCacheUtils.UTILITY_CACHE_NAME) &&
                    getTestIgniteInstanceName(1).equals(nodeName))
                    return true;
            }

            return false;
        });

        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        resetBaselineTopology();

        spi.waitForBlocked();

        for (CacheGroupContext grp: ((IgniteEx)ignite2).context().cache().cacheGroups()) {
            grp.preloader().rebalanceFuture().get();
        }

        ignite2.close();

        LogListener lnsr = LogListener.matches("This node is waiting for completion of rebalance for group")
            .build();

        listeningLog.registerListener(lnsr);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            ignite1.close();
        });

        assertTrue(GridTestUtils.waitForCondition(lnsr::check, 30_000));

        listeningLog.unregisterListener(lnsr);

        assertFalse(fut.isDone());

        ignite0.getOrCreateCache(new CacheConfiguration(DEFAULT_CACHE_NAME).setBackups(1));

        spi.stopBlock();

        lnsr = LogListener.matches("This node is waiting for backups of local partitions for group")
            .build();

        listeningLog.registerListener(lnsr);

        assertTrue(GridTestUtils.waitForCondition(lnsr::check, 30_000));
    }
}
