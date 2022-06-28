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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests a scenario when a temporary owned partition is released during PME.
 * The eviction should not start because this partition can be assigned as primary.
 */
@WithSystemProperty(key = "IGNITE_PRELOAD_RESEND_TIMEOUT", value = "0")
public class DelayedOwningDuringExchangeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setCacheMode(CacheMode.PARTITIONED).
            setBackups(0).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDelayedOwning_1() throws Exception {
        testDelayedRenting(0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDelayedOwning_2() throws Exception {
        testDelayedRenting(0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDelayedOwning_3() throws Exception {
        testDelayedRenting(1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDelayedOwning_4() throws Exception {
        testDelayedRenting(1, 1);
    }

    /**
     * @param idx Index.
     * @param mode Mode.
     */
    private void testDelayedRenting(int idx, int mode) throws Exception {
        final int nodes = 2;

        IgniteEx crd = startGrids(nodes);

        awaitPartitionMapExchange();

        IgniteEx testGrid = grid(idx);

        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);

        testGrid.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                wait(fut, 0);
            }

            @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                wait(fut, 1);
            }

            private void wait(GridDhtPartitionsExchangeFuture fut, int mode0) {
                if (fut.initialVersion().equals(new AffinityTopologyVersion(nodes + 2, 0)) && mode == mode0) {
                    l1.countDown();

                    try {
                        assertTrue(U.await(l2, 30_000, TimeUnit.MILLISECONDS));
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        fail(X.getFullStackTrace(e));
                    }
                }
            }
        });

        int p0 = evictingPartitionsAfterJoin(testGrid, testGrid.cache(DEFAULT_CACHE_NAME), 1).get(0);

        testGrid.cache(DEFAULT_CACHE_NAME).put(p0, 0);

        GridDhtPartitionTopology top0 = testGrid.cachex(DEFAULT_CACHE_NAME).context().topology();
        GridDhtLocalPartition evictPart = top0.localPartition(p0);
        assertTrue(evictPart.reserve());

        IgniteEx joined = startGrid(nodes);

        GridDhtPartitionTopology top1 = joined.cachex(DEFAULT_CACHE_NAME).context().topology();

        assertTrue(GridTestUtils.waitForCondition(
            () -> top0.nodes(p0, new AffinityTopologyVersion(nodes + 1, 1)).size() == 2, 5_000));

        assertTrue(GridTestUtils.waitForCondition(
            () -> top1.nodes(p0, new AffinityTopologyVersion(nodes + 1, 1)).size() == 2, 5_000));

        Collection<ClusterNode> affOwners = testGrid.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(p0);
        assertEquals(1, affOwners.size());

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                stopGrid(nodes);
            }
        });

        assertTrue(U.await(l1, 30_000, TimeUnit.MILLISECONDS));

        evictPart.release();

        doSleep(1000);

        l2.countDown();

        awaitPartitionMapExchange(true, true, null);

        fut.get();

        assertEquals(0, testGrid.cache(DEFAULT_CACHE_NAME).get(p0));
    }
}
