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

package org.apache.ignite.internal.processors.cache;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.blockSingleExhangeMessage;

/** */
public class IGN3576DynamicCacheOnNodeJoinTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cacheIgn3576";

    /** */
    private static final String GROUP_NAME = "testGrpIgn3576";

    /** */
    private static final String STABLE_CACHE_NAME = "stableCacheIgn3576";

    /** */
    private static final String STABLE_GROUP_NAME = "stableGrpIgn3576";

    /** */
    private final AtomicReference<FailureContext> failureCtx = new AtomicReference<>();

    /** */
    private final TestRecordingCommunicationSpi joiningNodeSpi = new TestRecordingCommunicationSpi();

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(GridAbstractTest.log);

    /** */
    private final LogListener tryMergeLsnr = LogListener.matches(
        "Coordinator received all messages, try merge").build();

    /** */
    private final LogListener customEventLsnr = LogListener.matches(
        "Stop merge, custom event found").build();

    /** */
    private final LogListener partValidationLsnr = LogListener.matches(
        "Partition states validation has failed").atMost(0).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(testLog);
        cfg.setConsistentId(igniteInstanceName);
        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(false)));

        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                IGN3576DynamicCacheOnNodeJoinTest.this.failureCtx.compareAndSet(null, failureCtx);

                return false;
            }
        });

        if (getTestIgniteInstanceName(2).equals(igniteInstanceName))
            cfg.setCommunicationSpi(joiningNodeSpi);
        else
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog.registerAllListeners(tryMergeLsnr, customEventLsnr, partValidationLsnr);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /** */
    @Test
    public void testDynamicCacheStartDuringNodeJoinExchange() throws Exception {
        IgniteEx crd = startGrid(1);

        crd.cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        assertEquals(1, crd.cluster().currentBaselineTopology().size());

        crd.getOrCreateCache(cacheConfiguration(STABLE_CACHE_NAME, STABLE_GROUP_NAME));

        awaitPartitionMapExchange();

        GridCachePartitionExchangeManager<Object, Object> exchMgr = crd.context().cache().context().exchange();

        exchMgr.mergeExchangesTestWaitVersion(new AffinityTopologyVersion(2, 1), null);

        joiningNodeSpi.blockMessages(blockSingleExhangeMessage());

        IgniteInternalFuture<IgniteEx> startFut = GridTestUtils.runAsync(() -> startGrid(2), "node-starter");

        try {
            assertTrue("Failed to block the joining node's single message.",
                joiningNodeSpi.waitForBlocked(1, 10_000));

            IgniteInternalFuture<IgniteCache<Object, Object>> cacheStartFut = GridTestUtils.runAsync(
                () -> crd.getOrCreateCache(cacheConfiguration(CACHE_NAME, GROUP_NAME)), "cache-starter");

            AffinityTopologyVersion cacheStartTopVer = new AffinityTopologyVersion(2, 1);

            assertTrue("The dynamic cache exchange was not queued.", GridTestUtils.waitForCondition(
                () -> cacheExchangeFuture(exchMgr, cacheStartTopVer) != null, 10_000));

            GridDhtPartitionsExchangeFuture cacheExchFut = cacheExchangeFuture(exchMgr, cacheStartTopVer);

            CacheAffinitySharedManager<Object, Object> affMgr = crd.context().cache().context().affinity();

            GridTestUtils.setFieldValue(cacheExchFut, "exchCtx",
                new ExchangeContext(crd.context().cache().context(), true, cacheExchFut));

            CachesRegistry cachesRegistry = GridTestUtils.getFieldValue(affMgr, "cachesRegistry");

            cachesRegistry.update(cacheExchFut.exchangeActions());

            int grpId = CU.cacheGroupId(CACHE_NAME, GROUP_NAME);

            CacheGroupDescriptor grpDesc = crd.context().cache().cacheGroupDescriptor(grpId);

            Method holderMtd = affMgr.getClass().getDeclaredMethod("getOrCreateGroupHolder",
                AffinityTopologyVersion.class, CacheGroupDescriptor.class);

            holderMtd.setAccessible(true);

            holderMtd.invoke(affMgr, new AffinityTopologyVersion(2, 0), grpDesc);

            GridAffinityAssignmentCache aff = affMgr.affinity(grpId);

            Method calculateMtd = affMgr.getClass().getDeclaredMethod("calculateAndInit",
                ExchangeDiscoveryEvents.class, GridAffinityAssignmentCache.class, AffinityTopologyVersion.class);

            calculateMtd.setAccessible(true);
            calculateMtd.invoke(affMgr, cacheExchFut.context().events(), aff, cacheStartTopVer);

            assertNotNull("The previous affinity assignment was not initialized.", aff.idealAssignmentRaw());

            joiningNodeSpi.stopBlock();

            startFut.get(30_000);
            cacheStartFut.get(30_000);

            awaitPartitionMapExchange();

            assertTrue("The merge protocol was not entered.", tryMergeLsnr.check());
            assertTrue("The dynamic cache start was not queued during the node join exchange.",
                customEventLsnr.check());
            assertTrue("Partition states validation failed.", partValidationLsnr.check());

            FailureContext failure = failureCtx.get();

            assertNull("Unexpected critical failure: " + (failure == null ? null : X.getFullStackTrace(failure.error())),
                failure);

            for (int p = 0; p < crd.affinity(CACHE_NAME).partitions(); p++)
                assertNotNull("No primary node for partition " + p, crd.affinity(CACHE_NAME).mapPartitionToNode(p));
        }
        finally {
            joiningNodeSpi.stopBlock();
        }
    }

    /**
     * @param exchMgr Exchange manager.
     * @param topVer Exchange topology version.
     * @return Cache start exchange future or {@code null}.
     */
    private GridDhtPartitionsExchangeFuture cacheExchangeFuture(
        GridCachePartitionExchangeManager<Object, Object> exchMgr,
        AffinityTopologyVersion topVer
    ) {
        for (GridDhtPartitionsExchangeFuture fut : exchMgr.exchangeFutures()) {
            if (topVer.equals(fut.initialVersion()) && fut.exchangeActions() != null)
                return fut;
        }

        return null;
    }

    /**
     * @param name Cache name.
     * @param grpName Group name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name, String grpName) {
        return new CacheConfiguration<Object, Object>()
            .setName(name)
            .setGroupName(grpName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(ATOMIC)
            .setBackups(0);
    }
}
