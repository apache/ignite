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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/**
 * The test starts an implicit transaction during the cache is stopping.
 * The transaction has to be completed, and the cache is stopped.
 */
public class StartImplicitlyTxOnStopCacheTest extends GridCommonAbstractTest {
    /** */
    private static final String GROUP = "test-group";

    /** Node failure occurs. */
    private final AtomicBoolean failure = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setFailureHandler(new AbstractFailureHandler() {
                @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                    failure.set(true);

                    return true;
                }
            })
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setGroupName(GROUP)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        IgniteEx client = startClientGrid("client");

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        TestRecordingCommunicationSpi commSpiClient = TestRecordingCommunicationSpi.spi(client);

        commSpiClient.blockMessages(GridNearTxPrepareRequest.class, getTestIgniteInstanceName(0));

        CyclicBarrier exchnageStartedBarrier = new CyclicBarrier(2, commSpiClient::stopBlock);

        ignite0.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    exchnageStartedBarrier.await();
                }
                catch (InterruptedException | BrokenBarrierException e) {
                    log.error("Exchange delay was interrupted.", e);
                }
            }
        });

        IgniteInternalFuture runTxFut = runAsync(() -> cache.put(100, 100));

        IgniteInternalFuture destroyCacheFut = runAsync(() ->
            client.destroyCache(DEFAULT_CACHE_NAME));

        exchnageStartedBarrier.await();

        assertTrue(GridTestUtils.waitForCondition(destroyCacheFut::isDone, 10_000));

        assertTrue(GridTestUtils.waitForCondition(runTxFut::isDone, 10_000));

        assertNull(client.cache(DEFAULT_CACHE_NAME));
    }

    /** @throws Exception If failed. */
    @Test
    public void testTxStartAfterGatewayBlockedOnCacheDestroy() throws Exception {
        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(2);
        GridCacheSharedContext<Object, Object> cctx = crd.context().cache().context();

        // Cache group with multiple caches are important here, partition topology will not be stopped on cache destroy.
        crd.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME + "_1")
            .setGroupName(GROUP)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        CountDownLatch txStarted = new CountDownLatch(1);
        CountDownLatch gatewayStopped = new CountDownLatch(1);

        IgniteTxManager tm = Mockito.spy(cctx.tm());
        setFieldValue(crd.context().cache().context(), "txMgr", tm);

        Mockito.doAnswer(m -> {
            // Create tx after gateway was stopped (but not blocked).
            txStarted.countDown();
            gatewayStopped.await();

            return m.callRealMethod();
        }).when(tm).onCreated(Mockito.any(), Mockito.any());

        GridCacheGateway gate = Mockito.spy(cctx.cache().cache(DEFAULT_CACHE_NAME).context().gate());
        setFieldValue(crd.context().cache().context().cache().cache(DEFAULT_CACHE_NAME).context(), "gate", gate);

        Mockito.doAnswer(m -> {
            // Await tx gateway enter and mark gateway to stop.
            txStarted.await();

            return m.callRealMethod();
        }).when(gate).stopped();

        Mockito.doAnswer(m -> {
            // Gateway is ready to block.
            gatewayStopped.countDown();

            return m.callRealMethod();
        }).when(gate).onStopped();

        IgniteInternalFuture<Object> fut = runAsync(() -> {
            txStarted.await();

            grid(1).destroyCache(DEFAULT_CACHE_NAME);
        });

        try {
            Integer remoteKey = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

            crd.cache(DEFAULT_CACHE_NAME).putAsync(remoteKey, "val").get();
        }
        catch (CacheException e) {
            // No-op.
        }

        fut.get();

        // Make sure exchange worker processed previous task to get potential failure.
        crd.getOrCreateCache("new-cache");

        assertFalse(failure.get());
    }
}
