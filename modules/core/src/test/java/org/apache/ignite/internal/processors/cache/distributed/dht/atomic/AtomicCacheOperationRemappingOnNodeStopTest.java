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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** */
public class AtomicCacheOperationRemappingOnNodeStopTest extends GridCommonAbstractTest {
    /** */
    private static final LogListener NEAR_UPDATE_RESP_LOG_LSNR = LogListener.matches("Sent near update response").build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger log = new ListeningTestLogger(GridAbstractTest.log);

        log.registerListener(NEAR_UPDATE_RESP_LOG_LSNR);

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setFailureHandler(new StopNodeOrHaltFailureHandler());
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrids(3);

        grid(0).createCache(DEFAULT_CACHE_NAME);

        IgniteEx firstNode = grid(1);

        CountDownLatch zeroNodePmeFinishedLatch = new CountDownLatch(1);
        CountDownLatch firstNodePmeStartedLatch = new CountDownLatch(1);
        CountDownLatch firstNodePmeUnblockedLatch = new CountDownLatch(1);
        CountDownLatch firstNodePmeFinishedLatch = new CountDownLatch(1);
        CountDownLatch firstNodeStopUnblockedLatch = new CountDownLatch(1);

        grid(0).context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                zeroNodePmeFinishedLatch.countDown();
            }
        });

        firstNode.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    firstNodePmeStartedLatch.countDown();

                    firstNodePmeUnblockedLatch.await(getTestTimeout(), MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        stopGrid(2);

        firstNodePmeStartedLatch.await(getTestTimeout(), MILLISECONDS);

        firstNode.context().cache().context().exchange().lastTopologyFuture().listen(() -> {
            try {
                firstNodePmeFinishedLatch.countDown();

                firstNodeStopUnblockedLatch.await(getTestTimeout(), MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        IgniteInternalFuture<Object> firstNodeStopFut = GridTestUtils.runAsync(() -> stopGrid(1));

        firstNodePmeUnblockedLatch.countDown();

        firstNodePmeFinishedLatch.await(getTestTimeout(), MILLISECONDS);

        zeroNodePmeFinishedLatch.await(getTestTimeout(), MILLISECONDS);

        int keyForFirstNode = keyForNode(grid(0).affinity(DEFAULT_CACHE_NAME), new AtomicInteger(), firstNode.localNode());

        NEAR_UPDATE_RESP_LOG_LSNR.reset();

        setLoggerDebugLevel();

        IgniteInternalFuture<Object> putFut = GridTestUtils.runAsync(() ->
            grid(0).cache(DEFAULT_CACHE_NAME).put(keyForFirstNode, "test-val"));

        NEAR_UPDATE_RESP_LOG_LSNR.check(getTestTimeout());

        firstNodeStopUnblockedLatch.countDown();

        firstNodeStopFut.get(getTestTimeout(), MILLISECONDS);

        putFut.get(getTestTimeout(), MILLISECONDS);

        assertEquals("test-val", grid(0).cache(DEFAULT_CACHE_NAME).get(keyForFirstNode));
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }
}
