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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** */
public class AtomicCacheOperationRemappingOnNodeStopTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        return super.getConfiguration().setFailureHandler(new StopNodeOrHaltFailureHandler());
    }

    /** */
    @Test
    public void testPut() throws Exception {
        doTest(false);
    }

    /** */
    @Test
    public void testPutAll() throws Exception {
        doTest(false);
    }

    /** */
    private void doTest(boolean putAll) throws Exception {
        IgniteEx node0 = startGrids(3);

        node0.createCache(DEFAULT_CACHE_NAME);

        IgniteEx node1 = grid(1);

        CountDownLatch node0PmeFinishedLatch = new CountDownLatch(1);
        CountDownLatch node1PmeStartedLatch = new CountDownLatch(1);
        CountDownLatch node1PmeProceedLatch = new CountDownLatch(1);
        CountDownLatch node1PmeFinishedLatch = new CountDownLatch(1);
        CountDownLatch node1StoppageBockedLatch = new CountDownLatch(1);

        node0.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                node0PmeFinishedLatch.countDown();
            }
        });

        node1.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    node1PmeStartedLatch.countDown();

                    node1PmeProceedLatch.await(getTestTimeout(), MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        stopGrid(2);

        // Node 1 started PME but isn't proceeding.
        node1PmeStartedLatch.await(getTestTimeout(), MILLISECONDS);

        node1.context().cache().context().exchange().lastTopologyFuture().listen(() -> {
            try {
                node1PmeFinishedLatch.countDown();

                node1StoppageBockedLatch.await(getTestTimeout(), MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(0, node0.cache(DEFAULT_CACHE_NAME).size());

        IgniteInternalFuture<Object> node1StopFut = GridTestUtils.runAsync(() -> stopGrid(1));

        // Node 1 proceeds PME.
        node1PmeProceedLatch.countDown();

        node1PmeFinishedLatch.await(getTestTimeout(), MILLISECONDS);

        node0PmeFinishedLatch.await(getTestTimeout(), MILLISECONDS);

        node0.context().io().addMessageListener(GridTopic.TOPIC_CACHE, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof GridNearAtomicUpdateResponse && nodeId.equals(node1.localNode().id()))
                    node1StoppageBockedLatch.countDown();
            }
        });

        if (putAll) {
            node0.cache(DEFAULT_CACHE_NAME).putAll(Collections.singletonMap(
                keyForNode(node0.affinity(DEFAULT_CACHE_NAME), new AtomicInteger(), node1.localNode()),
                "test-val"
            ));
        }
        else {
            node0.cache(DEFAULT_CACHE_NAME).put(
                keyForNode(node0.affinity(DEFAULT_CACHE_NAME), new AtomicInteger(), node1.localNode()),
                "test-val"
            );
        }

        node1StopFut.get(getTestTimeout(), MILLISECONDS);

        assertEquals(1, node0.cache(DEFAULT_CACHE_NAME).size());
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20_000;
    }
}
