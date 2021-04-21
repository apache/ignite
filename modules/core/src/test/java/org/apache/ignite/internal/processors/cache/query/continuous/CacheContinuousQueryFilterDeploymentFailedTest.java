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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.CountDownLatch;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.continuous.StopRoutineDiscoveryMessage;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.Ignition.allGrids;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests the behavior of continuous query registration in case the remote node failed to obtain the filter deployment.
 */
public class CacheContinuousQueryFilterDeploymentFailedTest extends GridCommonAbstractTest {
    /** Number of test nodes. */
    private static final int NODES_CNT = 3;

    /** Latch that indicates whether {@link StopRoutineDiscoveryMessage} was processed by all nodes. */
    private final CountDownLatch stopRoutineLatch = new CountDownLatch(NODES_CNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Failure detection timeout > P2P class loading timeout which is set as network timeout.
        cfg.setFailureDetectionTimeout(cfg.getNetworkTimeout() * 2);

        ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(new DiscoveryHook() {
            @Override public void afterDiscovery(DiscoveryCustomMessage customMsg) {
                if (customMsg instanceof StopRoutineDiscoveryMessage)
                    stopRoutineLatch.countDown();
            }
        });

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /**
     * Tests continuous query behavior in case of filter deployment obtaining failure.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings({"ThrowableNotThrown"})
    public void testContinuousQueryFilterDeploymentFailed() throws Exception {
        startGrids(NODES_CNT - 1);

        IgniteEx cli = startClientGrid(NODES_CNT - 1);

        IgniteCache<Integer, Integer> cache = cli.createCache(DEFAULT_CACHE_NAME);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        Class<Factory<CacheEntryEventFilter<Integer, Integer>>> rmtFilterFactoryCls =
            (Class<Factory<CacheEntryEventFilter<Integer, Integer>>>)getExternalClassLoader()
                .loadClass("org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilterFactory");

        qry.setRemoteFilterFactory(rmtFilterFactoryCls.newInstance());

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof GridDeploymentRequest);

        assertThrowsWithCause(() -> cache.query(qry), CacheException.class);

        assertTrue(stopRoutineLatch.await(getTestTimeout(), MILLISECONDS));

        assertTrue(allGrids().stream().allMatch(g ->
            ((IgniteEx)g).context().systemView().view(CQ_SYS_VIEW).size() == 0));
    }
}
