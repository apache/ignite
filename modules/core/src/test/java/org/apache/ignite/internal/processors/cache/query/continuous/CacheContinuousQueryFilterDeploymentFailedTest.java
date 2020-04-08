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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.continuous.StopRoutineDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.apache.ignite.testframework.GridTestUtils.DiscoverySpiListenerWrapper;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests the behavior of continuous query registration in case the remote node failed to obtain the filter deployment.
 */
public class CacheContinuousQueryFilterDeploymentFailedTest extends GridCommonAbstractTest {
    /** The name of the CQ filter factory class. Its obtaining on a non-local node requires P2P class loading. */
    private static final String EXT_FILTER_FACTORY_CLS =
        "org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilterFactory";

    /** Latch that indicates whether {@link StopRoutineDiscoveryMessage} was processed by all nodes. */
    private volatile CountDownLatch stopRoutineMsgProcessed;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DiscoveryHook discoveryHook = new DiscoveryHook() {
            @Override public void handleProcessedDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                DiscoveryCustomMessage customMsg = msg == null ?
                    null : (DiscoveryCustomMessage)U.field(msg, "delegate");

                if (customMsg instanceof StopRoutineDiscoveryMessage)
                    stopRoutineMsgProcessed.countDown();
            }
        };

        TcpDiscoverySpi spi = new TcpDiscoverySpi() {
            @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                super.setListener(DiscoverySpiListenerWrapper.wrap(lsnr, discoveryHook));
            }
        };

        spi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setNetworkTimeout(1000);
        cfg.setDiscoverySpi(spi);

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
        IgniteEx ignite = startGrids(2);

        IgniteEx client = startClientGrid(2);

        ignite.cluster().state(ACTIVE);

        client.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        Class<Factory<CacheEntryEventFilter<Integer, Integer>>> rmtFilterFactoryCls =
            (Class<Factory<CacheEntryEventFilter<Integer, Integer>>>)getExternalClassLoader()
                .loadClass(EXT_FILTER_FACTORY_CLS);

        Factory<CacheEntryEventFilter<Integer, Integer>> rmtFilterFactory = rmtFilterFactoryCls.newInstance();

        qry.setRemoteFilterFactory(rmtFilterFactory);

        stopRoutineMsgProcessed = new CountDownLatch(3);

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof GridDeploymentRequest);

        assertThrowsWithCause(() -> client.cache(DEFAULT_CACHE_NAME).query(qry), CacheException.class);

        List<Ignite> grids = G.allGrids();

        assertTrue(stopRoutineMsgProcessed.await(getTestTimeout(), MILLISECONDS));

        assertTrue(grids.stream().allMatch(g -> {
            SystemView<ContinuousQueryView> locQrys = ((IgniteEx)g).context().systemView().view(CQ_SYS_VIEW);

            return locQrys.size() == 0;
        }));
    }
}
