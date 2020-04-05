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

import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Tests the behavior of continuous query registration in case the remote node failed to obtain the filter deployment.
 */
public class CacheContinuousQueryFilterDeploymentFailedTest extends GridCommonAbstractTest {
    /** The name of the CQ filter factory class. Its obtaining on a non-local node requires P2P class loading. */
    private static final String EXT_FILTER_FACTORY_CLS =
        "org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilterFactory";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setNetworkTimeout(1000);

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
        startGrids(2).cluster().state(ClusterState.ACTIVE);

        grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        Class<Factory<CacheEntryEventFilter<Integer, Integer>>> rmtFilterFactoryCls =
            (Class<Factory<CacheEntryEventFilter<Integer, Integer>>>)getExternalClassLoader()
                .loadClass(EXT_FILTER_FACTORY_CLS);

        Factory<CacheEntryEventFilter<Integer, Integer>> rmtFilterFactory = rmtFilterFactoryCls.newInstance();

        qry.setRemoteFilterFactory(rmtFilterFactory);

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof GridDeploymentRequest);

        assertThrowsAnyCause(
            log,
            () -> grid(0).cache(DEFAULT_CACHE_NAME).query(qry),
            CacheException.class,
            "Failed to start continuous query."
        );

        assertEquals(0, grid(0).context().systemView().view(CQ_SYS_VIEW).size());
        assertEquals(0, grid(1).context().systemView().view(CQ_SYS_VIEW).size());
    }
}
