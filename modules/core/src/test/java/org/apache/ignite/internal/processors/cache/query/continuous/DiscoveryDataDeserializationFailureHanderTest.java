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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class DiscoveryDataDeserializationFailureHanderTest extends GridCommonAbstractTest {
    /** */
    private FailureHandler failureHnd;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(failureHnd);
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testFailureHander() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        failureHnd = new TestFailureHandler(latch);

        Ignite node1 = startGrid(1);

        IgniteCache<Integer, Integer> cache = node1.getOrCreateCache("cache");

        cache.query(continuousQuery());

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> startGrid(2), 1, "node-starter");

        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        }
        finally {
            GridTestUtils.assertThrows(log, (Callable<Object>)fut::get, IgniteCheckedException.class, "Failed to start");
        }
    }

    /**
     * @return Continuous query with a remote filter from an external class loader.
     */
    private ContinuousQuery<Integer, Integer> continuousQuery() throws Exception {
        final Class<Factory<CacheEntryEventFilter<Integer, Integer>>> evtFilterFactory =
            (Class<Factory<CacheEntryEventFilter<Integer, Integer>>>)getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilterFactory");

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setLocalListener(evts -> {});

        qry.setRemoteFilterFactory(evtFilterFactory.newInstance());

        return qry;
    }

    /** */
    private static class TestFailureHandler implements FailureHandler {
        /** */
        private final CountDownLatch latch;

        /**
         * @param latch Latch to call on failures.
         */
        private TestFailureHandler(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            latch.countDown();

            return true;
        }
    }
}
