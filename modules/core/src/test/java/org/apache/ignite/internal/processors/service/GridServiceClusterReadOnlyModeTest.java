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

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeAbstractTest;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertCachesReadOnlyMode;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks that updates from deployed service will be rejected, if cluster in a {@link ClusterState#ACTIVE_READ_ONLY}
 * mode.
 */
public class GridServiceClusterReadOnlyModeTest extends ClusterReadOnlyModeAbstractTest {
    /** Wait timeout. */
    private static final long WAIT_TIMEOUT = 60_000L;

    /** Barrier. */
    private static final CyclicBarrier BARRIER = new CyclicBarrier(2);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).services().cancelAll();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).services().cancelAll();

        super.afterTest();
    }

    /** */
    @Test
    public void test() throws BrokenBarrierException, InterruptedException, TimeoutException {
        grid(0).services().deployClusterSingleton("updater-service", new CacheUpdaterService());

        BARRIER.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS);

        changeClusterReadOnlyMode(true);

        BARRIER.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS);

        changeClusterReadOnlyMode(false);

        BARRIER.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     *
     */
    private static class CacheUpdaterService implements Service {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            assertCachesReadOnlyMode(ignite, false, CACHE_NAMES);

            BARRIER.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS);

            assertTrue(waitForCondition(() -> ignite.cluster().state() == ACTIVE_READ_ONLY, WAIT_TIMEOUT));

            assertCachesReadOnlyMode(ignite, true, CACHE_NAMES);

            BARRIER.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS);

            assertTrue(waitForCondition(() -> ignite.cluster().state() != ACTIVE_READ_ONLY, WAIT_TIMEOUT));

            assertCachesReadOnlyMode(ignite, false, CACHE_NAMES);

            BARRIER.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }
}
