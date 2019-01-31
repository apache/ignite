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

package org.apache.ignite.internal;

import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteComputeEmptyClusterGroupTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAsync() throws Exception {
        ClusterGroup empty = ignite(0).cluster().forNodeId(UUID.randomUUID());

        assertEquals(0, empty.nodes().size());

        IgniteCompute comp = ignite(0).compute(empty);

        checkFutureFails(comp.affinityRunAsync(DEFAULT_CACHE_NAME, 1, new FailRunnable()));

        checkFutureFails(comp.applyAsync(new FailClosure(), new Object()));

        checkFutureFails(comp.affinityCallAsync(DEFAULT_CACHE_NAME, 1, new FailCallable()));

        checkFutureFails(comp.broadcastAsync(new FailCallable()));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSync() throws Exception {
        ClusterGroup empty = ignite(0).cluster().forNodeId(UUID.randomUUID());

        assertEquals(0, empty.nodes().size());

        final IgniteCompute comp = ignite(0).compute(empty);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                comp.affinityRun(DEFAULT_CACHE_NAME, 1, new FailRunnable());

                return null;
            }
        }, ClusterGroupEmptyException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                comp.apply(new FailClosure(), new Object());

                return null;
            }
        }, ClusterGroupEmptyException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                comp.affinityCall(DEFAULT_CACHE_NAME, 1, new FailCallable());

                return null;
            }
        }, ClusterGroupEmptyException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                comp.broadcast(new FailCallable());

                return null;
            }
        }, ClusterGroupEmptyException.class, null);
    }

    /**
     * @param fut Future.
     */
    private void checkFutureFails(final IgniteFuture fut) {
        assertNotNull(fut);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                fut.get();

                return null;
            }
        }, ClusterGroupEmptyException.class, null);
    }

    /**
     *
     */
    private static class FailClosure implements IgniteClosure<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(Object o) {
            fail();

            return null;
        }
    }

    /**
     *
     */
    private static class FailRunnable implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            fail();
        }
    }

    /**
     *
     */
    private static class FailCallable implements IgniteCallable<Object> {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            fail();

            return null;
        }
    }
}
