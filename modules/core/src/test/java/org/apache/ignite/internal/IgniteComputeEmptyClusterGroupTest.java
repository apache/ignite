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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class IgniteComputeEmptyClusterGroupTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsync() throws Exception {
        ClusterGroup empty = ignite(0).cluster().forNodeId(UUID.randomUUID());

        assertEquals(0, empty.nodes().size());

        IgniteCompute comp = ignite(0).compute(empty).withAsync();

        comp.affinityRun(null, 1, new FailRunnable());

        checkFutureFails(comp);

        comp.apply(new FailClosure(), new Object());

        checkFutureFails(comp);

        comp.affinityCall(null, 1, new FailCallable());

        checkFutureFails(comp);

        comp.broadcast(new FailCallable());

        checkFutureFails(comp);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSync() throws Exception {
        ClusterGroup empty = ignite(0).cluster().forNodeId(UUID.randomUUID());

        assertEquals(0, empty.nodes().size());

        final IgniteCompute comp = ignite(0).compute(empty);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                comp.affinityRun(null, 1, new FailRunnable());

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
            @Override
            public Void call() throws Exception {
                comp.affinityCall(null, 1, new FailCallable());

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
     * @param comp Compute.
     */
    private void checkFutureFails(IgniteCompute comp) {
        final ComputeTaskFuture fut = comp.future();

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
