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

package org.apache.ignite.spi.communication.tcp;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponse;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;

/**
 *
 */
public class GridOrderedMessageCancelSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cancel latch. */
    private static CountDownLatch cancelLatch;

    /** Process response latch. */
    private static CountDownLatch resLatch;

    /** Finish latch. */
    private static CountDownLatch finishLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setRebalanceMode(NONE);

        cfg.setCacheConfiguration(cache);

        cfg.setCommunicationSpi(new CommunicationSpi());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cancelLatch = new CountDownLatch(1);
        resLatch = new CountDownLatch(1);
        finishLatch = new CountDownLatch(1);

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTask() throws Exception {
        ComputeTaskFuture<?> fut = executeAsync(compute(grid(0).cluster().forRemotes()), Task.class, null);

        testMessageSet(fut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskException() throws Exception {
        ComputeTaskFuture<?> fut = executeAsync(compute(grid(0).cluster().forRemotes()), FailTask.class, null);

        testMessageSet(fut);
    }

    /**
     * @param fut Future to cancel.
     * @throws Exception If failed.
     */
    private void testMessageSet(IgniteFuture<?> fut) throws Exception {
        cancelLatch.await();

        assertTrue(fut.cancel());

        resLatch.countDown();

        assertTrue(U.await(finishLatch, 5000, MILLISECONDS));

        Map map = U.field(((IgniteKernal)grid(0)).context().io(), "msgSetMap");

        info("Map: " + map);

        assertTrue(map.isEmpty());
    }

    /**
     * @param fut Future to cancel.
     * @throws Exception If failed.
     */
    private void testMessageSet(IgniteInternalFuture<?> fut) throws Exception {
        cancelLatch.await();

        assertTrue(fut.cancel());

        resLatch.countDown();

        assertTrue(U.await(finishLatch, 5000, MILLISECONDS));

        Map map = U.field(((IgniteKernal)grid(0)).context().io(), "msgSetMap");

        info("Map: " + map);

        assertTrue(map.isEmpty());
    }

    /**
     * Communication SPI.
     */
    private static class CommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected void notifyListener(UUID sndId, Message msg,
            IgniteRunnable msgC) {
            try {
                GridIoMessage ioMsg = (GridIoMessage)msg;

                boolean wait = ioMsg.message() instanceof GridCacheQueryResponse ||
                        ioMsg.message() instanceof GridJobExecuteResponse;

                if (wait) {
                    cancelLatch.countDown();

                    assertTrue(U.await(resLatch, 5000, MILLISECONDS));
                }

                super.notifyListener(sndId, msg, msgC);

                if (wait)
                    finishLatch.countDown();
            }
            catch (Exception e) {
                fail("Unexpected error: " + e);
            }
        }
    }

    /**
     * Test task.
     */
    @ComputeTaskSessionFullSupport
    private static class Task extends ComputeTaskSplitAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Test task.
     */
    @ComputeTaskSessionFullSupport
    private static class FailTask extends ComputeTaskSplitAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    throw new IgniteException("Task failed.");
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}