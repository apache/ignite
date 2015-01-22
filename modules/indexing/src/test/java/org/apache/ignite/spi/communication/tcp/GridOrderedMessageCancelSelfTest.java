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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.apache.ignite.internal.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCachePreloadMode.*;

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
        cache.setPreloadMode(NONE);

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
    public void testQuery() throws Exception {
        GridCacheQueryFuture<Map.Entry<Object, Object>> fut =
            grid(0).cache(null).queries().createSqlQuery(String.class, "_key is not null").execute();

        testMessageSet(fut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTask() throws Exception {
        ComputeTaskFuture<?> fut = executeAsync(compute(grid(0).forRemotes()), Task.class, null);

        testMessageSet(fut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskException() throws Exception {
        ComputeTaskFuture<?> fut = executeAsync(compute(grid(0).forRemotes()), FailTask.class, null);

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

        Map map = U.field(((GridKernal)grid(0)).context().io(), "msgSetMap");

        info("Map: " + map);

        assertTrue(map.isEmpty());
    }

    /**
     * Communication SPI.
     */
    private static class CommunicationSpi extends TcpCommunicationSpi {
        /** */
        @IgniteMarshallerResource
        private IgniteMarshaller marsh;

        /** {@inheritDoc} */
        @Override protected void notifyListener(UUID sndId, GridTcpCommunicationMessageAdapter msg,
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
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) throws IgniteCheckedException {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }

    /**
     * Test task.
     */
    @ComputeTaskSessionFullSupport
    private static class FailTask extends ComputeTaskSplitAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) throws IgniteCheckedException {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() throws IgniteCheckedException {
                    throw new IgniteCheckedException("Task failed.");
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }
}
