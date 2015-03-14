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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Tests timeout exception when message gets lost.
 */
public class GridCacheAtomicTimeoutSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    public static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(1);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setDistributionMode(PARTITIONED_ONLY);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setNetworkTimeout(3000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            final IgniteKernal grid = (IgniteKernal)grid(i);

            TestCommunicationSpi commSpi = (TestCommunicationSpi)grid.configuration().getCommunicationSpi();

            commSpi.skipNearRequest = false;
            commSpi.skipNearResponse = false;
            commSpi.skipDhtRequest = false;
            commSpi.skipDhtResponse = false;

            GridTestUtils.retryAssert(log, 10, 100, new CA() {
                @Override public void apply() {
                    assertTrue(grid.internalCache().context().mvcc().atomicFutures().isEmpty());
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearUpdateRequestLost() throws Exception {
        Ignite ignite = grid(0);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();

        IgniteCache<Object, Object> cache = ignite.jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        int key = keyForTest();

        cache.put(key, 0);

        commSpi.skipNearRequest = true;

        cacheAsync.put(key, 1);

        IgniteFuture<?> fut = cacheAsync.future();

        Map<UUID, GridCommunicationClient> clients = U.field(commSpi, "clients");

        GridTcpNioCommunicationClient client = (GridTcpNioCommunicationClient)clients.get(grid(1).localNode().id());

        client.session().close().get();

        try {
            fut.get();

            fail();
        }
        catch (IgniteException e) {
            if (!(e.getCause() instanceof CacheAtomicUpdateTimeoutCheckedException))
                throw e;

            // Expected exception.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearUpdateResponseLost() throws Exception {
        Ignite ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        int key = keyForTest();

        cache.put(key, 0);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(1).configuration().getCommunicationSpi();

        commSpi.skipNearResponse = true;

        cacheAsync.put(key, 1);

        IgniteFuture<?> fut = cacheAsync.future();

        Map<UUID, GridCommunicationClient> clients = U.field(commSpi, "clients");

        GridTcpNioCommunicationClient client = (GridTcpNioCommunicationClient)clients.get(grid(0).localNode().id());

        client.session().close().get();

        try {
            fut.get();

            fail();
        }
        catch (IgniteException e) {
            if (!(e.getCause() instanceof CacheAtomicUpdateTimeoutCheckedException))
                throw e;

            // Expected exception.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDhtUpdateRequestLost() throws Exception {
        Ignite ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        int key = keyForTest();

        cache.put(key, 0);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(1).configuration().getCommunicationSpi();

        commSpi.skipDhtRequest = true;

        cacheAsync.put(key, 1);

        IgniteFuture<?> fut = cacheAsync.future();

        Map<UUID, GridCommunicationClient> clients = U.field(commSpi, "clients");

        GridTcpNioCommunicationClient client = (GridTcpNioCommunicationClient)clients.get(grid(2).localNode().id());

        client.session().close().get();

        try {
            fut.get();

            fail();
        }
        catch (IgniteException e) {
            assertTrue("Invalid exception thrown: " + e, X.hasCause(e, CacheAtomicUpdateTimeoutCheckedException.class)
                || X.hasSuppressed(e, CacheAtomicUpdateTimeoutCheckedException.class));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDhtUpdateResponseLost() throws Exception {
        Ignite ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        int key = keyForTest();

        cache.put(key, 0);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(2).configuration().getCommunicationSpi();

        commSpi.skipDhtResponse = true;

        cacheAsync.put(key, 1);

        IgniteFuture<?> fut = cacheAsync.future();

        Map<UUID, GridCommunicationClient> clients = U.field(commSpi, "clients");

        GridTcpNioCommunicationClient client = (GridTcpNioCommunicationClient)clients.get(grid(1).localNode().id());

        client.session().close().get();

        try {
            fut.get();

            fail();
        }
        catch (IgniteException e) {
            assertTrue("Invalid exception thrown: " + e, X.hasCause(e, CacheAtomicUpdateTimeoutCheckedException.class)
                || X.hasSuppressed(e, CacheAtomicUpdateTimeoutCheckedException.class));
        }
    }

    /**
     * @return Key for test;
     */
    private int keyForTest() {
        int i = 0;

        CacheAffinity<Object> aff = grid(0).affinity(null);

        while (!aff.isPrimary(grid(1).localNode(), i) || !aff.isBackup(grid(2).localNode(), i))
            i++;

        return i;
    }

    /**
     * Communication SPI that will count single partition update messages.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private boolean skipNearRequest;

        /** */
        private boolean skipNearResponse;

        /** */
        private boolean skipDhtRequest;

        /** */
        private boolean skipDhtResponse;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg)
            throws IgniteSpiException {
            if (!skipMessage((GridIoMessage)msg))
                super.sendMessage(node, msg);
        }

        /**
         * Checks if message should be skipped.
         *
         * @param msg Message.
         */
        private boolean skipMessage(GridIoMessage msg) {
            return msg.message() instanceof GridNearAtomicUpdateRequest && skipNearRequest
                || msg.message() instanceof GridNearAtomicUpdateResponse && skipNearResponse
                || msg.message() instanceof GridDhtAtomicUpdateRequest && skipDhtRequest
                || msg.message() instanceof GridDhtAtomicUpdateResponse && skipDhtResponse;
        }
    }

}
