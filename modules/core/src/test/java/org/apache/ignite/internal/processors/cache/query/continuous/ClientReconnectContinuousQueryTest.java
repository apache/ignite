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
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ClientReconnectContinuousQueryTest extends GridCommonAbstractTest {
    /** Client index. */
    private static final int CLIENT_IDX = 1;

    /** Puts before reconnect. */
    private static final int PUTS_BEFORE_RECONNECT = 50;

    /** Puts after reconnect. */
    private static final int PUTS_AFTER_RECONNECT = 50;

    /** Recon latch. */
    private static final CountDownLatch reconLatch = new CountDownLatch(1);

    /** Discon latch. */
    private static final CountDownLatch disconLatch = new CountDownLatch(1);

    /** Updater received. */
    private static final CountDownLatch updaterReceived = new CountDownLatch(PUTS_BEFORE_RECONNECT);

    /** Receiver after reconnect. */
    private static final CountDownLatch receiverAfterReconnect = new CountDownLatch(PUTS_AFTER_RECONNECT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();

        commSpi.setSlowClientQueueLimit(50);
        commSpi.setIdleConnectionTimeout(300_000);

        if (!getTestIgniteInstanceName(CLIENT_IDX).equals(gridName)) {
            CacheConfiguration ccfg = defaultCacheConfiguration();

            ccfg.setAtomicityMode(atomicityMode());

            // TODO IGNITE-9530 Remove this clause.
            if (atomicityMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
                ccfg.setNearConfiguration(null);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @return Transaction snapshot.
     */
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     * Test client reconnect to alive grid.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        try {
            startGrid(0);

            final IgniteEx client = startClientGrid(CLIENT_IDX);

            client.events().localListen(new DisconnectListener(), EventType.EVT_CLIENT_NODE_DISCONNECTED);

            client.events().localListen(new ReconnectListener(), EventType.EVT_CLIENT_NODE_RECONNECTED);

            IgniteCache cache = client.cache(DEFAULT_CACHE_NAME);

            ContinuousQuery qry = new ContinuousQuery();

            qry.setLocalListener(new CQListener());

            cache.query(qry);

            putSomeKeys(PUTS_BEFORE_RECONNECT);

            info("updaterReceived Count: " + updaterReceived.getCount());

            assertTrue(updaterReceived.await(10_000, TimeUnit.MILLISECONDS));

            skipRead(client, true);

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    assertTrue(disconLatch.await(10_000, TimeUnit.MILLISECONDS));

                    skipRead(client, false);

                    return null;
                }
            });

            putSomeKeys(1_000);

            fut.get();

            assertTrue(reconLatch.await(10_000, TimeUnit.MILLISECONDS));

            putSomeKeys(PUTS_AFTER_RECONNECT);

            info("receiverAfterReconnect Count: " + receiverAfterReconnect.getCount());

            assertTrue(receiverAfterReconnect.await(10_000, TimeUnit.MILLISECONDS));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class ReconnectListener implements IgnitePredicate<Event> {
        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            reconLatch.countDown();

            return false;
        }
    }

    /**
     *
     */
    private static class DisconnectListener implements IgnitePredicate<Event> {
        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            disconLatch.countDown();

            return false;
        }
    }

    /**
     *
     */
    private static class CQListener implements CacheEntryUpdatedListener {
        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
            if (reconLatch.getCount() != 0) {
                for (Object o : iterable)
                    updaterReceived.countDown();
            }
            else {
                for (Object o : iterable)
                    receiverAfterReconnect.countDown();
            }
        }
    }

    /**
     * @param cnt Number of keys.
     */
    private void putSomeKeys(int cnt) {
        IgniteEx ignite = grid(0);

        IgniteCache<Object, Object> srvCache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < cnt; i++)
            srvCache.put(0, i);
    }

    /**
     * @param igniteClient Ignite client.
     * @param skip Skip.
     */
    private void skipRead(IgniteEx igniteClient, boolean skip) {
        GridIoManager ioMgr = igniteClient.context().io();

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)((Object[])U.field(ioMgr, "spis"))[0];

        GridNioServer nioSrvr = U.field(commSpi, "nioSrvr");

        GridTestUtils.setFieldValue(nioSrvr, "skipRead", skip);
    }
}
