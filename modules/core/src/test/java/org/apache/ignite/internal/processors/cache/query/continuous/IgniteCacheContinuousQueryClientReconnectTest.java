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

import java.util.concurrent.CountDownLatch;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.resources.LoggerResource;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheContinuousQueryClientReconnectTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Atomic mode.
     */
    protected CacheAtomicityMode atomicMode() {
        return ATOMIC;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectClient() throws Exception {
        Ignite client = grid(serverCount());

        Ignite srv = clientRouter(client);

        assertTrue(client.cluster().localNode().isClient());

        final CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        IgniteCache<Object, Object> clnCache = client.cache(DEFAULT_CACHE_NAME);

        QueryCursor<?> cur = clnCache.query(qry);

        int keyCnt = 100;

        for (int i = 0; i < 10; i++) {
            lsnr.latch = new CountDownLatch(keyCnt);

            for (int key = 0; key < keyCnt; key++)
                clnCache.put(key, key);

            assertTrue("Failed to wait for event.", lsnr.latch.await(5, SECONDS));

            reconnectClientNode(client, srv, null);

            lsnr.latch = new CountDownLatch(keyCnt);

            for (int key = 0; key < keyCnt; key++)
                clnCache.put(key, key);

            assertTrue("Failed to wait for event.", lsnr.latch.await(5, SECONDS));
        }

        cur.close();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectClientAndLeftRouter() throws Exception {
        if (!tcpDiscovery())
            return;

        Ignite client = grid(serverCount());

        final Ignite srv = clientRouter(client);

        final String clnRouterName = srv.name();

        assertTrue(client.cluster().localNode().isClient());

        final CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        IgniteCache<Object, Object> clnCache = client.cache(DEFAULT_CACHE_NAME);

        QueryCursor<?> cur = clnCache.query(qry);

        int keyCnt = 100;

        lsnr.latch = new CountDownLatch(keyCnt);

        for (int key = 0; key < keyCnt; key++)
            clnCache.put(key, key);

        assertTrue("Failed to wait for event.", lsnr.latch.await(5, SECONDS));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                stopGrid(clnRouterName);
            }
        });

        assertFalse("Client connected to the same server node.", clnRouterName.equals(clientRouter(client).name()));

        lsnr.latch = new CountDownLatch(keyCnt);

        for (int key = 0; key < keyCnt; key++)
            clnCache.put(key, key);

        assertTrue("Failed to wait for event.", lsnr.latch.await(5, SECONDS));

        cur.close();
    }

    /**
     *
     */
    private static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object> {
        /** */
        private volatile CountDownLatch latch = new CountDownLatch(1);

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts) {
                log.info("Received cache event: " + evt);

                latch.countDown();
            }
        }
    }
}
