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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheContinuousQueryReconnectTest extends GridCommonAbstractTest implements Serializable {
    /** */
    final private static AtomicInteger cnt = new AtomicInteger();

    /** */
    private volatile boolean isClient = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        if (isClient)
            cfg.setClientMode(true);

        return cfg;
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
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
    public void testReconnectServer() throws Exception {
        testReconnect(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectClient() throws Exception {
        testReconnect(true);
    }

    /**
     *
     */
    private void putAndCheck(IgniteCache<Object, Object> cache, int diff) {
        cnt.set(0);

        cache.put(1, "1");

        assertEquals(diff, cnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    private void testReconnect(boolean clientQuery) throws Exception {
        Ignite srv1 = startGrid(0);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
                // No-op.
            }
        });

        qry.setAutoUnsubscribe(false);

        qry.setRemoteFilter(new CacheEntryEventSerializableFilter<Object, Object>() {
            @Override public boolean evaluate(CacheEntryEvent<?, ?> event) throws CacheEntryListenerException {
                cnt.incrementAndGet();

                return true;
            }
        });

        isClient = true;

        Ignite client = startGrid(1);

        isClient = false;

        IgniteCache<Object, Object> cache1 = srv1.cache(null);
        IgniteCache<Object, Object> clCache = client.cache(null);

        putAndCheck(clCache, 0); // 0 remote listeners.

        (clientQuery ? clCache : cache1).query(qry);

        putAndCheck(clCache, 1); // 1 remote listener.

        startGrid(2);

        putAndCheck(clCache, 2); // 2 remote listeners.

        stopGrid(0);

        while (true) {
            try {
                clCache.get(1);

                break;
            }
            catch (IgniteClientDisconnectedException e) {
                e.reconnectFuture().get(); // Wait for reconnect.

            }
            catch (CacheException e) {
                if (e.getCause() instanceof IgniteClientDisconnectedException)
                    ((IgniteClientDisconnectedException)e.getCause()).reconnectFuture().get(); // Wait for reconnect.
            }
        }

        putAndCheck(clCache, 1); // 1 remote listener.

        startGrid(3);

        putAndCheck(clCache, 2); // 2 remote listeners.

        stopGrid(1); // Client node.

        isClient = true;

        client = startGrid(4);

        isClient = false;

        clCache = client.cache(null);

        putAndCheck(clCache, 2); // 2 remote listeners.

        startGrid(5);

        putAndCheck(clCache, 3); // 3 remote listeners.
    }
}
