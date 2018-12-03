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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteClientReconnectFailoverTest extends IgniteClientReconnectFailoverAbstractTest {
    /** */
    protected static final String ATOMIC_CACHE = "ATOMIC_CACHE";

    /** */
    protected static final String TX_CACHE = "TX_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg1 = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg1.setName(ATOMIC_CACHE);
        ccfg1.setBackups(1);
        ccfg1.setAtomicityMode(ATOMIC);

        CacheConfiguration ccfg2 = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg2.setName(TX_CACHE);
        ccfg2.setBackups(1);
        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectAtomicCache() throws Exception {
        final Ignite client = grid(serverCount());

        final IgniteCache<Integer, Integer> cache = client.cache(ATOMIC_CACHE);

        assertNotNull(cache);

        assertEquals(ATOMIC, cache.getConfiguration(CacheConfiguration.class).getAtomicityMode());

        reconnectFailover(new Callable<Void>() {
            @Override public Void call() throws Exception {
                TreeMap<Integer, Integer> map = new TreeMap<>();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 10; i++) {
                    Integer key = rnd.nextInt(0, 100_000);

                    cache.put(key, key);

                    assertEquals(key, cache.get(key));

                    map.put(key, key);
                }

                cache.putAll(map);

                Map<Integer, Integer> res = cache.getAll(map.keySet());

                assertEquals(map, res);

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectTxCache() throws Exception {
        final Ignite client = grid(serverCount());

        final IgniteCache<Integer, Integer> cache = client.cache(TX_CACHE);

        assertNotNull(cache);

        assertEquals(TRANSACTIONAL, cache.getConfiguration(CacheConfiguration.class).getAtomicityMode());

        final IgniteTransactions txs = client.transactions();

        reconnectFailover(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    TreeMap<Integer, Integer> map = new TreeMap<>();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < 5; i++) {
                        Integer key = rnd.nextInt(0, 100_000);

                        cache.put(key, key);

                        assertEquals(key, cache.get(key));

                        map.put(key, key);
                    }

                    for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
                        try (Transaction tx = txs.txStart(txConcurrency, REPEATABLE_READ)) {
                            for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                                cache.put(e.getKey(), e.getValue());

                                assertNotNull(cache.get(e.getKey()));
                            }

                            tx.commit();
                        }
                    }

                    cache.putAll(map);

                    Map<Integer, Integer> res = cache.getAll(map.keySet());

                    assertEquals(map, res);
                }
                catch (IgniteClientDisconnectedException e) {
                    throw e;
                }
                catch (IgniteException e) {
                    log.info("Ignore error: " + e);
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof IgniteClientDisconnectedException)
                        throw e;
                    else
                        log.info("Ignore error: " + e);
                }

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectComputeApi() throws Exception {
        final Ignite client = grid(serverCount());

        final IgniteCompute comp = client.compute();

        reconnectFailover(new Callable<Void>() {
            @Override public Void call() throws Exception {
                comp.call(new DummyClosure());

                comp.broadcast(new DummyClosure());

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectStreamerApi() throws Exception {
        final Ignite client = grid(serverCount());

        reconnectFailover(new Callable<Void>() {
            @Override public Void call() throws Exception {
                stream(ATOMIC_CACHE);

                stream(TX_CACHE);

                return null;
            }

            private void stream(String cacheName) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                try (IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(cacheName)) {
                    streamer.allowOverwrite(true);

                    streamer.perNodeBufferSize(10);

                    for (int i = 0; i < 100; i++)
                        streamer.addData(rnd.nextInt(100_000), 0);
                }
            }
        });
    }

    /**
     *
     */
    public static class DummyClosure implements IgniteCallable<Object> {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return 1;
        }
    }
}
