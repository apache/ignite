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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for specific user scenario.
 */
public class IgniteFailoverSelfTest extends IgniteCacheAbstractTest {
    /** */
    private static final int UPDATER_CNT = 64;

    /** */
    public static final int RANGE = 1000;

    /** */
    private static final int KEYS_CNT = 5;

    /** */
    protected IgniteCache<String, Long> cache;

    /** */
    protected ConcurrentMap<String, AtomicLong> map = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** */
    private volatile CountDownLatch latch = new CountDownLatch(1);

    /** */
    private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.equals(getTestGridName(gridCount() - 1)))
            cfg.setClientMode(true);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.PRIMARY;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalInvokeRetryConsistency() throws Exception {
        final int clientGrid = gridCount() - 1;

        assertTrue(ignite(clientGrid).configuration().isClientMode());

        cache = jcache(clientGrid);

        updateCache(cache);

        final AtomicBoolean stop = new AtomicBoolean();

        ArrayList<IgniteInternalFuture> updaterFuts = new ArrayList<>();

        for (int i = 0; i < UPDATER_CNT; i++) {
            final int finalI = i;
            IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Thread.currentThread().setName("update-thread-" + finalI);

                    assertTrue(latch.await(30_000, TimeUnit.MILLISECONDS));

                    int iter = 0;

                    while (!stop.get()) {
                        log.info("Start update: " + iter);

                        updateCache(cache);

                        log.info("End update: " + iter++);
                    }

                    log.info("Update iterations: " + iter);

                    return null;
                }
            });

            updaterFuts.add(updateFut);
        }

        IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Thread.currentThread().setName("restart-thread");

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    assertTrue(latch.await(30_000, TimeUnit.MILLISECONDS));

                    int node = rnd.nextInt(0, gridCount() - 1);

                    log.info("Stop node: " + node);

                    stopGrid(node);

                    U.sleep(100);

                    log.info("Start node: " + node);

                    startGrid(node);

                    latch = new CountDownLatch(1);

                    awaitPartitionMapExchange();

                    U.sleep(100);
                }

                return null;
            }
        });

        long endTime = System.currentTimeMillis() + 10 * 60_000;

        try {
            int iter = 0;

            while (System.currentTimeMillis() < endTime && !isDone(updaterFuts) && !restartFut.isDone()) {
                try {
                    log.info("Start checking cache: " + iter);

                    checkCache(cache);

                    log.info("End checking cache: " + iter++);
                }
                finally {
                    latch.countDown();
                }
            }

            log.info("Checking iteration: " + iter);
        }
        finally {
            latch.countDown();

            stop.set(true);
        }

        for (IgniteInternalFuture fut : updaterFuts)
            fut.get();

        restartFut.get();

        checkCache(cache);
    }

    /**
     * @param futs Futers.
     * @return {@code True} if all futures are done.
     */
    private static boolean isDone(ArrayList<IgniteInternalFuture> futs) {
        for (IgniteInternalFuture fut : futs) {
            if (!fut.isDone())
                return false;
        }

        return true;
    }

    private void checkCache(IgniteCache cache) {
        final IgniteCache<String, Long> c = cache;

        rwl.writeLock().lock();

        try {
            log.info("Start cache validation.");

            long startTime = U.currentTimeMillis();

            Map<String, Long> notEqualsCacheVals = new HashMap<>();
            Map<String, Long> notEqualsLocMapVals = new HashMap<>();

            for (int k = 0; k < RANGE; k++) {
                if (k % 10_000 == 0)
                    log.info("Start validation for keys like 'key-" + k + "-*'");

                int memberID = 0; //TODO redundant

                for (int i = 0; i < KEYS_CNT; i++) {
                    String key = "key-" + k + "-" + memberID + "-" + i;

                    Long cacheVal = c.get(key);

                    AtomicLong aVal = map.get(key);
                    Long mapVal = aVal != null ? aVal.get() : null;

                    if (!Objects.equals(cacheVal, mapVal)) {
                        notEqualsCacheVals.put(key, cacheVal);
                        notEqualsLocMapVals.put(key, mapVal);
                    }
                }
            }

            assert notEqualsCacheVals.size() == notEqualsLocMapVals.size() : "Invalid state " +
                "[cacheMapVals=" + notEqualsCacheVals + ", mapVals=" + notEqualsLocMapVals + "]";

            if (!notEqualsCacheVals.isEmpty()) {
                // Print all usefull information and finish.
                for (Map.Entry<String, Long> eLocMap : notEqualsLocMapVals.entrySet()) {
                    String key = eLocMap.getKey();
                    Long mapVal = eLocMap.getValue();
                    Long cacheVal = notEqualsCacheVals.get(key);

                    U.error(log, "Got different values [key='" + key
                        + "', cacheVal=" + cacheVal + ", localMapVal=" + mapVal + "]");
                }

                log.info("Local driver map contant:\n " + map);

                log.info("Cache content:");

                for (int k2 = 0; k2 < RANGE; k2++) {
                    int memberId = 0; // TODO redundant.

                    for (int i2 = 0; i2 < KEYS_CNT; i2++) {
                        String key2 = "key-" + k2 + "-" + memberId + "-" + i2;

                        Long val = c.get(key2);

                        if (val != null)
                            log.info("Entry [key=" + key2 + ", val=" + val + "]");
                    }
                }

                throw new IllegalStateException("Cache and local map are in inconsistent state.");
            }

            log.info("Cache validation successfully finished in "
                + (U.currentTimeMillis() - startTime) / 1000 + " sec.");
        }
        finally {
            rwl.writeLock().unlock();
        }
    }

    private void updateCache(IgniteCache cache) {
        final int k = ThreadLocalRandom.current().nextInt(RANGE);

        final String[] keys = new String[KEYS_CNT];

        assert keys.length > 0 : "Count of keys: " + keys.length;

        int clientId = 0; // TODO redundant.

        for (int i = 0; i < keys.length; i++)
            keys[i] = "key-" + k + "-" + clientId + "-" + i;

        for (String key : keys) {
            rwl.readLock().lock();

            try {
                cache.invoke(key, new IncrementCacheEntryProcessor());

                AtomicLong prevVal = map.putIfAbsent(key, new AtomicLong(0));

                if (prevVal != null)
                    prevVal.incrementAndGet();
            }
            finally {
                rwl.readLock().unlock();
            }
        }
    }

    /**
     */
    private static class IncrementCacheEntryProcessor implements CacheEntryProcessor<String, Long, Long> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<String, Long> entry,
            Object... arguments) throws EntryProcessorException {
            long newVal = entry.getValue() == null ? 0 : entry.getValue() + 1;

            entry.setValue(newVal);

            return newVal;
        }
    }
}
