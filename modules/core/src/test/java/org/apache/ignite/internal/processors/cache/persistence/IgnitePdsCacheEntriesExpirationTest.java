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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Class contains various tests related to cache entry expiration feature.
 */
public class IgnitePdsCacheEntriesExpirationTest extends GridCommonAbstractTest {
    /** */
    private static final int TIMEOUT = 10_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                .setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(60_000);

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 2))
            .setBackups(1)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 350)));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Verifies scenario of a deadlock between thread, modifying a cache entry (acquires cp read lock and entry lock),
     * ttl thread, expiring the entry (acquires cp read lock and entry lock)
     * and checkpoint thread (acquires cp write lock).
     *
     * Checkpoint thread in not used but emulated by the test to avoid test hang (interruptible API for acquiring
     * write lock is used).
     *
     * For more details see <a href="https://ggsystems.atlassian.net/browse/GG-23135">GG-23135</a>.
     *
     * <p>
     *     <strong>Important note</strong>
     *     Implementation of this test relies heavily on structure of existing code in
     * {@link GridCacheOffheapManager.GridCacheDataStore#purgeExpiredInternal(GridCacheContext, IgniteInClosure2X, int)}
     * and
     * {@link GridCacheMapEntry#onExpired(CacheObject, GridCacheVersion)} methods.
     *
     * Any changes to those methods could break logic inside the test so if new failures of the test occure
     * test code itself may require refactoring.
     * </p>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeadlockBetweenCachePutAndEntryExpiration() throws Exception {
        AtomicBoolean timeoutReached = new AtomicBoolean(false);

        AtomicBoolean cpWriteLocked = new AtomicBoolean(false);

        AtomicInteger partId = new AtomicInteger();

        CountDownLatch ttlLatch = new CountDownLatch(2);

        IgniteEx srv0 = startGrids(2);

        srv0.cluster().active(true);

        awaitPartitionMapExchange();

        srv0.getOrCreateCache(DEFAULT_CACHE_NAME);

        GridDhtPartitionTopologyImpl top =
            (GridDhtPartitionTopologyImpl)srv0.cachex(DEFAULT_CACHE_NAME).context().topology();

        top.partitionFactory((ctx, grp, id) -> {
            partId.set(id);
            return new GridDhtLocalPartition(ctx, grp, id, false) {
                /**
                 * This method is modified to bring threads in deadlock situation.
                 * Idea is the following: updater thread (see code below) on its way to
                 * {@link GridCacheMapEntry#onExpired(CacheObject, GridCacheVersion)} call stops here
                 * (already having entry lock acquired) and waits until checkpoint write lock is acquired
                 * by another special thread imulating checkpointer thread (cp-write-lock-holder, see code below).
                 * After that it enables ttl-cleanup-worker thread to proceed
                 * (by counting down ttLatch, see next overridden method) and reproduce deadlock scenario.
                 */
                @Override public IgniteCacheOffheapManager.CacheDataStore dataStore() {
                    Thread t = Thread.currentThread();
                    String tName = t.getName();

                    if (tName == null || !tName.contains("updater"))
                        return super.dataStore();

                    boolean unswapFoundInST = false;

                    for (StackTraceElement e : t.getStackTrace()) {
                        if (e.getMethodName().contains("unswap")) {
                            unswapFoundInST = true;

                            break;
                        }
                    }

                    if (!unswapFoundInST)
                        return super.dataStore();

                    while (!cpWriteLocked.get()) {
                        try {
                            Thread.sleep(10);
                        }
                        catch (InterruptedException ignored) {
                            log.warning(">>> Thread caught InterruptedException while waiting " +
                                "for cp write lock to be locked");
                        }
                    }

                    ttlLatch.countDown();

                    return super.dataStore();
                }

                /**
                 * This method is modified to bring threads in deadlock situation.
                 * Idea is the following: internal ttl-cleanup-worker thread wakes up to cleanup expired entries,
                 * reaches this method after calling purgeExpiredInternal (thus having checkpoint readlock acquired)
                 * and stops on ttlLatch until updater thread comes in, acquires entry lock and gets stuck
                 * on acquiring cp read lock
                 * (because of special cp-write-lock-holder thread already holding cp write lock).
                 *
                 * So situation of three threads stuck in deadlock is reproduced.
                 */
                @Override public boolean reserve() {
                    Thread t = Thread.currentThread();
                    String tName = t.getName();

                    if (tName == null || !tName.contains("ttl-cleanup-worker"))
                        return super.reserve();

                    boolean purgeExpiredFoundInST = false;

                    for (StackTraceElement e : t.getStackTrace()) {
                        if (e.getMethodName().contains("purgeExpiredInternal")) {
                            purgeExpiredFoundInST = true;

                            break;
                        }
                    }

                    if (!purgeExpiredFoundInST)
                        return super.reserve();

                    ttlLatch.countDown();

                    try {
                        ttlLatch.await();
                    }
                    catch (InterruptedException ignored) {
                        log.warning(">>> Thread caught InterruptedException while waiting for ttl latch" +
                            " to be released by updater thread");
                    }

                    return super.reserve();
                }
            };
        });

        stopGrid(1);
        //change BLT to force new partition creation with modified GridDhtLocalPartition class
        srv0.cluster().setBaselineTopology(srv0.cluster().topologyVersion());

        Thread.sleep(500);

        IgniteCache<Object, Object> cache = srv0.getOrCreateCache(DEFAULT_CACHE_NAME);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)srv0.context().cache().context().database();

        int key = 0;

        while (true) {
            if (srv0.affinity(DEFAULT_CACHE_NAME).partition(key) != partId.get())
                key++;
            else break;
        }

        cache.put(key, 1);

        int finalKey = key;

        IgniteInternalFuture updateFut = GridTestUtils.runAsync(() -> {
            log.info(">>> Updater thread has started, updating key " + finalKey);

            int i = 10;

            while (!timeoutReached.get()) {
                cache.put(finalKey, i++);

                try {
                    Thread.sleep(300);
                }
                catch (InterruptedException e) {
                    log.warning(">>> Updater thread sleep was interrupted");
                }
            }
        }, "updater-thread");

        IgniteInternalFuture writeLockHolderFut = GridTestUtils.runAsync(() -> {
            while (ttlLatch.getCount() != 1) {
                try {
                    Thread.sleep(20);
                }
                catch (InterruptedException e) {
                    log.warning(">>> Write lock holder thread sleep was interrupted");

                    break;
                }
            }

            try {
                cpWriteLocked.set(true);

                db.checkpointLock.writeLock().lockInterruptibly();

                ttlLatch.await();
            }
            catch (InterruptedException e) {
                log.warning(">>> Write lock holder thread was interrupted while obtaining write lock.");
            }
            finally {
                db.checkpointLock.writeLock().unlock();
            }
        }, "cp-write-lock-holder");

        GridTestUtils.runAsync(() -> {
            long start = System.currentTimeMillis();

            while (System.currentTimeMillis() - start < TIMEOUT) {
                doSleep(1_000);
            }

            timeoutReached.set(true);
        });

        try {
            updateFut.get(TIMEOUT * 2);
        }
        catch (IgniteFutureTimeoutCheckedException ignored) {
            fail("Failed to wait for futures for doubled timeout");
        }
        finally {
            while (ttlLatch.getCount() > 0)
                ttlLatch.countDown();

            writeLockHolderFut.cancel();
            updateFut.cancel();
        }
    }
}
