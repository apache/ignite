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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccManager.CACHE_EXPLICIT_LOCKS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccManager.CACHE_LOCKS_VIEW;

/**
 * Explicit lock timeout that expires while the lock is still being mapped, for example after a GC pause longer than
 * the timeout or while the mapping waits for an exchange: lock candidates registered by the mapping must not outlive
 * the failed lock attempt.
 */
public class ExplicitLockTimeoutDuringMappingTest extends GridCommonAbstractTest {
    /** */
    private static final long TIMEOUT = 10_000L;

    /** Pause of the key serialization in the current thread, consumed by the first serialization. */
    private static final ThreadLocal<Long> KEY_SERIALIZATION_PAUSE = new ThreadLocal<>();

    /** */
    private IgniteCache<Object, Object> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cache = startGrid(0).cache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** The timeout expires while the key is being mapped: the key must not stay locked by nobody. */
    @Test
    public void testTimeoutDuringMapping() throws Exception {
        StallingKey key = new StallingKey(1);

        Lock lock = cache.lock(key);

        KEY_SERIALIZATION_PAUSE.set(500L);

        if (lock.tryLock(100, TimeUnit.MILLISECONDS))
            lock.unlock();

        checkReleased(cache, key);
    }

    /** The timeout of a reentrant lock expires while the key is being mapped: the reentry must not outlive it. */
    @Test
    public void testReentrantLockTimeoutDuringMapping() throws Exception {
        StallingKey key = new StallingKey(1);

        Lock outer = cache.lock(key);

        outer.lock();

        KEY_SERIALIZATION_PAUSE.set(500L);

        Lock inner = cache.lock(key);

        if (inner.tryLock(100, TimeUnit.MILLISECONDS))
            inner.unlock();

        outer.unlock();

        checkReleased(cache, key);
    }

    /** A multi-key lock with a held key times out on the other key: the reentry of the held key must be reverted. */
    @Test
    public void testMultiKeyLockTimeoutWithHeldKey() throws Exception {
        Lock outer = cache.lock(1);

        outer.lock();

        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> holder = holdInOtherThread(cache, 2, release);

        assertFalse(cache.lockAll(Arrays.asList(1, 2)).tryLock(100, TimeUnit.MILLISECONDS));

        outer.unlock();

        release.countDown();

        holder.get(TIMEOUT);

        checkReleased(cache, 1);
    }

    /** The timeout expires while the mapping waits for an exchange: the postponed mapping must not lock the key. */
    @Test
    public void testTimeoutBeforePostponedMapping() throws Exception {
        IgniteEx node = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache1 = node.cache(DEFAULT_CACHE_NAME);

        Integer key = primaryKey(cache1);

        TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(grid(0));

        crdSpi.blockMessages(GridDhtPartitionsFullMessage.class, node.name());

        IgniteInternalFuture<?> exchFut = GridTestUtils.runAsync(() -> grid(0).createCache("tmp1"));

        crdSpi.waitForBlocked();

        assertFalse(cache1.lock(key).tryLock(500, TimeUnit.MILLISECONDS));

        crdSpi.stopBlock();

        exchFut.get(TIMEOUT);

        // The next exchange would wait for a leaked candidate forever.
        GridTestUtils.runAsync(() -> grid(0).createCache("tmp2")).get(TIMEOUT);

        checkReleased(cache1, key);
    }

    /** Checks that the key can be locked by another thread and is not considered locked by the current one. */
    private void checkReleased(IgniteCache<Object, Object> cache, Object key) throws Exception {
        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> holder = holdInOtherThread(cache, key, release);

        assertFalse("Lock candidate leaked: " + key, cache.lock(key).tryLock());

        release.countDown();

        holder.get(TIMEOUT);

        for (Ignite ignite : G.allGrids()) {
            assertEquals(0, ((IgniteEx)ignite).context().systemView().view(CACHE_EXPLICIT_LOCKS_VIEW).size());
            assertEquals(0, ((IgniteEx)ignite).context().systemView().view(CACHE_LOCKS_VIEW).size());
        }
    }

    /** @return Future of the thread that holds the lock on the key until the latch is released. */
    private IgniteInternalFuture<?> holdInOtherThread(
        IgniteCache<Object, Object> cache,
        Object key,
        CountDownLatch release
    ) throws Exception {
        CountDownLatch locked = new CountDownLatch(1);

        IgniteInternalFuture<?> holder = GridTestUtils.runAsync(() -> {
            Lock lock = cache.lock(key);

            lock.lock();

            locked.countDown();

            release.await();

            lock.unlock();

            return null;
        });

        assertTrue("Key stays locked: " + key, locked.await(TIMEOUT, TimeUnit.MILLISECONDS));

        return holder;
    }

    /** Key whose serialization pauses the thread, see {@link #KEY_SERIALIZATION_PAUSE}. */
    private static class StallingKey implements Binarylizable {
        /** */
        private int id;

        /** */
        private StallingKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            Long pause = KEY_SERIALIZATION_PAUSE.get();

            if (pause != null) {
                KEY_SERIALIZATION_PAUSE.remove();

                doSleep(pause);
            }

            writer.writeInt("id", id);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            id = reader.readInt("id");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "StallingKey [id=" + id + ']';
        }
    }
}
