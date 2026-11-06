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

package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/**
 * Values of entry processors on the primary node and B+tree pages are not reserved before the insert. While the concurrent
 * ones stay within the empty pages pool, the default eviction threshold must leave enough room for them. Otherwise, they
 * fail in the page memory (known limitation), which must not be confused with a refused reservation.
 */
public class PageEvictionDefaultThresholdSelfTest extends PageEvictionAbstractTest {
    /** Number of concurrent writers. */
    private static final int WRITERS = 8;

    /** Number of operations of a writer: enough to replace the content of the region many times. */
    private static final int OPS_PER_WRITER = 20_000;

    /** Size of a value produced by an entry processor, in pages: the writers together stay within the empty pages pool. */
    private static final int LARGE_VAL_PAGES = 10;

    /** Size of a value produced by an entry processor, in pages: the writers together exceed the empty pages pool. */
    private static final int ABOVE_POOL_VAL_PAGES = 60;

    /** Logger to check the messages of the cache update failure. */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = setEvictionMode(DataPageEvictionMode.RANDOM_2_LRU, super.getConfiguration(gridName));

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration()
            .setEvictionThreshold(new DataRegionConfiguration().getEvictionThreshold());

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        assertNoOomFailures();
    }

    /** The value of the default threshold. */
    @Test
    public void testDefaultThreshold() {
        assertEquals(0.95, new DataRegionConfiguration().getEvictionThreshold(), 0);
    }

    /**
     * The writers exceed the empty pages pool and exhaust the space above the threshold: an out-of-memory comes from the
     * page memory after the reservation has succeeded. It is reported as a critical error, not as a refused reservation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEntryProcessorsAboveThePoolFailInPageMemory() throws Exception {
        LogListener refusedLsnr = LogListener.matches("Cache update refused").build();
        LogListener criticalLsnr = LogListener.matches("Critical system error detected").build();

        testLog.registerListener(refusedLsnr);
        testLog.registerListener(criticalLsnr);

        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, byte[]> cache = createCache(ignite);

        assertTrue("The writers must exceed the pool",
            WRITERS * ABOVE_POOL_VAL_PAGES > defaultRegion(ignite).config().getEmptyPagesPoolSize());

        int largeValSize = ABOVE_POOL_VAL_PAGES * pageSize(ignite);

        AtomicInteger keyGen = new AtomicInteger();

        AtomicReference<Throwable> err = new AtomicReference<>();

        GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                while (err.get() == null)
                    cache.invoke(keyGen.incrementAndGet(), new LargeValueProcessor(), largeValSize);
            }
            catch (Throwable e) {
                err.compareAndSet(null, e);
            }
        }, WRITERS, "above-pool-writer").get(TimeUnit.MINUTES.toMillis(2));

        assertTrue("Unexpected failure: " + err.get(), X.hasCause(err.get(), IgniteOutOfMemoryException.class));

        assertTrue("A page memory out-of-memory must reach the failure handler", OOM_FAILURES.get() > 0);
        assertTrue(criticalLsnr.check());
        assertFalse("A page memory out-of-memory must not be logged as a refusal", refusedLsnr.check());

        // Expected in this test only.
        OOM_FAILURES.set(0);
    }

    /** @throws Exception If failed. */
    @Test
    public void testUnreservedAllocations() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, byte[]> cache = createCache(ignite);

        int largeValSize = LARGE_VAL_PAGES * pageSize(ignite);

        AtomicInteger keyGen = new AtomicInteger();

        assertTrue("The writers must stay within the pool",
            WRITERS * LARGE_VAL_PAGES < defaultRegion(ignite).config().getEmptyPagesPoolSize());

        GridTestUtils.runMultiThreadedAsync(() -> {
            for (int op = 0; op < OPS_PER_WRITER; op++)
                cache.invoke(keyGen.incrementAndGet(), new LargeValueProcessor(), largeValSize);
        }, WRITERS, "unreserved-writer").get(TimeUnit.MINUTES.toMillis(5));

        assertTrue("Eviction must have started", isEvictionsStarted(ignite));
    }

    /**
     * @param ignite Node.
     * @return Cache to write to.
     */
    private static IgniteCache<Integer, byte[]> createCache(IgniteEx ignite) {
        return ignite.createCache(new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(0)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));
    }

    /** Sets a value of the given size: it is unknown before the processor runs, so it is written without a reservation. */
    private static class LargeValueProcessor implements EntryProcessor<Integer, byte[], Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, byte[]> entry, Object... args) {
            entry.setValue(new byte[(int)args[0]]);

            return null;
        }
    }
}
