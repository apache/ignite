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

package org.apache.ignite.internal.processors.cache.persistence.evict;

import java.util.Arrays;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;

/**
 * Verifies the evictor tolerates a fragment chain mixed from two rows: a stale link of a removed row crossing
 * a chain of a new row that recycled a forgotten slot resolves to a link after two hops. The state is reachable
 * by legal protocol operations (no data corruption); the tolerant handling untracks the stale slot and keeps
 * sampling.
 * <p>
 * Page data bytes are asserted untouched across the evictor pass; end-to-end row content integrity is covered by
 * {@code PageEvictionConcurrentWritesAbstractTest}.
 */
public class PageEvictionTrackerFragmentChainSelfTest extends GridCommonAbstractTest {
    /** Page size. */
    private static final int PAGE_SIZE = 1024;

    /** Data region max size — small on purpose: keeps the tracking array tiny so the stale slot gets sampled. */
    private static final long REGION_MAX_SIZE = 16 * 1024;

    /** Page index of the stale entry fragment of the removed row. */
    private static final int IDX_F = 1;

    /** Page index of the fragment of the removed row already forgotten and recycled by the new row. */
    private static final int IDX_T = 2;

    /** Page index of the tail fragment of the new row. */
    private static final int IDX_TAIL2 = 3;

    /** Page index of the head fragment of the new row. */
    private static final int IDX_HEAD2 = 4;

    /**
     * Checks the mixed chain handling for {@link RandomLruPageEvictionTracker}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStaleFragmentLinkAfterRecycleRandomLru() throws Exception {
        checkStaleFragmentLinkAfterRecycle(false);
    }

    /**
     * Checks the mixed chain handling for {@link Random2LruPageEvictionTracker}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStaleFragmentLinkAfterRecycleRandom2Lru() throws Exception {
        checkStaleFragmentLinkAfterRecycle(true);
    }

    /**
     * @param random2Lru {@code true} to test {@link Random2LruPageEvictionTracker}, {@code false} for
     * {@link RandomLruPageEvictionTracker}.
     * @throws Exception If failed.
     */
    private void checkStaleFragmentLinkAfterRecycle(boolean random2Lru) throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration().setInitialSize(REGION_MAX_SIZE).setMaxSize(REGION_MAX_SIZE);

        PageMemoryNoStoreImpl pageMem = new PageMemoryNoStoreImpl(new NullLogger(), new UnsafeMemoryProvider(log()), null, PAGE_SIZE,
            plcCfg, new DataRegionMetricsImpl(plcCfg, new GridTestKernalContext(log())), false);

        DataRegionConfiguration regionCfg = new DataRegionConfiguration()
            .setName("fragment-chain-diag")
            .setInitialSize(REGION_MAX_SIZE)
            .setMaxSize(REGION_MAX_SIZE)
            .setEmptyPagesPoolSize(10)
            .setEvictionThreshold(0.9);

        PageAbstractEvictionTracker tracker = random2Lru ?
            new Random2LruPageEvictionTracker(pageMem, regionCfg, sharedCtx()) :
            new RandomLruPageEvictionTracker(pageMem, regionCfg, sharedCtx());

        try {
            pageMem.start();
            tracker.start();

            for (int i = 0; i <= IDX_HEAD2; i++)
                pageMem.allocatePage(0, 0, FLAG_DATA);

            // Fill the page bodies with a per-page pattern (keeping the first short non-data, so the evictor
            // rejects the pages by its type check without parsing them): the evictor pass below must not
            // modify page data bytes.
            byte[][] pageData = new byte[IDX_HEAD2 + 1][];

            for (int idx = 0; idx <= IDX_HEAD2; idx++) {
                long absPtr = pageMem.acquirePage(0, pageId(idx));

                try {
                    byte[] exp = new byte[PAGE_SIZE];

                    for (int i = 0; i < PAGE_SIZE; i++)
                        exp[i] = (byte)(idx * 31 + i);

                    exp[0] = (byte)0xFF;
                    exp[1] = (byte)0xFF;

                    PageUtils.putBytes(absPtr + PageMemoryNoStoreImpl.PAGE_OVERHEAD, 0, exp);

                    pageData[idx] = exp;
                }
                finally {
                    pageMem.releasePage(0, pageId(idx), absPtr);
                }
            }

            tracker.trackFragmentPage(pageId(IDX_F), pageId(IDX_T), false);

            assertEquals("F must link to T", slot(tracker, IDX_T), tracker.getFragmentLink(slot(tracker, IDX_F)));

            tracker.forgetPage(pageId(IDX_T));
            tracker.trackFragmentPage(pageId(IDX_T), pageId(IDX_TAIL2), false);

            assertEquals("T must link to the new row tail", slot(tracker, IDX_TAIL2), tracker.getFragmentLink(slot(tracker, IDX_T)));

            tracker.trackFragmentPage(pageId(IDX_HEAD2), pageId(IDX_TAIL2), true);

            assertTrue("The new row head must be tracked", tracker.checkTouch(pageId(IDX_HEAD2)));
            assertFalse("No row can be evicted from unwritten pages", tracker.evictDataPage(true));
            assertEquals("The sampled stale slot must be untracked", 0, tracker.getFragmentLink(slot(tracker, IDX_F)));

            assertEquals("The live row fragment link must stay intact",
                slot(tracker, IDX_TAIL2), tracker.getFragmentLink(slot(tracker, IDX_T)));

            assertTrue("The live row head must stay tracked", tracker.checkTouch(pageId(IDX_HEAD2)));

            assertFalse(tracker.evictDataPage(true));

            // The evictor pass must not touch page data bytes.
            for (int idx = 0; idx <= IDX_HEAD2; idx++) {
                long absPtr = pageMem.acquirePage(0, pageId(idx));

                try {
                    byte[] actual = PageUtils.getBytes(absPtr + PageMemoryNoStoreImpl.PAGE_OVERHEAD, 0, PAGE_SIZE);

                    assertTrue("Page " + idx + " bytes must stay untouched by the evictor", Arrays.equals(pageData[idx], actual));
                }
                finally {
                    pageMem.releasePage(0, pageId(idx), absPtr);
                }
            }
        }
        finally {
            tracker.stop();
            pageMem.stop(true);
        }
    }

    /**
     * @param tracker Tracker.
     * @param pageIdx Page index.
     * @return Tracking array slot of the page.
     */
    private static int slot(PageAbstractEvictionTracker tracker, int pageIdx) {
        return tracker.trackingIdx(pageIdx);
    }

    /**
     * @param pageIdx Page index.
     * @return Fabricated data page ID.
     */
    private static long pageId(int pageIdx) {
        return PageIdUtils.pageId(0, FLAG_DATA, pageIdx);
    }

    /** @return Minimal shared context for the tracker constructor. */
    private GridCacheSharedContext<?, ?> sharedCtx() {
        GridTestKernalContext kernalCtx = new GridTestKernalContext(log());

        kernalCtx.config().setDataStorageConfiguration(new DataStorageConfiguration().setPageSize(PAGE_SIZE));

        GridCacheSharedContext<?, ?> sharedCtx = Mockito.mock(GridCacheSharedContext.class);

        Mockito.when(sharedCtx.kernalContext()).thenReturn(kernalCtx);
        Mockito.when(sharedCtx.logger((Class<?>)Mockito.any())).thenReturn(new NullLogger());

        return sharedCtx;
    }
}
