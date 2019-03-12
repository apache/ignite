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

package org.apache.ignite.internal.processors.database;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.evict.NoOpPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.stat.IoStatisticsHolderNoOp;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class FreeListTest extends GridCommonAbstractTest {
    /** */
    private static final int CPUS = Runtime.getRuntime().availableProcessors();

    /** */
    private static final long MB = 1024L * 1024L;

    /** */
    private PageMemory pageMem;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (pageMem != null)
            pageMem.stop(true);

        pageMem = null;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteSingleThreaded_1024() throws Exception {
        checkInsertDeleteSingleThreaded(1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteSingleThreaded_2048() throws Exception {
        checkInsertDeleteSingleThreaded(2048);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteSingleThreaded_4096() throws Exception {
        checkInsertDeleteSingleThreaded(4096);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteSingleThreaded_8192() throws Exception {
        checkInsertDeleteSingleThreaded(8192);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteSingleThreaded_16384() throws Exception {
        checkInsertDeleteSingleThreaded(16384);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteMultiThreaded_1024() throws Exception {
        checkInsertDeleteMultiThreaded(1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteMultiThreaded_2048() throws Exception {
        checkInsertDeleteMultiThreaded(2048);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteMultiThreaded_4096() throws Exception {
        checkInsertDeleteMultiThreaded(4096);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteMultiThreaded_8192() throws Exception {
        checkInsertDeleteMultiThreaded(8192);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testInsertDeleteMultiThreaded_16384() throws Exception {
        checkInsertDeleteMultiThreaded(16384);
    }

    /**
     * @param pageSize Page size.
     * @throws Exception If failed.
     */
    protected void checkInsertDeleteMultiThreaded(final int pageSize) throws Exception {
        final FreeList list = createFreeList(pageSize);

        final ConcurrentMap<Long, Storable> stored = new ConcurrentHashMap<>();

        for (int i = 0; i < 100; i++) {
            Storable row = createTestStorable(pageSize);

            list.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE, false);

            assertTrue(row.link() != 0L);

            Storable old = stored.put(row.link(), row);

            assertNull(old);
        }

        final AtomicBoolean grow = new AtomicBoolean(true);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 200_000; i++) {
                    boolean grow0 = grow.get();

                    if (grow0) {
                        if (stored.size() > 20_000) {
                            if (grow.compareAndSet(true, false))
                                info("Shrink... [" + stored.size() + ']');

                            grow0 = false;
                        }
                    }
                    else {
                        if (stored.size() < 1_000) {
                            if (grow.compareAndSet(false, true))
                                info("Grow... [" + stored.size() + ']');

                            grow0 = true;
                        }
                    }

                    boolean insert = rnd.nextInt(100) < 70 == grow0;

                    if (insert) {
                        Storable row = createTestStorable(pageSize);

                        list.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE, false);

                        assertTrue(row.link() != 0L);

                        Storable old = stored.put(row.link(), row);

                        assertNull(old);
                    }
                    else {
                        while (true) {
                            Iterator<Storable> it = stored.values().iterator();

                            if (it.hasNext()) {
                                Storable row = it.next();

                                Storable rmvd = stored.remove(row.link());

                                if (rmvd != null) {
                                    list.removeDataRowByLink(row.link(), IoStatisticsHolderNoOp.INSTANCE);

                                    break;
                                }
                            }
                        }
                    }
                }

                return null;
            }
        }, 8, "runner");
    }

    /**
     * @throws Exception if failed.
     */
    protected void checkInsertDeleteSingleThreaded(int pageSize) throws Exception {
        FreeList list = createFreeList(pageSize);

        Random rnd = new Random();

        Map<Long, Storable> stored = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            Storable row = createTestStorable(pageSize);

            list.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE, false);

            assertTrue(row.link() != 0L);

            Storable old = stored.put(row.link(), row);

            assertNull(old);
        }

        boolean grow = true;

        for (int i = 0; i < 1_000_000; i++) {
            if (grow) {
                if (stored.size() > 20_000) {
                    grow = false;

                    info("Shrink... [" + stored.size() + ']');
                }
            }
            else {
                if (stored.size() < 1_000) {
                    grow = true;

                    info("Grow... [" + stored.size() + ']');
                }
            }

            boolean insert = rnd.nextInt(100) < 70 == grow;

            if (insert) {
                Storable row = createTestStorable(pageSize);

                list.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE, false);

                assertTrue(row.link() != 0L);

                Storable old = stored.put(row.link(), row);

                assertNull(old);
            }
            else {
                Iterator<Storable> it = stored.values().iterator();

                if (it.hasNext()) {
                    Storable row = it.next();

                    Storable rmvd = stored.remove(row.link());

                    assertTrue(rmvd == row);

                    list.removeDataRowByLink(row.link(), IoStatisticsHolderNoOp.INSTANCE);
                }
            }
        }
    }

    /**
     * @return Page memory.
     */
    protected PageMemory createPageMemory(int pageSize, DataRegionConfiguration plcCfg) throws Exception {
        PageMemory pageMem = new PageMemoryNoStoreImpl(log,
            new UnsafeMemoryProvider(log),
            null,
            pageSize,
            plcCfg,
            new DataRegionMetricsImpl(plcCfg),
            true);

        pageMem.start();

        return pageMem;
    }

    /**
     * @param pageSize Page size.
     * @return Free list.
     * @throws Exception If failed.
     */
    protected FreeList createFreeList(int pageSize) throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration()
            .setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        pageMem = createPageMemory(pageSize, plcCfg);

        long metaPageId = pageMem.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);

        DataRegionMetricsImpl regionMetrics = new DataRegionMetricsImpl(plcCfg);

        DataRegion dataRegion = new DataRegion(pageMem, plcCfg, regionMetrics, new NoOpPageEvictionTracker());

        return new CacheFreeListImpl(1, "freelist", regionMetrics, dataRegion, null, null, metaPageId, true);
    }

    /**
     * @param pageSize Page size.
     */
    protected Storable createTestStorable(int pageSize) {
        return new SimpleDataRow(0, new byte[ThreadLocalRandom.current().nextInt(pageSize * 3 / 2) + pageSize / 6]);
    }
}
