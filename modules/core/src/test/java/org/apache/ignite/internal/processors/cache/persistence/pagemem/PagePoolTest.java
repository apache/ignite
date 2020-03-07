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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class PagePoolTest extends GridCommonAbstractTest {
    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "PageSize={0}, segment={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {1024 + PageMemoryImpl.PAGE_OVERHEAD, 0},
            new Object[] {1024 + PageMemoryImpl.PAGE_OVERHEAD, 1},
            new Object[] {1024 + PageMemoryImpl.PAGE_OVERHEAD, 2},
            new Object[] {1024 + PageMemoryImpl.PAGE_OVERHEAD, 4},
            new Object[] {1024 + PageMemoryImpl.PAGE_OVERHEAD, 8},
            new Object[] {1024 + PageMemoryImpl.PAGE_OVERHEAD, 16},
            new Object[] {1024 + PageMemoryImpl.PAGE_OVERHEAD, 31},

            new Object[] {2048 + PageMemoryImpl.PAGE_OVERHEAD, 0},
            new Object[] {2048 + PageMemoryImpl.PAGE_OVERHEAD, 1},
            new Object[] {2048 + PageMemoryImpl.PAGE_OVERHEAD, 2},
            new Object[] {2048 + PageMemoryImpl.PAGE_OVERHEAD, 4},
            new Object[] {2048 + PageMemoryImpl.PAGE_OVERHEAD, 8},
            new Object[] {2048 + PageMemoryImpl.PAGE_OVERHEAD, 16},
            new Object[] {2048 + PageMemoryImpl.PAGE_OVERHEAD, 31},

            new Object[] {4096 + PageMemoryImpl.PAGE_OVERHEAD, 0},
            new Object[] {4096 + PageMemoryImpl.PAGE_OVERHEAD, 1},
            new Object[] {4096 + PageMemoryImpl.PAGE_OVERHEAD, 2},
            new Object[] {4096 + PageMemoryImpl.PAGE_OVERHEAD, 4},
            new Object[] {4096 + PageMemoryImpl.PAGE_OVERHEAD, 8},
            new Object[] {4096 + PageMemoryImpl.PAGE_OVERHEAD, 16},
            new Object[] {4096 + PageMemoryImpl.PAGE_OVERHEAD, 31},

            new Object[] {8192 + PageMemoryImpl.PAGE_OVERHEAD, 0},
            new Object[] {8192 + PageMemoryImpl.PAGE_OVERHEAD, 1},
            new Object[] {8192 + PageMemoryImpl.PAGE_OVERHEAD, 2},
            new Object[] {8192 + PageMemoryImpl.PAGE_OVERHEAD, 4},
            new Object[] {8192 + PageMemoryImpl.PAGE_OVERHEAD, 8},
            new Object[] {8192 + PageMemoryImpl.PAGE_OVERHEAD, 16},
            new Object[] {8192 + PageMemoryImpl.PAGE_OVERHEAD, 31},

            new Object[] {16384 + PageMemoryImpl.PAGE_OVERHEAD, 0},
            new Object[] {16384 + PageMemoryImpl.PAGE_OVERHEAD, 1},
            new Object[] {16384 + PageMemoryImpl.PAGE_OVERHEAD, 2},
            new Object[] {16384 + PageMemoryImpl.PAGE_OVERHEAD, 4},
            new Object[] {16384 + PageMemoryImpl.PAGE_OVERHEAD, 8},
            new Object[] {16384 + PageMemoryImpl.PAGE_OVERHEAD, 16},
            new Object[] {16384 + PageMemoryImpl.PAGE_OVERHEAD, 31}
        );
    }

    /** */
    @Parameterized.Parameter
    public int sysPageSize;

    /** */
    @Parameterized.Parameter(1)
    public int segment;

    /** */
    private static final int PAGES = 100;

    /** */
    private OffheapReadWriteLock rwLock = new OffheapReadWriteLock(U.nextPowerOf2(Runtime.getRuntime().availableProcessors()));

    /** */
    private DirectMemoryProvider provider;

    /** */
    private DirectMemoryRegion region;

    /** */
    private PagePool pool;

    /**
     */
    @Before
    public void prepare() {
        provider = new UnsafeMemoryProvider(log);
        provider.initialize(new long[] {sysPageSize * PAGES + 16});

        region = provider.nextRegion();

        pool = new PagePool(segment, region, sysPageSize, rwLock);
    }

    /**
     */
    @After
    public void cleanup() {
        provider.shutdown(true);
    }

    /**
     */
    @Test
    public void testSingleThreadedBorrowRelease() {
        assertEquals(PAGES, pool.pages());
        assertEquals(0, pool.size());

        Set<Long> allocated = new LinkedHashSet<>();
        LinkedList<Long> allocatedQueue = new LinkedList<>();

        info("Region start: " + U.hexLong(region.address()));

        int tag = 1;

        for (int i = 0; i < PAGES; i++) {
            long relPtr = pool.borrowOrAllocateFreePage(tag);

            assertTrue("Failed for i=" + i, relPtr != PageMemoryImpl.INVALID_REL_PTR);

            assertTrue(allocated.add(relPtr));
            allocatedQueue.add(relPtr);

            PageHeader.writeTimestamp(pool.absolute(relPtr), 0xFFFFFFFFFFFFFFFFL);

            assertEquals(i + 1, pool.size());
        }

        info("Done allocating");

        assertEquals(PageMemoryImpl.INVALID_REL_PTR, pool.borrowOrAllocateFreePage(tag));

        assertEquals(PAGES, pool.size());

        {
            int i = 0;

            for (Long relPtr : allocated) {
                pool.releaseFreePage(relPtr);

                i++;

                assertEquals(PAGES - i, pool.size());
            }
        }

        info("Done releasing");

        assertEquals(0, pool.size());

        {
            Iterator<Long> it = allocatedQueue.descendingIterator();

            int i = 0;

            while (it.hasNext()) {
                long relPtr = it.next();

                long fromPool = pool.borrowOrAllocateFreePage(tag);

                assertEquals(relPtr, fromPool);

                i++;

                assertEquals(i, pool.size());

                PageHeader.writeTimestamp(pool.absolute(relPtr), 0xFFFFFFFFFFFFFFFFL);
            }
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMultithreadedConsistency() throws Exception {
        assertEquals(PAGES, pool.pages());
        assertEquals(0, pool.size());

        ConcurrentMap<Long, Long> allocated = new ConcurrentHashMap<>();

        {
            long relPtr;

            while ((relPtr = pool.borrowOrAllocateFreePage(1)) != PageMemoryImpl.INVALID_REL_PTR) {
                assertNull(allocated.put(relPtr, relPtr));

                PageHeader.writeTimestamp(pool.absolute(relPtr), 0xFFFFFFFFFFFFFFFFL);
            }
        }

        assertEquals(PAGES, pool.size());
        assertEquals(PAGES, allocated.size());

        GridTestUtils.runMultiThreaded(() -> {
            while (!allocated.isEmpty()) {
                Long polled = pollRandom(allocated);

                if (polled != null)
                    pool.releaseFreePage(polled);
            }
        }, Runtime.getRuntime().availableProcessors(), "load-runner");

        assertTrue(allocated.isEmpty());
        assertEquals(0, pool.size());

        GridTestUtils.runMultiThreaded(() -> {
            long polled;

            while ((polled = pool.borrowOrAllocateFreePage(1)) != PageMemoryImpl.INVALID_REL_PTR) {
                assertNull(allocated.put(polled, polled));

                PageHeader.writeTimestamp(pool.absolute(polled), 0xFFFFFFFFFFFFFFFFL);
            }
        }, Runtime.getRuntime().availableProcessors(), "load-runner");

        assertEquals(PAGES, pool.size());
        assertEquals(PAGES, allocated.size());

        GridTestUtils.runMultiThreaded(() -> {
            boolean toPool = true;

            for (int i = 0; i < 10_000; i++) {
                if (toPool) {
                    if (allocated.size() < PAGES / 3) {
                        toPool = false;

                        log.info("Direction switched: " + toPool);
                    }
                }
                else {
                    if (allocated.size() > 2 * PAGES / 3) {
                        toPool = true;

                        log.info("Direction switched: " + toPool);
                    }
                }

                boolean inverse = ThreadLocalRandom.current().nextInt(3) == 0;

                if (toPool ^ inverse) {
                    Long polled = pollRandom(allocated);

                    if (polled != null)
                        pool.releaseFreePage(polled);
                }
                else {
                    long polled = pool.borrowOrAllocateFreePage(1);

                    if (polled != PageMemoryImpl.INVALID_REL_PTR) {
                        long abs = pool.absolute(polled);

                        PageHeader.writeTimestamp(abs, 0xFFFFFFFFFFFFFFFFL);

                        assertNull(allocated.put(polled, polled));
                    }
                }
            }
        }, Runtime.getRuntime().availableProcessors(), "load-runner");

        {
            long relPtr;

            while ((relPtr = pool.borrowOrAllocateFreePage(1)) != PageMemoryImpl.INVALID_REL_PTR)
                assertNull(allocated.put(relPtr, relPtr));

            assertEquals(PAGES, allocated.size());
            assertEquals(PAGES, pool.size());
        }
    }

    /**
     * @param allocated Map of allocated pages.
     * @return Random page polled from the map.
     */
    private Long pollRandom(ConcurrentMap<Long, Long> allocated) {
        int size = allocated.size();

        if (size == 0)
            return null;

        int cnt = ThreadLocalRandom.current().nextInt(size);

        Iterator<Long> it = allocated.keySet().iterator();

        for (int i = 0; i < cnt; i++) {
            if (it.hasNext())
                it.next();
            else
                break;
        }

        if (it.hasNext()) {
            Long key = it.next();

            if (allocated.remove(key) != null)
                return key;
        }

        return null;
    }
}
