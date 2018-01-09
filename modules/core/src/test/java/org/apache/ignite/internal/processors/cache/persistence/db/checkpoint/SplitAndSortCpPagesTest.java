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
package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.AsyncCheckpointer;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointScope;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.FullPageIdsBuffer;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesConcurrentHashSet;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;

/**
 * Test for async splitting and sorting pages.
 */
public class SplitAndSortCpPagesTest {
    /** Logger. */
    private IgniteLogger log = new NullLogger();

    /**
     * Test existing implementation.
     */
    @Test
    public void testSingleThreadSplitSort() {
        CheckpointScope coll = getTestCollection();

        DataStorageConfiguration dsCfg = config().getDataStorageConfiguration();

        dsCfg.setCheckpointThreads(13);

        Collection<FullPageIdsBuffer> ids = coll.splitAndSortCpPagesIfNeeded(dsCfg);

        int sz = 0;

        Collection<FullPageId[]> res = new ArrayList<>();

        for (FullPageIdsBuffer next : ids) {
            sz += next.remaining();
            FullPageId[] e = next.toArray();
            
            res.add(e);

            validateOrder(Collections.singletonList(e));
        }

        assertEquals(sz, coll.totalCpPages());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAsyncLazyCpPagesSubmit() throws Exception {
        final CheckpointScope scope = getTestCollection();

        final AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(6, getClass().getSimpleName(), log);

        final AtomicInteger totalPagesAfterSort = new AtomicInteger();

        final IgniteClosure<FullPageId[], Callable<Void>> taskFactory = new IgniteClosure<FullPageId[], Callable<Void>>() {
            @Override public Callable<Void> apply(final FullPageId[] ids) {
                return new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        final int len = ids.length;

                        totalPagesAfterSort.addAndGet(len);

                        validateOrder(Collections.singletonList(ids));

                        return null;
                    }
                };
            }
        };

        asyncCheckpointer.quickSortAndWritePages(scope, taskFactory).get();

        Assert.assertEquals(totalPagesAfterSort.get(), scope.totalCpPages());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testGlobalOrder() throws Exception {
        CheckpointScope scope = getTestCollection();

        AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(6, getClass().getSimpleName(), log);

        final IgniteClosure<FullPageId[], Callable<Void>> taskFactory = new IgniteClosure<FullPageId[], Callable<Void>>() {
            @Override public Callable<Void> apply(final FullPageId[] ids) {
                return new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        return null;
                    }
                };
            }
        };

        List<FullPageIdsBuffer> pageIds = scope.toBuffers();

        asyncCheckpointer.quickSortAndWritePages(pageIds, taskFactory).get();

        for (FullPageIdsBuffer next : pageIds) {
            validateOrder(Collections.singletonList(next.toArray()));
        }

    }

    /**
     * Checks if order of element is correct.
     * @param ids sorted IDs.
     */
    private static void validateOrder(Iterable<FullPageId[]> ids) {
        FullPageId prevId = null;

        for (FullPageId[] nextArr : ids) {
            for (FullPageId next : nextArr) {
                if (prevId != null) {
                    final boolean cond =
                        GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR.compare(next, prevId) >= 0;

                    if (!cond) {
                        assertTrue("Incorrect order of pages: [\nprev=" + prevId + ",\n cur=" + next + "]",
                            cond);
                    }
                }
                prevId = next;
            }
        }
    }

    /**
     * @return test configuration
     */
    private IgniteConfiguration config() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        return cfg.setDataStorageConfiguration(dsCfg);
    }

    /**
     * Uses control map to verify no elements were lost
     * @throws Exception if failed.
     */
    @Test
    public void testAsyncAllPagesArePresent() throws Exception {
        CheckpointScope scope = getTestCollection();
        final ConcurrentHashMap<FullPageId, FullPageId> map = createCopyAsMap(scope);

        AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(16, getClass().getSimpleName(), log);

        IgniteClosure<FullPageId[], Callable<Void>> taskFactory = new IgniteClosure<FullPageId[], Callable<Void>>() {
            @Override public Callable<Void> apply(final FullPageId[] ids) {
                return new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        for (FullPageId id : ids) {
                            map.remove(id);
                        }

                        return null;
                    }
                };
            }
        };

        asyncCheckpointer.quickSortAndWritePages(scope, taskFactory).get();

        boolean empty = map.isEmpty();
        if (!empty)
            assertTrue("Control map should be empty: " + map.toString(), empty);
    }

    /**
     * @param scope Checkpoint scope.
     * @return map with scope pages.
     */
    private ConcurrentHashMap<FullPageId, FullPageId> createCopyAsMap(CheckpointScope scope) {
        ConcurrentHashMap<FullPageId, FullPageId> map = new ConcurrentHashMap<>();
        List<FullPageIdsBuffer> ids = scope.toBuffers();

        for (FullPageIdsBuffer next : ids) {
            for (FullPageId id : next.toArray()) {
                map.put(id, id);
            }
        }

        return map;
    }

    /**
     * Test page index is growing after sort.
     * @throws Exception if failed.
     */
    @Test
    public void testIndexGrowing() throws Exception {
        CheckpointScope scope = getTestCollection(1024);

        AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(16, getClass().getSimpleName(), log);

        final IgniteClosure<FullPageId[], Callable<Void>> taskFactory = new IgniteClosure<FullPageId[], Callable<Void>>() {
            @Override public Callable<Void> apply(final FullPageId[] ids) {
                return new Callable<Void>() {
                    @Override public Void call() throws Exception {

                        long prevIdx = -1;
                        long prevPart = -1;

                        for (FullPageId id : ids) {
                            long partId = partId(id.pageId());
                            long idx = pageIndex(id.pageId());
                            //System.out.println(partId + " idx= " + idx);
                            if (prevIdx >= 0 && prevPart >= 0) {
                                if (partId < prevPart)
                                    throw new IllegalStateException("Invalid order " + partId + " idx=" + prevPart);

                                if (idx < prevIdx && partId == prevPart)
                                    throw new IllegalStateException("Invalid order " + prevIdx + " idx=" + idx);
                            }

                            prevPart = partId;
                            prevIdx = idx;
                        }
                        return null;
                    }
                };
            }
        };

        asyncCheckpointer.quickSortAndWritePages(scope, taskFactory).get();
    }


    /**
     * Uses control map to verify no elements were lost
     * @throws Exception if failed.
     */
    @Test
    public void testWithEviction() throws Exception {
        final CheckpointScope scope = getTestCollection();
        final ConcurrentHashMap<FullPageId, FullPageId> ctrlMap = createCopyAsMap(scope);

        final CountDownLatch evictingThreadStarted = new CountDownLatch(1);
        final Random random = new Random();
        final Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                while(!Thread.currentThread().isInterrupted()) {
                    evictingThreadStarted.countDown();

                    List<GridMultiCollectionWrapper<FullPageId>> pages = U.field(scope, "pages");
                    try {
                        int i = random.nextInt(pages.size());
                        GridMultiCollectionWrapper<FullPageId> ids = pages.get(i);
                        int i1 = ids.collectionsSize();

                        int i2 = random.nextInt(i1);
                        Collection<FullPageId> cpPages = ids.innerCollection(i2);

                        int size = cpPages.size();
                        if (size == 0)
                            continue;

                        FullPageId page = cpPages.iterator().next();

                        cpPages.remove(page);
                        ctrlMap.remove(page);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread.start();

        evictingThreadStarted.await();

        final AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(16, getClass().getSimpleName(), log);

        final IgniteClosure<FullPageId[], Callable<Void>> taskFactory = new IgniteClosure<FullPageId[], Callable<Void>>() {
            @Override public Callable<Void> apply(final FullPageId[] ids) {
                return new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        for (FullPageId id : ids) {
                            ctrlMap.remove(id);
                        }

                        return null;
                    }
                };
            }
        };

        asyncCheckpointer.quickSortAndWritePages(scope, taskFactory).get();
        thread.interrupt();

        boolean empty = ctrlMap.isEmpty();

        if (!empty)
            assertTrue("Control map should be empty: " + ctrlMap.toString(), empty);
    }

    /**
     * @return test pages set
     */
    @NotNull static CheckpointScope getTestCollection() {
        int pow = (int)Math.pow(10, 7);

        return getTestCollection(pow / CheckpointScope.EXPECTED_SEGMENTS_COUNT);
    }

    /**
     * @return test pages set
     */
    private static CheckpointScope getTestCollection(int pagesPerSegment) {
        int regions = 1;
        CheckpointScope scope = new CheckpointScope(regions);
        Random random = new Random();

        for (int r = 0; r < regions; r++) {
            int segments = CheckpointScope.EXPECTED_SEGMENTS_COUNT;

            PagesConcurrentHashSet[] arrFromRegion = new PagesConcurrentHashSet[segments];

            for (int s = 0; s < segments; s++) {
                PagesConcurrentHashSet set = arrFromRegion[s] = new PagesConcurrentHashSet();
                 
                for (int p = 0; p < pagesPerSegment; p++) {
                    long pageId = randomPageId(random);

                    set.add(new FullPageId(pageId, 123));
                }
            }

            scope.addDataRegionCpPages(arrFromRegion);
        }

        return scope;
    }

    /**
     * @param random Random.
     * @return page ID.
     */
    private static long randomPageId(Random random) {
        int partId = random.nextInt(1024);
        int pageIdx = random.nextInt(1000000);

        return PageIdUtils.pageId(
            partId,
            (byte)0,
            pageIdx);
    }
}
