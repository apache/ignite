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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.AsyncCheckpointer;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointScope;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;

public class SplitAndSortCpPagesTest {
    /**
     *
     */
    private static final int PAGES_MILLIONS = 2;

    /**
     *
     */
    @Test
    public void testSingleThreadSplitSort() {
        CheckpointScope coll = getTestCollection();

        DataStorageConfiguration dsCfg = config().getDataStorageConfiguration();

        dsCfg.setCheckpointThreads(13);

        Collection<FullPageId[]> ids = coll.splitAndSortCpPagesIfNeeded(dsCfg);

        int sz = 0;

        for (FullPageId[] next : ids) {
            sz += next.length;
        }

        assertEquals(sz, coll.totalCpPages());
        validateOrder(ids);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAsyncLazyCpPagesSubmit() throws Exception {
        final CheckpointScope scope = getTestCollection();

        final AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(6, getClass().getSimpleName());

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

        final CountDownFuture fut = asyncCheckpointer.quickSortAndWritePages(scope, taskFactory);

        fut.get();
        Assert.assertEquals(totalPagesAfterSort.get(), scope.totalCpPages());
    }

    /**
     * @param ids sorted IDs
     */
    private static void validateOrder(Iterable<FullPageId[]> ids) {
        FullPageId prevId = null;
        for (FullPageId[] nextArr : ids) {
            for (FullPageId next : nextArr) {
                if (prevId != null) {
                    final boolean condition = GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR.compare(next, prevId) >= 0;
                    if (!condition)
                        assertTrue("Incorrect order of pages: [\nprev=" + prevId + ",\n cur=" + next + "]",
                            condition);
                }
                prevId = next;
            }
        }
    }

    @NotNull private IgniteConfiguration config() {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        final DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        cfg.setDataStorageConfiguration(dsCfg);
        return cfg;
    }

    @Test
    public void testAsyncAllPagesArePresent() throws Exception {
        final CheckpointScope scope = getTestCollection();
        ConcurrentHashMap<FullPageId, FullPageId> map = new ConcurrentHashMap<>();
        FullPageId[] ids = scope.toArray();

        for (FullPageId id : ids) {
            map.put(id, id);
        }

        final AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(16, getClass().getSimpleName());

        final IgniteClosure<FullPageId[], Callable<Void>> taskFactory = new IgniteClosure<FullPageId[], Callable<Void>>() {
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
        if (!empty) {
            assertTrue("Map should be empty: " + map.toString(), empty);
        }
    }

    @Test
    public void testIndexGrowing() throws Exception {
        final CheckpointScope scope = getTestCollection(1, 1024);

        final AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(16, getClass().getSimpleName());

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
                                if (partId < prevPart) {
                                    throw new IllegalStateException("Invalid order " + partId + " idx=" + prevPart);
                                }
                                if (idx < prevIdx && partId == prevPart) {
                                    throw new IllegalStateException("Invalid order " + prevIdx + " idx=" + idx);
                                }
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
     * @return test pages set
     */
    @NotNull static CheckpointScope getTestCollection() {
        return getTestCollection(PAGES_MILLIONS, 1024);
    }

    /**
     * @return test pages set
     */
    @NotNull private static CheckpointScope getTestCollection(int pagesMillions, int innerCollections) {
        final int regions = 1024;
        final CheckpointScope scope = new CheckpointScope(regions);
        final Random random = new Random();
        for (int i = 0; i < regions; i++) {
            final Collection[] collections = new Collection[innerCollections];
            for (int j = 0; j < innerCollections; j++) {
                final Collection<FullPageId> innerColl = new ArrayList<>();
                collections[j] = innerColl;
                for (int k = 0; k < pagesMillions; k++) {
                    final int curPartId = random.nextInt(1024);
                    final int idx = random.nextInt(1000000);
                    long id = PageIdUtils.pageId(
                        curPartId,
                        (byte)0,
                        idx);

                    innerColl.add(new FullPageId(id, 123));
                }
            }
            final GridMultiCollectionWrapper<FullPageId> e = new GridMultiCollectionWrapper<>(collections);

            scope.addCpPages(e);
        }
        return scope;
    }
}
