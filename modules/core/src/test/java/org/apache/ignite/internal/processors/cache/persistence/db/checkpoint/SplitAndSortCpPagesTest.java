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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.AsyncCheckpointer;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointScope;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class SplitAndSortCpPagesTest {
    /**
     *
     */
    private static final int PAGES_MILLIONS = 4;

    /**
     *
     */
    @Test
    public void testSpeedOfSort() {
        CheckpointScope coll = getTestCollection();

        final Collection<FullPageId[]> ids = coll.splitAndSortCpPagesIfNeeded(config().getDataStorageConfiguration());

        assertEquals(ids.size(), coll.totalCpPages());
        validateOrder(ids, coll.totalCpPages());
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

                        validateOrder(Collections.singletonList(ids), len);

                        return null;
                    }
                };
            }
        };

        final CountDownFuture fut = asyncCheckpointer.quickSortAndWritePages(scope, taskFactory);

        fut.get();
        Assert.assertEquals(totalPagesAfterSort.get(), scope.totalCpPages());

    }

    static void validateOrder(Iterable<FullPageId[]> ids, int size) {
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

    @NotNull private GridCacheDatabaseSharedManager getManager() {
        final GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        final IgniteConfiguration cfg = config();
        Mockito.when(ctx.config()).thenReturn(cfg);
        return new GridCacheDatabaseSharedManager(ctx);
    }

    @NotNull private IgniteConfiguration config() {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        final DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        cfg.setDataStorageConfiguration(dsCfg);
        return cfg;
    }

    @NotNull static CheckpointScope getTestCollection() {

        final int regions = 1024;
        final CheckpointScope scope = new CheckpointScope(regions);
        final Random random = new Random();
        for (int i = 0; i < regions; i++) {
            final int innerCollections = regions;
            final Collection[] collections = new Collection[innerCollections];
            for (int j = 0; j < innerCollections; j++) {
                final Collection<FullPageId> innerColl = new ArrayList<>();
                collections[j] = innerColl;
                for (int k = 0; k < PAGES_MILLIONS; k++) {
                    final int id = random.nextInt(1_000_000_000);
                    innerColl.add(new FullPageId(id, 123));
                }
            }
            final GridMultiCollectionWrapper<FullPageId> e = new GridMultiCollectionWrapper<>(collections);

            scope.addCpPages(e);
        }
        return scope;
    }
}
