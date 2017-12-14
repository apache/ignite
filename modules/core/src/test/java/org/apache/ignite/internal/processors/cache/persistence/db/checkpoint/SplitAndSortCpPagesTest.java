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
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointScope;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class SplitAndSortCpPagesTest {
    public static final int MILLIONS_OF_PAGES = 4;
    private final GridCacheDatabaseSharedManager mgr = getManager();
    private static final ForkJoinPool pool = new ForkJoinPool();

    @AfterClass
    public static void close() {
        pool.shutdown();
        try {
            if(!pool.awaitTermination(1, TimeUnit.SECONDS))
                pool.shutdownNow();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSpeedOfSort() {
        CheckpointScope coll = getTestCollection();

        final GridMultiCollectionWrapper<FullPageId> ids = mgr.splitAndSortCpPagesIfNeeded(coll);

        assertEquals(ids.size(), coll.totalCpPages());
        validateOrder(ids, coll.totalCpPages());
    }

    static void validateOrder(Iterable<FullPageId> ids, int size) {
        FullPageId prevId = null;
        for (FullPageId next : ids) {
            if (prevId != null) {
                final boolean condition = GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR.compare(next, prevId) >= 0;
                if (!condition)
                    assertTrue("Incorrect order of pages: [\nprev=" + prevId + ",\n cur=" + next + "]",
                        condition);
            }
            prevId = next;
        }
    }

    @NotNull private GridCacheDatabaseSharedManager getManager() {
        final GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        final IgniteConfiguration cfg = new IgniteConfiguration();
        final DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        cfg.setDataStorageConfiguration(dsCfg);
        Mockito.when(ctx.config()).thenReturn(cfg);
        return new GridCacheDatabaseSharedManager(ctx);
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
                for (int k = 0; k < MILLIONS_OF_PAGES; k++) {
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
