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
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.typedef.T2;
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
    public void testSpeedOfSortParallel() {
        Collection<GridMultiCollectionWrapper<FullPageId>> coll = getTestCollection();
        int size = getCpPagesCntFromPagesMulticollection(coll);
        final GridMultiCollectionWrapper<FullPageId> ids = mgr.splitAndSortCpPagesIfNeeded2(pool, new T2<>(coll, size));

        validateOrder(ids, size);
    }


   // @Test
    public void testSpeedOfSortParallelCycle() {
        while (true) {
            testSpeedOfSortParallel();
        }
    }

    @Test
    public void testSpeedOfSort() {
        Collection<GridMultiCollectionWrapper<FullPageId>> coll = getTestCollection();
        int size = getCpPagesCntFromPagesMulticollection(coll);
        final GridMultiCollectionWrapper<FullPageId> ids = mgr.splitAndSortCpPagesIfNeeded(new T2<>(coll, size));

        assertEquals(ids.size(), size);
        validateOrder(ids, size);
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

    static int getCpPagesCntFromPagesMulticollection(Collection<GridMultiCollectionWrapper<FullPageId>> coll) {
        int size = 0;
        for (GridMultiCollectionWrapper<FullPageId> next : coll) {
            size += next.size();
        }
        return size;
    }

    @NotNull static Collection<GridMultiCollectionWrapper<FullPageId>> getTestCollection() {
        Collection<GridMultiCollectionWrapper<FullPageId>> coll = new ArrayList<>();

        final Random random = new Random();
        for (int i = 0; i < 1024; i++) {
            final int innerCollections = 1024;
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

            coll.add(e);
        }
        return coll;
    }
}
