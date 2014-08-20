/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 */
public class GridH2IndexRebuildTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 1;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * Overrides rebuildIndexes to check it can be interrupted.
     */
    private static class SleepingH2Indexing extends GridH2IndexingSpi {
        /** */
        private volatile boolean sleepInRebuild;

        /** */
        private volatile boolean interrupted;

        /** {@inheritDoc} */
        @Override public void rebuildIndexes(@Nullable String spaceName, GridIndexingTypeDescriptor type) {
            if (sleepInRebuild) {
                try {
                    U.sleep(Long.MAX_VALUE);
                }
                catch (GridInterruptedException ignored) {
                    interrupted = true;
                }
            }

            super.rebuildIndexes(spaceName, type);
        }
    }

    /** */
    private static SleepingH2Indexing spi;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        spi = new SleepingH2Indexing();

        cfg.setIndexingSpi(spi);

        return cfg;
    }

    /**
     * Value class with regular and compound indexes.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestValue1 {
        /** */
        @GridCacheQuerySqlField(index = true)
        private long val1;

        /** */
        @GridCacheQuerySqlField(index = true)
        private String val2;

        /** */
        @GridCacheQuerySqlField(groups = "group1")
        private int val3;

        /** */
        @GridCacheQuerySqlField(groups = "group1")
        private int val4;

        /**
         */
        TestValue1(long val1, String val2, int val3, int val4) {
            this.val1 = val1;
            this.val2 = val2;
            this.val3 = val3;
            this.val4 = val4;
        }
    }

    /**
     * Value class with regular and text indexes.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestValue2 {
        /** */
        @GridCacheQuerySqlField(index = true)
        private long val1;

        /** */
        @GridCacheQueryTextField
        private String val2;

        /**
         */
        TestValue2(long val1, String val2) {
            this.val1 = val1;
            this.val2 = val2;
        }
    }

    /** */
    private static final int ENTRY_CNT = 10000;

    /**
     * @throws Exception if failed.
     */
    public void testRebuildIndexes() throws Exception {
        cache().queries().rebuildIndexes(ArrayList.class).get();

        cache().queries().rebuildAllIndexes().get();

        GridCache<Integer, TestValue1> cache1 = grid(0).cache(null);
        GridCache<Integer, TestValue2> cache2 = grid(0).cache(null);

        for (int i = 0; i < ENTRY_CNT; i++) {
            cache1.put(i, new TestValue1(i, "val2-" + i, i, i));
            cache2.put(ENTRY_CNT * 2 + i, new TestValue2(i, "val2-" + i));
        }

        GridCacheQuery<Map.Entry<Integer, TestValue1>> qry1 =
            cache1.queries().createSqlQuery(TestValue1.class, "val1 = 9000");

        GridCacheQuery<Map.Entry<Integer, TestValue1>> qry2 =
            cache1.queries().createSqlQuery(TestValue1.class, "val2 = 'val2-9000'");

        GridCacheQuery<Map.Entry<Integer, TestValue1>> qry3 =
            cache1.queries().createSqlQuery(TestValue1.class, "val3 = 9000 and val4 = 9000");

        GridCacheQuery<Map.Entry<Integer, TestValue2>> qry4 =
            cache2.queries().createSqlQuery(TestValue2.class, "val1 = 9000");

        GridCacheQuery<Map.Entry<Integer, TestValue2>> qry5 =
            cache2.queries().createFullTextQuery(TestValue2.class, "val2 = 'val2-9000'");

        checkQueryReturnsOneEntry(qry1, qry2, qry3, qry4, qry5);

        for (int i = 0; i < ENTRY_CNT / 2; i++) {
            cache1.remove(i);
            cache2.remove(ENTRY_CNT * 2 + i);
        }

        cache().queries().rebuildIndexes(TestValue1.class).get();
        cache().queries().rebuildIndexes(TestValue2.class).get();

        checkQueryReturnsOneEntry(qry1, qry2, qry3, qry4, qry5);

        cache().queries().rebuildAllIndexes().get();

        checkQueryReturnsOneEntry(qry1, qry2, qry3, qry4, qry5);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebuildInterrupted() throws Exception {
        spi.sleepInRebuild = true;

        GridCache<Integer, TestValue1> cache1 = grid(0).cache(null);
        GridCache<Integer, TestValue2> cache2 = grid(0).cache(null);

        cache1.put(0, new TestValue1(0, "val0", 0 ,0));
        cache2.put(1, new TestValue2(0, "val0"));

        checkCancel(grid(0).cache(null).queries().rebuildIndexes("TestValue1"));

        checkCancel(grid(0).cache(null).queries().rebuildAllIndexes());

        spi.sleepInRebuild = false;

        final GridFuture<?> fut1 = grid(0).cache(null).queries().rebuildIndexes(TestValue1.class);

        assertFalse(fut1.isCancelled());

        fut1.get();

        final GridFuture<?> fut2 = grid(0).cache(null).queries().rebuildAllIndexes();

        assertFalse(fut2.isCancelled());

        fut2.get();
    }

    /**
     * @throws Exception if failed.
     */
    private void checkCancel(final GridFuture<?> fut) throws Exception {
        assertTrue(fut.cancel());

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                fut.get();
                return null;
            }
        }, GridFutureCancelledException.class, null);

        assertTrue(spi.interrupted);

        spi.interrupted = false;
    }

    /**
     * @throws Exception if failed.
     */
    private void checkQueryReturnsOneEntry(GridCacheQuery<?>... qrys) throws Exception {
        for (GridCacheQuery<?> qry : qrys)
            assertEquals(1, qry.execute().get().size());
    }
}
