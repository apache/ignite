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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

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
    private static class SleepingH2Indexing extends IgniteH2Indexing {
        /** */
        private volatile boolean sleepInRebuild;

        /** */
        private volatile CountDownLatch interrupted;

        /**
         * Constructor.
         */
        public SleepingH2Indexing() {
            spi = this;
        }

        /** {@inheritDoc} */
        @Override public void rebuildIndexes(@Nullable String spaceName, GridQueryTypeDescriptor type) {
            if (sleepInRebuild) {
                try {
                    U.sleep(Long.MAX_VALUE);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    interrupted.countDown();
                }
            }

            super.rebuildIndexes(spaceName, type);
        }
    }

    /** */
    private static SleepingH2Indexing spi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridQueryProcessor.idxCls = SleepingH2Indexing.class;

        return cfg;
    }

    /**
     * Value class with regular and compound indexes.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestValue1 {
        /** */
        @QuerySqlField(index = true)
        private long val1;

        /** */
        @QuerySqlField(index = true)
        private String val2;

        /** */
        @QuerySqlField(groups = "group1")
        private int val3;

        /** */
        @QuerySqlField(groups = "group1")
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
        @QuerySqlField(index = true)
        private long val1;

        /** */
        @QueryTextField
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

    /** {@inheritDoc} */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[] {
            Integer.class, TestValue1.class,
            Integer.class, TestValue2.class
        };
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebuildIndexes() throws Exception {
        grid(0).context().query().rebuildIndexes(null, ArrayList.class.getName()).get();

        grid(0).context().query().rebuildAllIndexes().get();

        IgniteCache<Integer, TestValue1> cache1 = grid(0).cache(null);
        IgniteCache<Integer, TestValue2> cache2 = grid(0).cache(null);

        for (int i = 0; i < ENTRY_CNT; i++) {
            cache1.put(i, new TestValue1(i, "val2-" + i, i, i));
            cache2.put(ENTRY_CNT * 2 + i, new TestValue2(i, "val2-" + i));
        }

        SqlQuery<Integer, TestValue1> qry1 = new SqlQuery(TestValue1.class, "val1 = 9000");

        SqlQuery<Integer, TestValue1> qry2 = new SqlQuery(TestValue1.class, "val2 = 'val2-9000'");

        SqlQuery<Integer, TestValue1> qry3 = new SqlQuery(TestValue1.class, "val3 = 9000 and val4 = 9000");

        SqlQuery<Integer, TestValue2> qry4 = new SqlQuery(TestValue2.class, "val1 = 9000");

        SqlQuery<Integer, TestValue2> qry5 = new SqlQuery(TestValue2.class, "val2 = 'val2-9000'");

        assertEquals(1, cache1.query(qry1).getAll().size());
        assertEquals(1, cache1.query(qry2).getAll().size());
        assertEquals(1, cache1.query(qry3).getAll().size());
        assertEquals(1, cache2.query(qry4).getAll().size());
        assertEquals(1, cache2.query(qry5).getAll().size());

        for (int i = 0; i < ENTRY_CNT / 2; i++) {
            cache1.remove(i);
            cache2.remove(ENTRY_CNT * 2 + i);
        }

        grid(0).context().query().rebuildIndexes(null, TestValue1.class.getName()).get();
        grid(0).context().query().rebuildIndexes(null, TestValue2.class.getName()).get();

        assertEquals(1, cache1.query(qry1).getAll().size());
        assertEquals(1, cache1.query(qry2).getAll().size());
        assertEquals(1, cache1.query(qry3).getAll().size());
        assertEquals(1, cache2.query(qry4).getAll().size());
        assertEquals(1, cache2.query(qry5).getAll().size());

        grid(0).context().query().rebuildAllIndexes().get();

        assertEquals(1, cache1.query(qry1).getAll().size());
        assertEquals(1, cache1.query(qry2).getAll().size());
        assertEquals(1, cache1.query(qry3).getAll().size());
        assertEquals(1, cache2.query(qry4).getAll().size());
        assertEquals(1, cache2.query(qry5).getAll().size());
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebuildInterrupted() throws Exception {
        spi.sleepInRebuild = true;

        IgniteCache<Integer, TestValue1> cache1 = grid(0).cache(null);
        IgniteCache<Integer, TestValue2> cache2 = grid(0).cache(null);

        cache1.put(0, new TestValue1(0, "val0", 0 ,0));
        cache2.put(1, new TestValue2(0, "val0"));

        checkCancel(grid(0).context().query().rebuildIndexes(null, "TestValue1"));

        checkCancel(grid(0).context().query().rebuildAllIndexes());

        spi.sleepInRebuild = false;

        final IgniteInternalFuture<?> fut1 = grid(0).context().query().rebuildIndexes(null, TestValue1.class.getName());

        assertFalse(fut1.isCancelled());

        fut1.get();

        final IgniteInternalFuture<?> fut2 = grid(0).context().query().rebuildAllIndexes();;

        assertFalse(fut2.isCancelled());

        fut2.get();
    }

    /**
     * @param fut Future.
     * @throws Exception if failed.
     */
    private void checkCancel(final IgniteInternalFuture<?> fut) throws Exception {
        spi.interrupted = new CountDownLatch(1);

        assertTrue(fut.cancel());

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                fut.get();
                return null;
            }
        }, IgniteFutureCancelledCheckedException.class, null);

        assertTrue(spi.interrupted.await(5, TimeUnit.SECONDS));
    }
}