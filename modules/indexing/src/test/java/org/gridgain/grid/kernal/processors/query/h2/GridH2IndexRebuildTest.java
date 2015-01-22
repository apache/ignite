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

package org.gridgain.grid.kernal.processors.query.h2;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.query.*;
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
    private static class SleepingH2Indexing extends GridH2Indexing {
        /** */
        private volatile boolean sleepInRebuild;

        /** */
        private volatile boolean interrupted;

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

        final IgniteFuture<?> fut1 = grid(0).cache(null).queries().rebuildIndexes(TestValue1.class);

        assertFalse(fut1.isCancelled());

        fut1.get();

        final IgniteFuture<?> fut2 = grid(0).cache(null).queries().rebuildAllIndexes();

        assertFalse(fut2.isCancelled());

        fut2.get();
    }

    /**
     * @throws Exception if failed.
     */
    private void checkCancel(final IgniteFuture<?> fut) throws Exception {
        assertTrue(fut.cancel());

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                fut.get();
                return null;
            }
        }, IgniteFutureCancelledException.class, null);

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
