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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "true")
public class HashIndexSpoolExecutionTest extends AbstractExecutionTest {
    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCnt = 1;
        super.setup();
    }

    /**
     *
     */
    @Test
    public void testIndexSpool() throws Exception {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        int[] sizes = {1, inBufSize / 2 - 1, inBufSize / 2, inBufSize / 2 + 1, inBufSize, inBufSize + 1, inBufSize * 4};
        int[] eqCnts = {1, 10};

        for (int size : sizes) {
            for (int eqCnt : eqCnts) {
                // (filter, search, expected result size)
                GridTuple3<Predicate<Object[]>, Object[], Integer>[] testBounds;

                if (size == 1) {
                    testBounds = new GridTuple3[] {
                        new GridTuple3(null, new Object[] {0, null, null}, eqCnt)
                    };
                }
                else {
                    testBounds = new GridTuple3[] {
                        new GridTuple3(
                            null,
                            new Object[] {size / 2, null, null},
                            eqCnt
                        ),
                        new GridTuple3(
                            null,
                            new Object[] {size / 2 + 1, null, null},
                            eqCnt
                        ),
                        new GridTuple3(
                            (Predicate<Object[]>)r -> ((int)r[2]) == 0,
                            new Object[] {size / 2 + 1, null, null},
                            1
                        )
                    };
                }

                log.info("Check: size=" + size);

                ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, new TestTable(
                    size * eqCnt,
                    rowType,
                    (rowId) -> rowId / eqCnt,
                    (rowId) -> "val_" + (rowId % eqCnt),
                    (rowId) -> rowId % eqCnt
                ) {
                    boolean first = true;

                    @Override public @NotNull Iterator<Object[]> iterator() {
                        assertTrue("Rewind right", first);

                        first = false;
                        return super.iterator();
                    }
                });

                Object[] searchRow = new Object[3];
                TestPredicate testFilter = new TestPredicate();

                IndexSpoolNode<Object[]> spool = IndexSpoolNode.createHashSpool(
                    ctx,
                    rowType,
                    ImmutableBitSet.of(0),
                    testFilter,
                    () -> searchRow,
                    false
                );

                spool.register(Arrays.asList(scan));

                RootRewindable<Object[]> root = new RootRewindable<>(ctx, rowType);
                root.register(spool);

                for (GridTuple3<Predicate<Object[]>, Object[], Integer> bound : testBounds) {
                    log.info("Check: bound=" + bound);

                    // Set up bounds
                    testFilter.delegate = bound.get1();
                    System.arraycopy(bound.get2(), 0, searchRow, 0, searchRow.length);

                    assertEquals("Invalid result size", (int)bound.get3(), root.rowsCount());
                }

                root.closeRewindableRoot();
            }
        }
    }

    /** */
    @Test
    public void testNullsInSearchRow() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        RelDataType rowType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, int.class);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, new TestTable(
            4,
            rowType,
            (rowId) -> (rowId & 1) == 0 ? null : 1,
            (rowId) -> (rowId & 2) == 0 ? null : 1
        ));

        Object[] searchRow = new Object[2];

        IndexSpoolNode<Object[]> spool = IndexSpoolNode.createHashSpool(
            ctx,
            rowType,
            ImmutableBitSet.of(0, 1),
            null,
            () -> searchRow,
            false
        );

        spool.register(scan);

        RootRewindable<Object[]> root = new RootRewindable<>(ctx, rowType);

        root.register(spool);

        List<T2<Object[], Integer>> testBounds = F.asList(
            new T2<>(new Object[] {null, null}, 0),
            new T2<>(new Object[] {1, null}, 0),
            new T2<>(new Object[] {null, 1}, 0),
            new T2<>(new Object[] {1, 1}, 1)
        );

        for (T2<Object[], Integer> bound : testBounds) {
            System.arraycopy(bound.get1(), 0, searchRow, 0, searchRow.length);

            assertEquals("Invalid result size", (int)bound.get2(), root.rowsCount());
        }

        root.closeRewindableRoot();
    }

    /** */
    static class TestPredicate implements Predicate<Object[]> {
        /** */
        Predicate<Object[]> delegate;

        /** {@inheritDoc} */
        @Override public boolean test(Object[] objects) {
            if (delegate == null)
                return true;
            else
                return delegate.test(objects);
        }
    }
}
