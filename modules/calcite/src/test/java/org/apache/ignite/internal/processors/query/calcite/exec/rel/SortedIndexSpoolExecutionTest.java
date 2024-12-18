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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeCondition;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.lang.GridTuple4;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public class SortedIndexSpoolExecutionTest extends AbstractExecutionTest {
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

        for (int size : sizes) {
            // (filter, lower, upper, expected result size)
            GridTuple4<Predicate<Object[]>, Object[], Object[], Integer>[] testBounds;

            if (size == 1) {
                testBounds = new GridTuple4[] {
                    new GridTuple4(null, new Object[] {null, null, null}, new Object[] {null, null, null}, 1),
                    new GridTuple4(null, new Object[] {0, null, null}, new Object[] {0, null, null}, 1)
                };
            }
            else {
                testBounds = new GridTuple4[] {
                    new GridTuple4(
                        null,
                        new Object[] {null, null, null},
                        new Object[] {null, null, null},
                        size
                    ),
                    new GridTuple4(
                        null,
                        new Object[] {size / 2, null, null},
                        new Object[] {size / 2, null, null},
                        1
                    ),
                    new GridTuple4(
                        null,
                        new Object[] {size / 2 + 1, null, null},
                        new Object[] {size / 2 + 1, null, null},
                        1
                    ),
                    new GridTuple4(
                        null,
                        new Object[] {size / 2, null, null},
                        new Object[] {null, null, null},
                        size - size / 2
                    ),
                    new GridTuple4(
                        null,
                        new Object[] {null, null, null},
                        new Object[] {size / 2, null, null},
                        size / 2 + 1
                    ),
                    new GridTuple4<>(
                        (Predicate<Object[]>)(r -> ((int)r[0]) < size / 2),
                        new Object[] {null, null, null},
                        new Object[] {size / 2, null, null},
                        size / 2
                    ),
                };
            }

            log.info("Check: size=" + size);

            ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, new TestTable(size, rowType) {
                boolean first = true;

                @Override public @NotNull Iterator<Object[]> iterator() {
                    assertTrue("Rewind right", first);

                    first = false;
                    return super.iterator();
                }
            });

            Object[] lower = new Object[3];
            Object[] upper = new Object[3];
            TestPredicate testFilter = new TestPredicate();

            IndexSpoolNode<Object[]> spool = IndexSpoolNode.createTreeSpool(
                ctx,
                rowType,
                RelCollations.of(ImmutableIntList.of(0)),
                (o1, o2) -> o1[0] != null ? ((Comparable)o1[0]).compareTo(o2[0]) : 0,
                testFilter,
                new StaticRangeIterable(lower, upper)
            );

            spool.register(Arrays.asList(scan));

            RootRewindable<Object[]> root = new RootRewindable<>(ctx, rowType);
            root.register(spool);

            for (GridTuple4<Predicate<Object[]>, Object[], Object[], Integer> bound : testBounds) {
                log.info("Check: bound=" + bound);

                // Set up bounds
                testFilter.delegate = bound.get1();
                System.arraycopy(bound.get2(), 0, lower, 0, lower.length);
                System.arraycopy(bound.get3(), 0, upper, 0, upper.length);

                assertEquals("Invalid result size", (int)bound.get4(), root.rowsCount());
            }

            root.closeRewindableRoot();
        }
    }

    /** */
    @Test
    public void testUnspecifiedValuesInSearchRow() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        ScanNode<Object[]> scan = new ScanNode<>(
            ctx,
            rowType,
            new TestTable(100, rowType, rowId -> rowId / 10, rowId -> rowId % 10, rowId -> rowId)
        );

        Object[] lower = new Object[3];
        Object[] upper = new Object[3];

        RelCollation collation = RelCollations.of(ImmutableIntList.of(0, 1));

        IndexSpoolNode<Object[]> spool = IndexSpoolNode.createTreeSpool(
            ctx,
            rowType,
            collation,
            ctx.expressionFactory().comparator(collation),
            v -> true,
            new StaticRangeIterable(lower, upper)
        );

        spool.register(scan);

        RootRewindable<Object[]> root = new RootRewindable<>(ctx, rowType);
        root.register(spool);

        Object x = ctx.unspecifiedValue(); // Unspecified filter value.

        // Test tuple (lower, upper, expected result size).
        List<T3<Object[], Object[], Integer>> testBounds = F.asList(
            new T3<>(new Object[] {x, x, x}, new Object[] {x, x, x}, 100),
            new T3<>(new Object[] {0, 0, x}, new Object[] {4, 9, x}, 50),
            new T3<>(new Object[] {0, x, x}, new Object[] {4, 9, x}, 50),
            new T3<>(new Object[] {0, 0, x}, new Object[] {4, x, x}, 50),
            new T3<>(new Object[] {4, x, x}, new Object[] {4, x, x}, 10),
            // This is a special case, we shouldn't compare the next field if current field bound value is null, or we
            // can accidentally find wrong lower/upper row. So, {x, 4} bound must be converted to {x, x} and redunant
            // rows must be filtered out by predicate.
            new T3<>(new Object[] {x, 4, x}, new Object[] {x, 5, x}, 100)
        );

        for (T3<Object[], Object[], Integer> bound : testBounds) {
            log.info("Check: lowerBound=" + Arrays.toString(bound.get1()) +
                ", upperBound=" + Arrays.toString(bound.get2()));

            // Set up bounds.
            System.arraycopy(bound.get1(), 0, lower, 0, lower.length);
            System.arraycopy(bound.get2(), 0, upper, 0, upper.length);

            assertEquals("Invalid result size", (int)bound.get3(), root.rowsCount());
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

    /** */
    private static class StaticRangeIterable implements RangeIterable<Object[]> {
        /** */
        private final Object[] lower;

        /** */
        private final Object[] upper;

        /** */
        private StaticRangeIterable(Object[] lower, Object[] upper) {
            this.lower = lower;
            this.upper = upper;
        }

        /** {@inheritDoc} */
        @Override public boolean multiBounds() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Iterator<RangeCondition<Object[]>> iterator() {
            RangeCondition<Object[]> range = new RangeCondition<Object[]>() {
                @Override public Object[] lower() {
                    return lower;
                }

                @Override public Object[] upper() {
                    return upper;
                }

                @Override public boolean lowerInclude() {
                    return true;
                }

                @Override public boolean upperInclude() {
                    return true;
                }
            };

            return Collections.singleton(range).iterator();
        }
    }
}
