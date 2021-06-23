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

import java.util.Iterator;
import java.util.function.Predicate;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
public class TreeIndexSpoolExecutionTest extends AbstractExecutionTest {
    /** */
    @Test
    public void testIndexSpool() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        int inBufSize = Commons.IN_BUFFER_SIZE;

        int[] sizes = {1, inBufSize / 2 - 1, inBufSize / 2, inBufSize / 2 + 1, inBufSize, inBufSize + 1, inBufSize * 4};

        for (int size : sizes) {
            // (filter, lower, upper, expected result size)
            TestParams[] testParams;

            if (size == 1) {
                testParams = new TestParams[] {
                    new TestParams(null, new Object[] {null, null, null}, new Object[] {null, null, null}, 1),
                    new TestParams(null, new Object[] {0, null, null}, new Object[] {0, null, null}, 1)
                };
            }
            else {
                testParams = new TestParams[] {
                    new TestParams(
                        null,
                        new Object[] {null, null, null},
                        new Object[] {null, null, null},
                        size
                    ),
                    new TestParams(
                        null,
                        new Object[] {size / 2, null, null},
                        new Object[] {size / 2, null, null},
                        1
                    ),
                    new TestParams(
                        null,
                        new Object[] {size / 2 + 1, null, null},
                        new Object[] {size / 2 + 1, null, null},
                        1
                    ),
                    new TestParams(
                        null,
                        new Object[] {size / 2, null, null},
                        new Object[] {null, null, null},
                        size - size / 2
                    ),
                    new TestParams(
                        null,
                        new Object[] {null, null, null},
                        new Object[] {size / 2, null, null},
                        size / 2 + 1
                    ),
                    new TestParams(
                        (r -> ((int)r[0]) < size / 2),
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
                    assertTrue(first, "Rewind right");

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
                () -> lower,
                () -> upper
            );

            spool.register(singletonList(scan));

            RootRewindable<Object[]> root = new RootRewindable<>(ctx, rowType);
            root.register(spool);

            for (TestParams param : testParams) {
                log.info("Check: param=" + param);

                // Set up bounds
                testFilter.delegate = param.pred;
                System.arraycopy(param.lower, 0, lower, 0, lower.length);
                System.arraycopy(param.upper, 0, upper, 0, upper.length);

                int cnt = 0;

                while (root.hasNext()) {
                    root.next();

                    cnt++;
                }

                assertEquals(param.expectedResultSize, cnt, "Invalid result size");

                root.rewind();
            }

            root.closeRewindableRoot();
        }
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
    private static class TestParams {
        /** */
        final Predicate<Object[]> pred;

        /** */
        final Object[] lower;

        /** */
        final Object[] upper;

        /** */
        final int expectedResultSize;

        /** */
        private TestParams(Predicate<Object[]> pred, Object[] lower, Object[] upper, int expectedResultSize) {
            this.pred = pred;
            this.lower = lower;
            this.upper = upper;
            this.expectedResultSize = expectedResultSize;
        }
    }
}
