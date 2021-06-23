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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class TableSpoolExecutionTest extends AbstractExecutionTest {
    /** */
    @Test
    public void testLazyTableSpool() {
        checkTableSpool(
            (ctx, rowType) -> new TableSpoolNode<>(ctx, rowType, true)
        );
    }

    /** */
    @Test
    public void testEagerTableSpool() {
        checkTableSpool(
            (ctx, rowType) -> new TableSpoolNode<>(ctx, rowType, false)
        );
    }

    /**
     * Ensure eager spool reads underlying input till the end before emmitting
     * the very first row.
     */
    @Test
    public void testEagerSpoolReadsWholeInput() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        int inBufSize = Commons.IN_BUFFER_SIZE;

        int[] sizes = {inBufSize / 2, inBufSize, inBufSize + 1, inBufSize * 2};

        for (int size : sizes) {
            log.info("Check: size=" + size);

            AtomicReference<Iterator<Object[]>> itRef = new AtomicReference<>();

            ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, new Iterable<Object[]>() {
                @NotNull @Override public Iterator<Object[]> iterator() {
                    if (itRef.get() != null)
                        throw new AssertionError();

                    itRef.set(IntStream.range(0, size).boxed().map(i -> new Object[]{i}).iterator());

                    return itRef.get();
                }
            });

            TableSpoolNode<Object[]> spool = new TableSpoolNode<>(ctx, rowType, false);

            spool.register(singletonList(scan));

            RootNode<Object[]> root = new RootNode<>(ctx, rowType);
            root.register(spool);

            assertTrue(root.hasNext());

            root.next();

            assertFalse(itRef.get().hasNext());
        }
    }

    /** */
    public void checkTableSpool(
        BiFunction<ExecutionContext<Object[]>, RelDataType, TableSpoolNode<Object[]>> spoolFactory) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        int inBufSize = Commons.IN_BUFFER_SIZE;

        int[] sizes = {1, inBufSize / 2 - 1, inBufSize / 2, inBufSize / 2 + 1, inBufSize, inBufSize + 1, inBufSize * 4};
//        int[] sizes = {inBufSize * 4};
        int rewindCnts = 32;

        for (int size : sizes) {
            log.info("Check: size=" + size);

            ScanNode<Object[]> right = new ScanNode<>(ctx, rowType, new TestTable(size, rowType) {
                boolean first = true;

                @Override public @NotNull Iterator<Object[]> iterator() {
                    assertTrue(first, "Rewind table");

                    first = false;
                    return super.iterator();
                }
            });

            TableSpoolNode<Object[]> spool = spoolFactory.apply(ctx, rowType);

            spool.register(singletonList(right));

            RootRewindable<Object[]> root = new RootRewindable<>(ctx, rowType);
            root.register(spool);

            for (int i = 0; i < rewindCnts; ++i) {
                int cnt = 0;

                while (root.hasNext()) {
                    root.next();

                    cnt++;
                }

                assertEquals(size, cnt, "Invalid result size");

                root.rewind();
            }
        }
    }
}
