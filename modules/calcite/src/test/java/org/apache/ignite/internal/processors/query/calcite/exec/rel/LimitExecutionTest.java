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

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Test LimitNode execution.
 */
public class LimitExecutionTest extends AbstractExecutionTest {
    /** */
    @Test
    public void testLimit() throws Exception {
        checkLimit(0, 1);
        checkLimit(1, 0);
        checkLimit(1, 1);
        checkLimit(0, inBufSize);
        checkLimit(inBufSize, 0);
        checkLimit(inBufSize, inBufSize);
        checkLimit(inBufSize - 1, 1);
        checkLimit(2000, 0);
        checkLimit(0, 3000);
        checkLimit(2000, 3000);
    }

    /** Tests Sort node can limit its output when fetch param is set. */
    @Test
    public void testSortLimit() throws Exception {
        checkLimitSort(0, 1);
        checkLimitSort(1, 0);
        checkLimitSort(1, 1);
        checkLimitSort(0, inBufSize);
        checkLimitSort(inBufSize, 0);
        checkLimitSort(inBufSize, inBufSize);
        checkLimitSort(inBufSize - 1, 1);
        checkLimitSort(2000, 0);
        checkLimitSort(0, 3000);
        checkLimitSort(2000, 3000);
    }

    /**
     * @param offset Rows offset.
     * @param fetch Fetch rows count (zero means unlimited).
     */
    private void checkLimitSort(int offset, int fetch) {
        assert offset >= 0;
        assert fetch >= 0;

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class);

        RootNode<Object[]> rootNode = new RootNode<>(ctx, rowType);

        SortNode<Object[]> sortNode = new SortNode<>(ctx, rowType, F::compareArrays, () -> offset,
            fetch == 0 ? null : () -> fetch);

        List<Object[]> data = IntStream.range(0, SourceNode.IN_BUFFER_SIZE + fetch + offset).boxed()
            .map(i -> new Object[] {i}).collect(Collectors.toList());
        Collections.shuffle(data);

        ScanNode<Object[]> srcNode = new ScanNode<>(ctx, rowType, data);

        rootNode.register(sortNode);

        sortNode.register(srcNode);

        for (int i = 0; i < offset + fetch; i++) {
            assertTrue(rootNode.hasNext());
            assertEquals(i, rootNode.next()[0]);
        }

        assertEquals(fetch == 0, rootNode.hasNext());
    }

    /**
     * @param offset Rows offset.
     * @param fetch Fetch rows count (zero means unlimited).
     */
    private void checkLimit(int offset, int fetch) {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class);

        RootNode<Object[]> rootNode = new RootNode<>(ctx, rowType);
        LimitNode<Object[]> limitNode = new LimitNode<>(ctx, rowType, () -> offset, fetch == 0 ? null : () -> fetch);
        SourceNode srcNode = new SourceNode(ctx, rowType);

        rootNode.register(limitNode);
        limitNode.register(srcNode);

        if (fetch > 0) {
            for (int i = offset; i < offset + fetch; i++) {
                assertTrue(rootNode.hasNext());
                assertEquals(i, rootNode.next()[0]);
            }

            assertFalse(rootNode.hasNext());
            assertEquals(srcNode.requested.get(), offset + fetch);
        }
        else {
            assertTrue(rootNode.hasNext());
            assertEquals(offset, rootNode.next()[0]);
            assertTrue(srcNode.requested.get() > offset);
        }
    }

    /** */
    private static class SourceNode extends AbstractNode<Object[]> {
        /** */
        AtomicInteger requested = new AtomicInteger();

        /** */
        public SourceNode(ExecutionContext<Object[]> ctx, RelDataType rowType) {
            super(ctx, rowType);
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected Downstream<Object[]> requestDownstream(int idx) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void request(int rowsCnt) {
            int r = requested.getAndAdd(rowsCnt);

            context().execute(() -> {
                for (int i = 0; i < rowsCnt; i++)
                    downstream().push(new Object[] {r + i});
            }, this::onError);
        }
    }
}
