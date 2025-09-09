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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test execution/idle time calculation.
 */
public class TimeCalculationExecutionTest extends AbstractExecutionTest {
    /** */
    @Test
    public void testTime() throws Exception {
        long sleepTime = 200;

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class);

        long ts0 = System.nanoTime();

        RootNode<Object[]> rootNode = new RootNode<>(ctx, rowType);
        SourceNode srcNode = new SourceNode(ctx, rowType);

        rootNode.register(srcNode);

        // Check time calculation after execution.
        while (System.nanoTime() < ts0 + TimeUnit.MILLISECONDS.toNanos(sleepTime)) {
            assertTrue(rootNode.hasNext());
            rootNode.next();
        }

        srcNode.latch = new CountDownLatch(1);

        rootNode.hasNext();

        assertTrue(srcNode.latch.await(1_000L, TimeUnit.MILLISECONDS));

        long execTime0 = rootNode.execTime();
        long idleTime0 = rootNode.idleTime();

        long ts1 = System.nanoTime();

        assertTrue(execTime0 + idleTime0 <= TimeUnit.NANOSECONDS.toMillis(ts1 - ts0) + 1);
        assertTrue(execTime0 >= idleTime0);

        // Check time calculation during idle.
        doSleep(sleepTime);

        long ts2 = System.nanoTime();

        long execTime1 = rootNode.execTime();
        long idleTime1 = rootNode.idleTime();

        assertEquals(execTime0, execTime1);
        assertTrue(idleTime1 - idleTime0 >= sleepTime);

        // Check time calculation during execution.
        while (System.nanoTime() < ts2 + TimeUnit.MILLISECONDS.toNanos(sleepTime)) {
            assertTrue(rootNode.hasNext());
            rootNode.next();

            long execTime2 = rootNode.execTime();
            long idleTime2 = rootNode.idleTime();

            assertTrue(idleTime2 >= idleTime1);
            assertTrue(idleTime2 <= idleTime1 + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ts2) + 1);
            assertTrue(execTime2 >= execTime1);
            assertTrue(execTime2 <= execTime1 + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ts2) + 1);
        }

        // Check time calculation after stop.
        srcNode.stop = true;
        rootNode.hasNext();
        assertTrue(GridTestUtils.waitForCondition(() -> srcNode.stopped, 1_000L));

        long execTime3 = rootNode.execTime();
        long idleTime3 = rootNode.idleTime();

        doSleep(sleepTime);

        assertEquals(execTime3, rootNode.execTime());
        assertEquals(idleTime3, rootNode.idleTime());
    }

    /** */
    private static class SourceNode extends AbstractNode<Object[]> {
        /** */
        volatile int requested;

        /** */
        volatile boolean stop;

        /** */
        volatile boolean stopped;

        /** */
        volatile CountDownLatch latch;

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
            requested = rowsCnt;

            context().execute(this::push, this::onError);
        }

        /** */
        public void push() throws Exception {
            if (stop) {
                downstream().end();
                stopped = true;
                return;
            }

            int rowsCnt = ThreadLocalRandom.current().nextInt(requested) + 1;

            for (int i = 0; i < rowsCnt; i++)
                downstream().push(new Object[] {0});

            requested -= rowsCnt;

            if (requested > 0)
                context().execute(this::push, this::onError);
            else {
                if (latch != null)
                    latch.countDown();
            }
        }
    }
}
