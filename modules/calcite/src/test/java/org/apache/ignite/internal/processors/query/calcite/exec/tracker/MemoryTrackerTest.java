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

package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class MemoryTrackerTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testBatchesAllocation() {
        MemoryTracker globalTracker = new GlobalMemoryTracker(10_000_000L);
        MemoryTracker qryTracker = new QueryMemoryTracker(globalTracker, 1_000_000L);
        RowTracker<Object[]> rowTracker1 = new ExecutionNodeMemoryTracker<>(qryTracker, 0L);
        RowTracker<Object[]> rowTracker2 = new ExecutionNodeMemoryTracker<>(qryTracker, 0L);

        rowTracker1.onRowAdded(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());

        rowTracker1.onRowAdded(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());

        rowTracker1.onRowAdded(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());

        rowTracker1.onRowRemoved(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());

        rowTracker1.onRowRemoved(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());

        rowTracker2.onRowAdded(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE * 2, qryTracker.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE * 2, globalTracker.allocated());

        rowTracker1.onRowRemoved(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());

        rowTracker2.onRowRemoved(new Object[1]);

        assertEquals(0L, qryTracker.allocated());
        assertEquals(0L, globalTracker.allocated());
    }

    /** */
    @Test
    public void testReset() {
        MemoryTracker globalTracker = new GlobalMemoryTracker(10_000_000L);
        MemoryTracker qryTracker1 = new QueryMemoryTracker(globalTracker, 1_000_000L);
        RowTracker<Object[]> rowTracker1 = new ExecutionNodeMemoryTracker<>(qryTracker1, 0L);
        MemoryTracker qryTracker2 = new QueryMemoryTracker(globalTracker, 1_000_000L);
        RowTracker<Object[]> rowTracker2 = new ExecutionNodeMemoryTracker<>(qryTracker2, 0L);
        MemoryTracker qryTracker3 = new QueryMemoryTracker(globalTracker, 1_000_000L);
        RowTracker<Object[]> rowTracker3 = new ExecutionNodeMemoryTracker<>(qryTracker3, 0L);

        rowTracker1.onRowAdded(new Object[1]);
        rowTracker2.onRowAdded(new Object[1]);
        rowTracker3.onRowAdded(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker1.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker2.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker3.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE * 3, globalTracker.allocated());

        rowTracker1.reset();

        assertEquals(0, qryTracker1.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE * 2, globalTracker.allocated());

        qryTracker2.reset();

        assertEquals(0, qryTracker2.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());

        qryTracker3.reset();

        assertEquals(0, qryTracker3.allocated());
        assertEquals(0, globalTracker.allocated());
    }

    /** */
    @Test
    public void testRemoveOverflow() {
        MemoryTracker globalTracker = new GlobalMemoryTracker(10_000_000L);
        MemoryTracker qryTracker = new QueryMemoryTracker(globalTracker, 1_000_000L);
        RowTracker<Object[]> rowTracker = new ExecutionNodeMemoryTracker<>(qryTracker, 0L);

        rowTracker.onRowAdded(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker.allocated());
        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());

        rowTracker.onRowRemoved(new Object[1]);
        rowTracker.onRowRemoved(new Object[1]);

        assertEquals(0L, qryTracker.allocated());
        assertEquals(0L, globalTracker.allocated());
    }

    /** */
    @Test
    public void testConcurrentModification() throws Exception {
        MemoryTracker globalTracker = new GlobalMemoryTracker(10_000_000L);
        MemoryTracker qryTracker = new QueryMemoryTracker(globalTracker, 1_000_000L);
        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                qryTracker.onMemoryAllocated(1_000L);
                qryTracker.onMemoryReleased(1_000L);

                if (ThreadLocalRandom.current().nextInt(10) == 0)
                    qryTracker.reset();

                assertTrue(qryTracker.allocated() >= 0);
            }
        }, 20, "mem-tracker-modifier");

        doSleep(1_000L);

        stop.set(true);

        fut.get();

        assertEquals(0L, qryTracker.allocated());
        assertEquals(0L, globalTracker.allocated());
    }

    /** */
    @Test
    public void testQuotaExceed() {
        MemoryTracker globalTracker = new GlobalMemoryTracker(1_000_000L);
        MemoryTracker qryTracker1 = new QueryMemoryTracker(globalTracker, 900_000L);
        MemoryTracker qryTracker2 = new QueryMemoryTracker(globalTracker, 900_000L);
        qryTracker1.onMemoryAllocated(499_000L);
        qryTracker2.onMemoryAllocated(499_000L);
        RowTracker<Object[]> rowTracker1 = new ExecutionNodeMemoryTracker<>(qryTracker1, 1_000L);
        RowTracker<Object[]> rowTracker2 = new ExecutionNodeMemoryTracker<>(qryTracker2, 1_000L);

        GridTestUtils.assertThrows(log, () -> rowTracker1.onRowAdded(new Object[1]), IgniteException.class,
            "Global memory quota");

        GridTestUtils.assertThrows(log, () -> rowTracker2.onRowAdded(new Object[1]), IgniteException.class,
            "Global memory quota");

        assertEquals(499_000L, qryTracker1.allocated());
        assertEquals(499_000L, qryTracker2.allocated());

        qryTracker1.onMemoryReleased(499_000L);
        qryTracker2.onMemoryAllocated(400_000L);

        assertEquals(0L, qryTracker1.allocated());
        assertEquals(899_000L, qryTracker2.allocated());

        rowTracker1.onRowAdded(new Object[1]);

        assertEquals(ExecutionNodeMemoryTracker.BATCH_SIZE, qryTracker1.allocated());

        GridTestUtils.assertThrows(log, () -> rowTracker2.onRowAdded(new Object[1]), IgniteException.class,
            "Query quota");

        assertEquals(899_000L, qryTracker2.allocated());
        assertEquals(899_000L + ExecutionNodeMemoryTracker.BATCH_SIZE, globalTracker.allocated());
    }

    /** */
    @Test
    public void testObjectSizeCalculator() {
        // Can't check absolute values, since they depend on JDK. Check only relative invariants.
        ObjectSizeCalculator<Object[]> calc = new ObjectSizeCalculator<>();

        // Simple classes.
        assertTrue(calc.sizeOf(new Object[] {(byte)1}) <= calc.sizeOf(new Object[] {1}));
        assertTrue(calc.sizeOf(new Object[] {1}) <= calc.sizeOf(new Object[] {1L}));
        assertTrue(calc.sizeOf(new Object[] {"0"}) < calc.sizeOf(new Object[] {"0123456789"}));

        // User defined classes.
        Test2 objTest2 = new Test2();
        Test3 objTest3 = new Test3();

        assertTrue(calc.sizeOf(new Object[] {objTest2}) <= calc.sizeOf(new Object[] {objTest3}));

        Test4 objTest4 = new Test4();
        Test4 objTest4ref = new Test4();

        assertEquals(calc.sizeOf(new Object[] {objTest4}), calc.sizeOf(new Object[] {objTest4ref}));

        objTest4ref.field = objTest3;

        assertTrue(calc.sizeOf(new Object[] {objTest4}) < calc.sizeOf(new Object[] {objTest4ref}));

        objTest4ref.field = objTest4ref; // Already processed objects are not counted.

        assertEquals(calc.sizeOf(new Object[] {objTest4}), calc.sizeOf(new Object[] {objTest4ref}));
    }

    /** */
    public static class Test1 {
        /** */
        private byte field1;
    }

    /** */
    public static class Test2 extends Test1 {
        /** */
        private long field2;
    }

    /** */
    public static class Test3 extends Test2 {
        /** */
        private byte field3;
    }

    /** */
    public static class Test4 {
        /** */
        private Object field;
    }
}
