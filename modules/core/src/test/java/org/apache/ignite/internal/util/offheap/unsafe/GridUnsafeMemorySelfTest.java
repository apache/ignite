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

package org.apache.ignite.internal.util.offheap.unsafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.LongAdder8;

/**
 * Tests unsafe memory.
 */
public class GridUnsafeMemorySelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testBytes() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(64);

        String s = "123";

        byte[] bytes = s.getBytes();

        int size = bytes.length * 2;

        long addr = mem.allocate(size);

        try {
            mem.writeBytes(addr, bytes);

            byte[] read = mem.readBytes(addr, bytes.length);

            assert Arrays.equals(bytes, read);
        }
        finally {
            mem.release(addr, size);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testByte() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(64);

        int size = 32;

        long addr = mem.allocate(size);

        try {
            byte b1 = 123;

            mem.writeByte(addr, b1);

            byte b2 = mem.readByte(addr);

            assertEquals(b1, b2);

            byte b3 = 11;

            mem.writeByteVolatile(addr, b3);

            assertEquals(b3, mem.readByteVolatile(addr));
        }
        finally {
            mem.release(addr, size);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testShort() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(64);

        int size = 16;

        long addr = mem.allocate(size);

        try {
            short s1 = (short)56777;

            mem.writeShort(addr, s1);

            assertEquals(s1, mem.readShort(addr));
        }
        finally {
            mem.release(addr, size);
        }
    }

    /**
     *
     */
    public void testFloat() {
        GridUnsafeMemory mem = new GridUnsafeMemory(64);

        int size = 32;

        long addr = mem.allocate(size);

        try {
            float f1 = 0.23223f;

            mem.writeFloat(addr, f1);

            assertEquals(f1, mem.readFloat(addr));
        }
        finally {
            mem.release(addr, size);
        }
    }

    /**
     *
     */
    public void testDouble() {
        GridUnsafeMemory mem = new GridUnsafeMemory(64);

        int size = 32;

        long addr = mem.allocate(size);

        try {
            double d1 = 0.2323423;

            mem.writeDouble(addr, d1);

            assertEquals(d1, mem.readDouble(addr));
        }
        finally {
            mem.release(addr, size);
        }
    }


    /**
     * @throws Exception If failed.
     */
    public void testInt() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(64);

        int size = 32;

        long addr = mem.allocate(size);

        try {
            int i1 = 123;

            mem.writeInt(addr, i1);

            int i2 = mem.readInt(addr);

            assertEquals(i1, i2);

            int i3 = 321;

            mem.writeIntVolatile(addr, i3);

            int i4 = 222;

            assertTrue(mem.casInt(addr, i3, i4));
            assertFalse(mem.casInt(addr, i3, 0));

            assertEquals(i4, mem.readIntVolatile(addr));

        }
        finally {
            mem.release(addr, size);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLong() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(64);

        int size = 32;

        long addr = mem.allocate(size);

        try {
            long l1 = 123456;

            mem.writeLong(addr, l1);

            long l2 = mem.readLong(addr);

            assertEquals(l1, l2);

            long l3 = 654321;

            mem.writeLongVolatile(addr, l3);

            long l4 = 666666;

            assertTrue(mem.casLong(addr, l3, l4));
            assertFalse(mem.casLong(addr, l3, 0));

            assertEquals(l4, mem.readLongVolatile(addr));
        }
        finally {
            mem.release(addr, size);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGuardedOpsSimple() throws Exception {
        final GridUnsafeGuard guard = new GridUnsafeGuard();

        final AtomicInteger i = new AtomicInteger();

        guard.begin();

        guard.finalizeLater(new Runnable() {
            @Override
            public void run() {
                i.incrementAndGet();
            }
        });

        guard.begin();
        assertEquals(0, i.get());
        guard.end();

        assertEquals(0, i.get());
        guard.end();

        X.println("__ " + guard);

        assertEquals(1, i.get());
    }

    /**
     * @throws Exception if failed.
     */
    public void testGuardedOps() throws Exception {
        final int lineSize = 16;
        final int ptrsCnt = 4;

        final AtomicReferenceArray<CmpMem> ptrs = new AtomicReferenceArray<>(ptrsCnt * lineSize);

        final AtomicBoolean finished = new AtomicBoolean();

        final LongAdder8 cntr = new LongAdder8();

        final GridUnsafeGuard guard = new GridUnsafeGuard();

        GridRandom rnd = new GridRandom();

        for (int a = 0; a < 7; a++) {
            finished.set(false);

            int threads = 2 + rnd.nextInt(37);
            int time = rnd.nextInt(5);

            X.println("__ starting threads: " + threads + " time: " + time + " sec");

            final LongAdder8 locAdder = new LongAdder8();

            IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Random rnd = new GridRandom();

                    while (!finished.get()) {
                        int idx = rnd.nextInt(ptrsCnt) * lineSize;

                        guard.begin();

                        try {
                            final CmpMem old;

                            CmpMem ptr = null;

                            switch (rnd.nextInt(6)) {
                                case 0:
                                    ptr = new CmpMem(cntr);

                                    //noinspection fallthrough
                                case 1:
                                    old = ptrs.getAndSet(idx, ptr);

                                    if (old != null) {
                                        guard.finalizeLater(new Runnable() {
                                            @Override public void run() {
                                                old.deallocate();
                                            }
                                        });
                                    }

                                    break;

                                case 2:
                                    if (rnd.nextBoolean())
                                        ptr = new CmpMem(cntr);

                                    old = ptrs.getAndSet(idx, ptr);

                                    if (old != null)
                                        guard.releaseLater(old);

                                    break;

                                default:
                                    old = ptrs.get(idx);

                                    if (old != null)
                                        old.touch();
                            }
                        }
                        finally {
                            guard.end();

                            locAdder.increment();
                        }
                    }

                    return null;
                }
            }, threads);

            Thread.sleep(1000 * time);

            X.println("__ stopping ops...");

            finished.set(true);

            fut.get();

            X.println("__ stopped, performed ops: " + locAdder.sum());

            for (int i = 0; i < ptrs.length(); i++) {
                CmpMem ptr = ptrs.getAndSet(i, null);

                if (ptr != null) {
                    ptr.touch();

                    ptr.deallocate();
                }
            }

            X.println("__ " + guard);

            assertEquals(0, cntr.sum());
        }
    }

    private static class CmpMem extends AtomicInteger implements GridUnsafeCompoundMemory {
        /** */
        private AtomicBoolean deallocated = new AtomicBoolean();

        /** */
        private LongAdder8 cntr;

        /**
         * @param cntr Counter.
         */
        CmpMem(LongAdder8 cntr) {
            this.cntr = cntr;

            cntr.increment();
        }

        public void touch() {
            assert !deallocated.get();
        }

        @Override public void deallocate() {
            boolean res = deallocated.compareAndSet(false, true);

            assert res;

            cntr.add(-get() - 1); // Merged plus this instance.
        }

        @Override public void merge(GridUnsafeCompoundMemory compound) {
            touch();

            CmpMem c = (CmpMem)compound;

            c.touch();

            assert c.get() == 0;

            incrementAndGet();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompare1() throws Exception {
        checkCompare("123");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompare2() throws Exception {
        checkCompare("1234567890");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompare3() throws Exception {
        checkCompare("12345678901234567890");
    }

    /**
     * @param s String.
     * @throws Exception If failed.
     */
    public void checkCompare(String s) throws Exception {
        byte[] bytes = s.getBytes();

        int size = bytes.length + 8;

        GridUnsafeMemory mem = new GridUnsafeMemory(size);

        for (int i = 0; i < 8; i++) {
            long addr = mem.allocate(size);

            long ptr = addr + i;

            try {
                mem.writeBytes(ptr, bytes);

                assert mem.compare(ptr, bytes);

                byte[] read = mem.readBytes(ptr, bytes.length);

                assert Arrays.equals(bytes, read);
            }
            finally {
                mem.release(addr, size);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOutOfMemory() throws Exception {
        int cap = 64;
        int block = 9;

        int allowed = cap / block;

        Collection<Long> addrs = new ArrayList<>(allowed);

        GridUnsafeMemory mem = new GridUnsafeMemory(cap);

        try {
            boolean oom = false;

            for (int i = 0; i <= allowed; i++) {
                boolean reserved = mem.reserve(block);

                long addr = mem.allocate(block, true, true);

                addrs.add(addr);

                if (!reserved) {
                    assertEquals(i, allowed);

                    oom = true;

                    break;
                }
            }

            assertTrue(oom);
        }
        finally {
            for (Long addr : addrs)
                mem.release(addr, block);
        }

        assertEquals(mem.allocatedSize(), 0);
    }
}
