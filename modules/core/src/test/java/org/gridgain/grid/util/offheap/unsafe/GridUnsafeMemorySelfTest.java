/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

import org.gridgain.grid.GridFuture;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

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
     * @throws Exception if failed.
     */
    public void testMultithreadedOps() throws Exception {
        final AtomicLongArray ptrs = new AtomicLongArray(32);

        final AtomicBoolean finished = new AtomicBoolean();

        final GridUnsafeMemory mem = new GridUnsafeMemory(0);

        final GridUnsafeGuard guard = new GridUnsafeGuard();

        GridFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                while (!finished.get()) {
                    int idx = rnd.nextInt(ptrs.length());

                    guard.begin();

                    try {
                        long old;

                        long ptr = 0;

                        switch(rnd.nextInt(15)) {
                            case 0:
                                ptr = mem.allocate(8);

                                mem.writeLong(ptr, -1);

                                //noinspection fallthrough
                            case 1:
                            case 2:
                                old = ptrs.getAndSet(idx, ptr);

                                assert old >= 0 : old;

                                if (old != 0) {
                                    assertEquals(-1, mem.readLong(old));

                                    guard.releaseLater(new CmpMem(old, mem));
                                }

                                break;

                            default:
                                old = ptrs.get(idx);

                                assert old >= 0 : old;

                                if (old != 0)
                                    mem.readLong(old);
                        }
                    }
                    finally {
                        guard.end();
                    }
                }

                return null;
            }
        }, 64);

        Thread.sleep(60000);

        finished.set(true);

        fut.get();

        for (int i = 0; i < ptrs.length(); i++) {
            long ptr = ptrs.get(i);

            assert ptr >= 0 : ptr;

            if (ptr != 0)
                mem.release(ptr, 8);
        }

        assertEquals(0, mem.allocatedSize());
    }

    /**
     * Test compound memory.
     */
    private static class CmpMem extends AtomicLong implements GridUnsafeCompoundMemory {
        /** */
        protected final AtomicLong head = new AtomicLong();

        /** */
        private final GridUnsafeMemory mem;

        /**
         * @param ptr Pointer.
         * @param mem Memory.
         */
        private CmpMem(long ptr, GridUnsafeMemory mem) {
            this.mem = mem;
            head.set(ptr);
        }

        /** {@inheritDoc} */
        @Override public void deallocate() {
            long h = head.get();

            while (h > 0) {
                assert h > 0 : h;

                long prev = h;

                h = mem.readLongVolatile(h);

                mem.release(prev, 8);
            }
        }

        /** {@inheritDoc} */
        @Override public void merge(GridUnsafeCompoundMemory qq) {
            CmpMem q = (CmpMem)qq;

            long node = q.head.get();

            assert node > 0 : node;

            add(node);
        }

        /**
         * @param node Head node pointer.
         * @return {@code true} If succeeded.
         */
        protected boolean add(long node) {
            for (;;) {
                long h = head.get();

                assert h > 0 : h;

                mem.writeLongVolatile(node, h);// If h == 0 we still need to write it to tail.

                if (head.compareAndSet(h, node))
                    return true;
            }
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

