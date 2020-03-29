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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests unsafe memory.
 */
public class GridUnsafeMemorySelfTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testBuffers() {
        ByteBuffer b1 = GridUnsafe.allocateBuffer(10);
        ByteBuffer b2 = GridUnsafe.allocateBuffer(20);

        assertEquals(GridUnsafe.NATIVE_BYTE_ORDER, b2.order());
        assertTrue(b2.isDirect());
        assertEquals(20, b2.capacity());
        assertEquals(20, b2.limit());
        assertEquals(0, b2.position());

        assertEquals(GridUnsafe.NATIVE_BYTE_ORDER, b1.order());
        assertTrue(b1.isDirect());
        assertEquals(10, b1.capacity());
        assertEquals(10, b1.limit());
        assertEquals(0, b1.position());

        b1.putLong(1L);
        b1.putShort((short)7);

        b2.putLong(2L);

        GridUnsafe.freeBuffer(b1);

        b2.putLong(3L);
        b2.putInt(9);

        for (int i = 0; i <= 16; i++)
            b2.putInt(i, 100500);

        GridUnsafe.freeBuffer(b2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
    public void testCompare1() throws Exception {
        checkCompare("123");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCompare2() throws Exception {
        checkCompare("1234567890");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
