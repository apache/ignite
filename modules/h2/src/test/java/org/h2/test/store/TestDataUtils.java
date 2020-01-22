/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import org.h2.mvstore.Chunk;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.test.TestBase;

/**
 * Test utility classes.
 */
public class TestDataUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() {
        testParse();
        testWriteBuffer();
        testEncodeLength();
        testFletcher();
        testMap();
        testMapRandomized();
        testMaxShortVarIntVarLong();
        testVarIntVarLong();
        testCheckValue();
        testPagePos();
    }

    private static void testWriteBuffer() {
        WriteBuffer buff = new WriteBuffer();
        buff.put(new byte[1500000]);
        buff.put(new byte[1900000]);
    }

    private void testFletcher() {
        byte[] data = new byte[10000];
        for (int i = 0; i < 10000; i += 1000) {
            assertEquals(-1, DataUtils.getFletcher32(data, 0, i));
        }
        Arrays.fill(data, (byte) 255);
        for (int i = 0; i < 10000; i += 1000) {
            assertEquals(-1, DataUtils.getFletcher32(data, 0, i));
        }
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 255; j++) {
                Arrays.fill(data, 0, i, (byte) j);
                data[i] = 0;
                int a = DataUtils.getFletcher32(data, 0, i);
                if (i % 2 == 1) {
                    // add length: same as appending a 0
                    int b = DataUtils.getFletcher32(data, 0, i + 1);
                    assertEquals(a, b);
                }
                data[i] = 10;
                int c = DataUtils.getFletcher32(data, 0, i);
                assertEquals(a, c);
            }
        }
        long last = 0;
        for (int i = 1; i < 255; i++) {
            Arrays.fill(data, (byte) i);
            for (int j = 0; j < 10; j += 2) {
                int x = DataUtils.getFletcher32(data, 0, j);
                assertTrue(x != last);
                last = x;
            }
        }
        Arrays.fill(data, (byte) 10);
        assertEquals(0x1e1e1414,
                DataUtils.getFletcher32(data, 0, 10000));
        assertEquals(0x1e3fa7ed,
                DataUtils.getFletcher32("Fletcher32".getBytes(), 0, 10));
        assertEquals(0x1e3fa7ed,
                DataUtils.getFletcher32("XFletcher32".getBytes(), 1, 10));
    }

    private void testMap() {
        StringBuilder buff = new StringBuilder();
        DataUtils.appendMap(buff,  "", "");
        DataUtils.appendMap(buff,  "a", "1");
        DataUtils.appendMap(buff,  "b", ",");
        DataUtils.appendMap(buff,  "c", "1,2");
        DataUtils.appendMap(buff,  "d", "\"test\"");
        DataUtils.appendMap(buff,  "e", "}");
        DataUtils.appendMap(buff,  "name", "1:1\",");
        String encoded = buff.toString();
        assertEquals(":,a:1,b:\",\",c:\"1,2\",d:\"\\\"test\\\"\",e:},name:\"1:1\\\",\"", encoded);

        HashMap<String, String> m = DataUtils.parseMap(encoded);
        assertEquals(7, m.size());
        assertEquals("", m.get(""));
        assertEquals("1", m.get("a"));
        assertEquals(",", m.get("b"));
        assertEquals("1,2", m.get("c"));
        assertEquals("\"test\"", m.get("d"));
        assertEquals("}", m.get("e"));
        assertEquals("1:1\",", m.get("name"));
        assertEquals("1:1\",", DataUtils.getMapName(encoded));

        buff.setLength(0);
        DataUtils.appendMap(buff,  "1", "1");
        DataUtils.appendMap(buff,  "name", "2");
        DataUtils.appendMap(buff,  "3", "3");
        encoded = buff.toString();
        assertEquals("2", DataUtils.parseMap(encoded).get("name"));
        assertEquals("2", DataUtils.getMapName(encoded));

        buff.setLength(0);
        DataUtils.appendMap(buff,  "name", "xx");
        encoded = buff.toString();
        assertEquals("xx", DataUtils.parseMap(encoded).get("name"));
        assertEquals("xx", DataUtils.getMapName(encoded));
    }

    private void testMapRandomized() {
        Random r = new Random(1);
        String chars = "a_1,\\\":";
        for (int i = 0; i < 1000; i++) {
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < 20; j++) {
                buff.append(chars.charAt(r.nextInt(chars.length())));
            }
            try {
                HashMap<String, String> map = DataUtils.parseMap(buff.toString());
                assertFalse(map == null);
                // ok
            } catch (IllegalStateException e) {
                // ok - but not another exception
            }
        }
    }

    private void testMaxShortVarIntVarLong() {
        ByteBuffer buff = ByteBuffer.allocate(100);
        DataUtils.writeVarInt(buff, DataUtils.COMPRESSED_VAR_INT_MAX);
        assertEquals(3, buff.position());
        buff.rewind();
        DataUtils.writeVarInt(buff, DataUtils.COMPRESSED_VAR_INT_MAX + 1);
        assertEquals(4, buff.position());
        buff.rewind();
        DataUtils.writeVarLong(buff, DataUtils.COMPRESSED_VAR_LONG_MAX);
        assertEquals(7, buff.position());
        buff.rewind();
        DataUtils.writeVarLong(buff, DataUtils.COMPRESSED_VAR_LONG_MAX + 1);
        assertEquals(8, buff.position());
        buff.rewind();
    }

    private void testVarIntVarLong() {
        ByteBuffer buff = ByteBuffer.allocate(100);
        for (long x = 0; x < 1000; x++) {
            testVarIntVarLong(buff, x);
            testVarIntVarLong(buff, -x);
        }
        for (long x = Long.MIN_VALUE, i = 0; i < 1000; x++, i++) {
            testVarIntVarLong(buff, x);
        }
        for (long x = Long.MAX_VALUE, i = 0; i < 1000; x--, i++) {
            testVarIntVarLong(buff, x);
        }
        for (int shift = 0; shift < 64; shift++) {
            for (long x = 250; x < 260; x++) {
                testVarIntVarLong(buff, x << shift);
                testVarIntVarLong(buff, -(x << shift));
            }
        }
        // invalid varInt / varLong
        // should work, but not read far too much
        for (int i = 0; i < 50; i++) {
            buff.put((byte) 255);
        }
        buff.flip();
        assertEquals(-1, DataUtils.readVarInt(buff));
        assertEquals(5, buff.position());
        buff.rewind();
        assertEquals(-1, DataUtils.readVarLong(buff));
        assertEquals(10, buff.position());

        buff.clear();
        testVarIntVarLong(buff, DataUtils.COMPRESSED_VAR_INT_MAX);
        testVarIntVarLong(buff, DataUtils.COMPRESSED_VAR_INT_MAX + 1);
        testVarIntVarLong(buff, DataUtils.COMPRESSED_VAR_LONG_MAX);
        testVarIntVarLong(buff, DataUtils.COMPRESSED_VAR_LONG_MAX + 1);
    }

    private void testVarIntVarLong(ByteBuffer buff, long x) {
        int len;
        byte[] data;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataUtils.writeVarLong(out, x);
            data = out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataUtils.writeVarLong(buff, x);
        len = buff.position();
        assertEquals(data.length, len);
        byte[] data2 = new byte[len];
        buff.position(0);
        buff.get(data2);
        assertEquals(data2, data);
        buff.flip();
        long y = DataUtils.readVarLong(buff);
        assertEquals(y, x);
        assertEquals(len, buff.position());
        assertEquals(len, DataUtils.getVarLongLen(x));
        buff.clear();

        int intX = (int) x;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataUtils.writeVarInt(out, intX);
            data = out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataUtils.writeVarInt(buff, intX);
        len = buff.position();
        assertEquals(data.length, len);
        data2 = new byte[len];
        buff.position(0);
        buff.get(data2);
        assertEquals(data2, data);
        buff.flip();
        int intY = DataUtils.readVarInt(buff);
        assertEquals(intY, intX);
        assertEquals(len, buff.position());
        assertEquals(len, DataUtils.getVarIntLen(intX));
        buff.clear();
    }

    private void testCheckValue() {
        // 0 xor 0 = 0
        assertEquals(0, DataUtils.getCheckValue(0));
        // 1111... xor 1111... = 0
        assertEquals(0, DataUtils.getCheckValue(-1));
        // 0 xor 1111... = 1111...
        assertEquals((short) -1, DataUtils.getCheckValue(-1 >>> 16));
        // 1111... xor 0 = 1111...
        assertEquals((short) -1, DataUtils.getCheckValue(-1 << 16));
        // 0 xor 1000... = 1000...
        assertEquals((short) (1 << 15), DataUtils.getCheckValue(1 << 15));
        // 1000... xor 0 = 1000...
        assertEquals((short) (1 << 15), DataUtils.getCheckValue(1 << 31));
    }

    private void testParse() {
        for (long i = -1; i != 0; i >>>= 1) {
            String x = Long.toHexString(i);
            assertEquals(i, DataUtils.parseHexLong(x));
            x = Long.toHexString(-i);
            assertEquals(-i, DataUtils.parseHexLong(x));
            int j = (int) i;
            x = Integer.toHexString(j);
            assertEquals(j, DataUtils.parseHexInt(x));
            j = (int) -i;
            x = Integer.toHexString(j);
            assertEquals(j, DataUtils.parseHexInt(x));
        }
    }

    private void testPagePos() {
        assertEquals(0, DataUtils.PAGE_TYPE_LEAF);
        assertEquals(1, DataUtils.PAGE_TYPE_NODE);

        long max = DataUtils.getPagePos(Chunk.MAX_ID, Integer.MAX_VALUE,
                    Integer.MAX_VALUE, DataUtils.PAGE_TYPE_NODE);
        String hex = Long.toHexString(max);
        assertEquals(max, DataUtils.parseHexLong(hex));
        assertEquals(Chunk.MAX_ID, DataUtils.getPageChunkId(max));
        assertEquals(Integer.MAX_VALUE, DataUtils.getPageOffset(max));
        assertEquals(DataUtils.PAGE_LARGE, DataUtils.getPageMaxLength(max));
        assertEquals(DataUtils.PAGE_TYPE_NODE, DataUtils.getPageType(max));

        long overflow = DataUtils.getPagePos(Chunk.MAX_ID + 1,
                Integer.MAX_VALUE, Integer.MAX_VALUE, DataUtils.PAGE_TYPE_NODE);
        assertTrue(Chunk.MAX_ID + 1 != DataUtils.getPageChunkId(overflow));

        for (int i = 0; i < Chunk.MAX_ID; i++) {
            long pos = DataUtils.getPagePos(i, 3, 128, 1);
            assertEquals(i, DataUtils.getPageChunkId(pos));
            assertEquals(3, DataUtils.getPageOffset(pos));
            assertEquals(128, DataUtils.getPageMaxLength(pos));
            assertEquals(1, DataUtils.getPageType(pos));
        }
        for (int type = 0; type <= 1; type++) {
            for (int chunkId = 0; chunkId < Chunk.MAX_ID;
                    chunkId += Chunk.MAX_ID / 100) {
                for (long offset = 0; offset < Integer.MAX_VALUE;
                        offset += Integer.MAX_VALUE / 100) {
                    for (int length = 0; length < 2000000; length += 200000) {
                        long pos = DataUtils.getPagePos(
                                chunkId, (int) offset, length, type);
                        assertEquals(chunkId, DataUtils.getPageChunkId(pos));
                        assertEquals(offset, DataUtils.getPageOffset(pos));
                        assertTrue(DataUtils.getPageMaxLength(pos) >= length);
                        assertTrue(DataUtils.getPageType(pos) == type);
                    }
                }
            }
        }
    }

    private void testEncodeLength() {
        int lastCode = 0;
        assertEquals(0, DataUtils.encodeLength(32));
        assertEquals(1, DataUtils.encodeLength(33));
        assertEquals(1, DataUtils.encodeLength(48));
        assertEquals(2, DataUtils.encodeLength(49));
        assertEquals(2, DataUtils.encodeLength(64));
        assertEquals(3, DataUtils.encodeLength(65));
        assertEquals(30, DataUtils.encodeLength(1024 * 1024));
        assertEquals(31, DataUtils.encodeLength(1024 * 1024 + 1));
        assertEquals(31, DataUtils.encodeLength(Integer.MAX_VALUE));
        int[] maxLengthForIndex = {32, 48, 64, 96, 128, 192, 256,
                384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144,
                8192, 12288, 16384, 24576, 32768, 49152, 65536,
                98304, 131072, 196608, 262144, 393216, 524288,
                786432, 1048576};
        for (int i = 0; i < maxLengthForIndex.length; i++) {
            assertEquals(i, DataUtils.encodeLength(maxLengthForIndex[i]));
            assertEquals(i + 1, DataUtils.encodeLength(maxLengthForIndex[i] + 1));
        }
        for (int i = 1024 * 1024 + 1; i < 100 * 1024 * 1024; i += 1024) {
            int code = DataUtils.encodeLength(i);
            assertEquals(31, code);
        }
        for (int i = 0; i < 2 * 1024 * 1024; i++) {
            int code = DataUtils.encodeLength(i);
            assertTrue(code <= 31 && code >= 0);
            assertTrue(code >= lastCode);
            if (code > lastCode) {
                lastCode = code;
            }
            int max = DataUtils.getPageMaxLength(code << 1);
            assertTrue(max >= i && max >= 32);
        }
    }

}
