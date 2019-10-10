/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.StreamStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * Test the stream store.
 */
public class TestStreamStore extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws IOException {
        testMaxBlockKey();
        testIOException();
        testSaveCount();
        testExceptionDuringStore();
        testReadCount();
        testLarge();
        testDetectIllegalId();
        testTreeStructure();
        testFormat();
        testWithExistingData();
        testWithFullMap();
        testLoop();
    }

    private void testMaxBlockKey() throws IOException {
        TreeMap<Long, byte[]> map = new TreeMap<>();
        StreamStore s = new StreamStore(map);
        s.setMaxBlockSize(128);
        s.setMinBlockSize(64);
        map.clear();
        for (int len = 1; len < 1024 * 1024; len *= 2) {
            byte[] id = s.put(new ByteArrayInputStream(new byte[len]));
            long max = s.getMaxBlockKey(id);
            if (max == -1) {
                assertTrue(map.isEmpty());
            } else {
                assertEquals(map.lastKey(), (Long) max);
            }
        }
    }

    private void testIOException() throws IOException {
        HashMap<Long, byte[]> map = new HashMap<>();
        StreamStore s = new StreamStore(map);
        byte[] id = s.put(new ByteArrayInputStream(new byte[1024 * 1024]));
        InputStream in = s.get(id);
        map.clear();
        try {
            while (true) {
                if (in.read() < 0) {
                    break;
                }
            }
            fail();
        } catch (IOException e) {
            assertEquals(DataUtils.ERROR_BLOCK_NOT_FOUND,
                    DataUtils.getErrorCode(e.getMessage()));
        }
    }

    private void testSaveCount() throws IOException {
        String fileName = getBaseDir() + "/testSaveCount.h3";
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().
                fileName(fileName).
                open();
        MVMap<Long, byte[]> map = s.openMap("data");
        StreamStore streamStore = new StreamStore(map);
        int blockSize = 256 * 1024;
        assertEquals(blockSize, streamStore.getMaxBlockSize());
        for (int i = 0; i < 8 * 16; i++) {
            streamStore.put(new RandomStream(blockSize, i));
        }
        long writeCount = s.getFileStore().getWriteCount();
        assertTrue(writeCount > 2);
        s.close();
    }

    private void testExceptionDuringStore() throws IOException {
        // test that if there is an IOException while storing
        // the data, the entries in the map are "rolled back"
        HashMap<Long, byte[]> map = new HashMap<>();
        StreamStore s = new StreamStore(map);
        s.setMaxBlockSize(1024);
        assertThrows(IOException.class, s).
            put(createFailingStream(new IOException()));
        assertEquals(0, map.size());
        // the runtime exception is converted to an IOException
        assertThrows(IOException.class, s).
            put(createFailingStream(new IllegalStateException()));
        assertEquals(0, map.size());
    }

    private void testReadCount() throws IOException {
        String fileName = getBaseDir() + "/testReadCount.h3";
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().
                fileName(fileName).
                open();
        s.setCacheSize(1);
        StreamStore streamStore = getAutoCommitStreamStore(s);
        long size = s.getPageSplitSize() * 2;
        for (int i = 0; i < 100; i++) {
            streamStore.put(new RandomStream(size, i));
        }
        s.commit();
        MVMap<Long, byte[]> map = s.openMap("data");
        assertTrue("size: " + map.size(), map.sizeAsLong() >= 100);
        s.close();

        s = new MVStore.Builder().
                fileName(fileName).
                open();
        streamStore = getAutoCommitStreamStore(s);
        for (int i = 0; i < 100; i++) {
            streamStore.put(new RandomStream(size, -i));
        }
        s.commit();
        long readCount = s.getFileStore().getReadCount();
        // the read count should be low because new blocks
        // are appended at the end (not between existing blocks)
        assertTrue("rc: " + readCount, readCount < 15);
        map = s.openMap("data");
        assertTrue("size: " + map.size(), map.sizeAsLong() >= 200);
        s.close();
    }

    private static StreamStore getAutoCommitStreamStore(final MVStore s) {
        MVMap<Long, byte[]> map = s.openMap("data");
        return new StreamStore(map) {
            @Override
            protected void onStore(int len) {
                if (s.getUnsavedMemory() > s.getAutoCommitMemory() / 2) {
                    s.commit();
                }
            }
        };
    }

    private void testLarge() throws IOException {
        String fileName = getBaseDir() + "/testVeryLarge.h3";
        FileUtils.delete(fileName);
        final MVStore s = new MVStore.Builder().
                fileName(fileName).
                open();
        MVMap<Long, byte[]> map = s.openMap("data");
        final AtomicInteger count = new AtomicInteger();
        StreamStore streamStore = new StreamStore(map) {
            @Override
            protected void onStore(int len) {
                count.incrementAndGet();
                s.commit();
            }
        };
        long size = 1 * 1024 * 1024;
        streamStore.put(new RandomStream(size, 0));
        s.close();
        assertEquals(4, count.get());
    }

    /**
     * A stream of incompressible data.
     */
    static class RandomStream extends InputStream {

        private long pos, size;
        private int seed;

        RandomStream(long size, int seed) {
            this.size = size;
            this.seed = seed;
        }

        @Override
        public int read() {
            byte[] data = new byte[1];
            int len = read(data, 0, 1);
            return len <= 0 ? len : data[0] & 255;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= size) {
                return -1;
            }
            len = (int) Math.min(size - pos, len);
            int x = seed, end = off + len;
            // a fast and very simple pseudo-random number generator
            // with a period length of 4 GB
            // also good: x * 9 + 1, shift 6; x * 11 + 1, shift 7
            while (off < end) {
                x = (x << 4) + x + 1;
                b[off++] = (byte) (x >> 8);
            }
            seed = x;
            pos += len;
            return len;
        }

    }

    private void testDetectIllegalId() throws IOException {
        Map<Long, byte[]> map = new HashMap<>();
        StreamStore store = new StreamStore(map);
        try {
            store.length(new byte[]{3, 0, 0});
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            store.remove(new byte[]{3, 0, 0});
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        map.put(0L, new byte[]{3, 0, 0});
        InputStream in = store.get(new byte[]{2, 1, 0});
        try {
            in.read();
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private void testTreeStructure() throws IOException {

        final AtomicInteger reads = new AtomicInteger();
        Map<Long, byte[]> map = new HashMap<Long, byte[]>() {

            private static final long serialVersionUID = 1L;

            @Override
            public byte[] get(Object k) {
                reads.incrementAndGet();
                return super.get(k);
            }

        };

        StreamStore store = new StreamStore(map);
        store.setMinBlockSize(10);
        store.setMaxBlockSize(100);
        byte[] id = store.put(new ByteArrayInputStream(new byte[10000]));
        InputStream in = store.get(id);
        assertEquals(0, in.read(new byte[0]));
        assertEquals(0, in.read());
        assertEquals(3, reads.get());
    }

    private void testFormat() throws IOException {
        Map<Long, byte[]> map = new HashMap<>();
        StreamStore store = new StreamStore(map);
        store.setMinBlockSize(10);
        store.setMaxBlockSize(20);
        store.setNextKey(123);

        byte[] id;

        id = store.put(new ByteArrayInputStream(new byte[200]));
        assertEquals(200, store.length(id));
        assertEquals("02c8018801", StringUtils.convertBytesToHex(id));

        id = store.put(new ByteArrayInputStream(new byte[0]));
        assertEquals("", StringUtils.convertBytesToHex(id));

        id = store.put(new ByteArrayInputStream(new byte[1]));
        assertEquals("000100", StringUtils.convertBytesToHex(id));

        id = store.put(new ByteArrayInputStream(new byte[3]));
        assertEquals("0003000000", StringUtils.convertBytesToHex(id));

        id = store.put(new ByteArrayInputStream(new byte[10]));
        assertEquals("010a8901", StringUtils.convertBytesToHex(id));

        byte[] combined = StringUtils.convertHexToBytes("0001aa0002bbcc");
        assertEquals(3, store.length(combined));
        InputStream in = store.get(combined);
        assertEquals(1, in.skip(1));
        assertEquals(0xbb, in.read());
        assertEquals(1, in.skip(1));
    }

    private void testWithExistingData() throws IOException {

        final AtomicInteger tests = new AtomicInteger();
        Map<Long, byte[]> map = new HashMap<Long, byte[]>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean containsKey(Object k) {
                tests.incrementAndGet();
                return super.containsKey(k);
            }

        };
        StreamStore store = new StreamStore(map);
        store.setMinBlockSize(10);
        store.setMaxBlockSize(20);
        store.setNextKey(0);
        for (int i = 0; i < 10; i++) {
            store.put(new ByteArrayInputStream(new byte[20]));
        }
        assertEquals(10, map.size());
        assertEquals(10, tests.get());
        for (int i = 0; i < 10; i++) {
            map.containsKey((long)i);
        }
        assertEquals(20, tests.get());
        store = new StreamStore(map);
        store.setMinBlockSize(10);
        store.setMaxBlockSize(20);
        store.setNextKey(0);
        assertEquals(0, store.getNextKey());
        for (int i = 0; i < 5; i++) {
            store.put(new ByteArrayInputStream(new byte[20]));
        }
        assertEquals(88, tests.get());
        assertEquals(15, store.getNextKey());
        assertEquals(15, map.size());
        for (int i = 0; i < 15; i++) {
            map.containsKey((long)i);
        }
    }

    private void testWithFullMap() throws IOException {
        final AtomicInteger tests = new AtomicInteger();
        Map<Long, byte[]> map = new HashMap<Long, byte[]>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean containsKey(Object k) {
                tests.incrementAndGet();
                if (((Long) k) < Long.MAX_VALUE / 2) {
                    // simulate a *very* full map
                    return true;
                }
                return super.containsKey(k);
            }

        };
        StreamStore store = new StreamStore(map);
        store.setMinBlockSize(20);
        store.setMaxBlockSize(100);
        store.setNextKey(0);
        store.put(new ByteArrayInputStream(new byte[100]));
        assertEquals(1, map.size());
        assertEquals(64, tests.get());
        assertEquals(Long.MAX_VALUE / 2 + 1, store.getNextKey());
    }

    private void testLoop() throws IOException {
        Map<Long, byte[]> map = new HashMap<>();
        StreamStore store = new StreamStore(map);
        assertEquals(256 * 1024, store.getMaxBlockSize());
        assertEquals(256, store.getMinBlockSize());
        store.setNextKey(0);
        assertEquals(0, store.getNextKey());
        test(store, 10, 20, 1000);
        for (int i = 0; i < 20; i++) {
            test(store, 0, 128, i);
            test(store, 10, 128, i);
        }
        for (int i = 20; i < 200; i += 10) {
            test(store, 0, 128, i);
            test(store, 10, 128, i);
        }
    }

    private void test(StreamStore store, int minBlockSize, int maxBlockSize,
            int length) throws IOException {
        store.setMinBlockSize(minBlockSize);
        assertEquals(minBlockSize, store.getMinBlockSize());
        store.setMaxBlockSize(maxBlockSize);
        assertEquals(maxBlockSize, store.getMaxBlockSize());
        long next = store.getNextKey();
        Random r = new Random(length);
        byte[] data = new byte[length];
        r.nextBytes(data);
        byte[] id = store.put(new ByteArrayInputStream(data));
        if (length > 0 && length >= minBlockSize) {
            assertFalse(store.isInPlace(id));
        } else {
            assertTrue(store.isInPlace(id));
        }
        long next2 = store.getNextKey();
        if (length > 0 && length >= minBlockSize) {
            assertTrue(next2 > next);
        } else {
            assertEquals(next, next2);
        }
        if (length == 0) {
            assertEquals(0, id.length);
        }

        assertEquals(length, store.length(id));

        InputStream in = store.get(id);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
        assertTrue(Arrays.equals(data, out.toByteArray()));

        in = store.get(id);
        in.close();
        assertEquals(-1, in.read());

        in = store.get(id);
        assertEquals(0, in.skip(0));
        if (length > 0) {
            assertEquals(1, in.skip(1));
            if (length > 1) {
                assertEquals(data[1] & 255, in.read());
                if (length > 2) {
                    assertEquals(1, in.skip(1));
                    if (length > 3) {
                        assertEquals(data[3] & 255, in.read());
                    }
                } else {
                    assertEquals(0, in.skip(1));
                }
            } else {
                assertEquals(-1, in.read());
            }
        } else {
            assertEquals(0, in.skip(1));
        }

        if (length > 12) {
            in = store.get(id);
            assertEquals(12, in.skip(12));
            assertEquals(data[12] & 255, in.read());
            long skipped = 0;
            while (true) {
                long s = in.skip(Integer.MAX_VALUE);
                if (s == 0) {
                    break;
                }
                skipped += s;
            }
            assertEquals(length - 13, skipped);
            assertEquals(-1, in.read());
        }

        store.remove(id);
        assertEquals(0, store.getMap().size());
    }

}
