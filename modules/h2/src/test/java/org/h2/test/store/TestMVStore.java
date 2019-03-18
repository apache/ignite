/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.h2.mvstore.Chunk;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.OffHeapStore;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.mvstore.type.StringDataType;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.AssertThrows;

/**
 * Tests the MVStore.
 */
public class TestMVStore extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.config.big = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        testRemoveMapRollback();
        testProvidedFileStoreNotOpenedAndClosed();
        testVolatileMap();
        testEntrySet();
        testCompressEmptyPage();
        testCompressed();
        testFileFormatExample();
        testMaxChunkLength();
        testCacheInfo();
        testRollback();
        testVersionsToKeep();
        testVersionsToKeep2();
        testRemoveMap();
        testIsEmpty();
        testOffHeapStorage();
        testNewerWriteVersion();
        testCompactFully();
        testBackgroundExceptionListener();
        testOldVersion();
        testAtomicOperations();
        testWriteBuffer();
        testWriteDelay();
        testEncryptedFile();
        testFileFormatChange();
        testRecreateMap();
        testRenameMapRollback();
        testCustomMapType();
        testCacheSize();
        testConcurrentOpen();
        testFileHeader();
        testFileHeaderCorruption();
        testIndexSkip();
        testMinMaxNextKey();
        testStoreVersion();
        testIterateOldVersion();
        testObjects();
        testExample();
        testExampleMvcc();
        testOpenStoreCloseLoop();
        testVersion();
        testTruncateFile();
        testFastDelete();
        testRollbackInMemory();
        testRollbackStored();
        testMeta();
        testInMemory();
        testLargeImport();
        testBtreeStore();
        testCompact();
        testCompactMapNotOpen();
        testReuseSpace();
        testRandom();
        testKeyValueClasses();
        testIterate();
        testCloseTwice();
        testSimple();

        // longer running tests
        testLargerThan2G();
    }

    private void testRemoveMapRollback() {
        MVStore store = new MVStore.Builder().
                open();
        MVMap<String, String> map = store.openMap("test");
        map.put("1", "Hello");
        store.commit();
        store.removeMap(map);
        store.rollback();
        assertTrue(store.hasMap("test"));
        map = store.openMap("test");
        // TODO the data should get back alive
        assertNull(map.get("1"));
        store.close();

        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        store = new MVStore.Builder().
                autoCommitDisabled().
                fileName(fileName).
                open();
        map = store.openMap("test");
        map.put("1", "Hello");
        store.commit();
        store.removeMap(map);
        store.rollback();
        assertTrue(store.hasMap("test"));
        map = store.openMap("test");
        // the data will get back alive
        assertEquals("Hello", map.get("1"));
        store.close();
    }

    private void testProvidedFileStoreNotOpenedAndClosed() {
        final AtomicInteger openClose = new AtomicInteger();
        FileStore fileStore = new OffHeapStore() {

            @Override
            public void open(String fileName, boolean readOnly, char[] encryptionKey) {
                openClose.incrementAndGet();
                super.open(fileName, readOnly, encryptionKey);
            }

            @Override
            public void close() {
                openClose.incrementAndGet();
                super.close();
            }
        };
        MVStore store = new MVStore.Builder().
                fileStore(fileStore).
                open();
        store.close();
        assertEquals(0, openClose.get());
    }

    private void testVolatileMap() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore store = new MVStore.Builder().
                fileName(fileName).
                open();
        MVMap<String, String> map = store.openMap("test");
        assertFalse(map.isVolatile());
        map.setVolatile(true);
        assertTrue(map.isVolatile());
        map.put("1", "Hello");
        assertEquals("Hello", map.get("1"));
        assertEquals(1, map.size());
        store.close();
        store = new MVStore.Builder().
                fileName(fileName).
                open();
        assertTrue(store.hasMap("test"));
        map = store.openMap("test");
        assertEquals(0, map.size());
        store.close();
    }

    private void testEntrySet() {
        MVStore s = new MVStore.Builder().open();
        MVMap<Integer, Integer> map;
        map = s.openMap("data");
        for (int i = 0; i < 20; i++) {
            map.put(i, i * 10);
        }
        int next = 0;
        for (Entry<Integer, Integer> e : map.entrySet()) {
            assertEquals(next, e.getKey().intValue());
            assertEquals(next * 10, e.getValue().intValue());
            next++;
        }
    }

    private void testCompressEmptyPage() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore store = new MVStore.Builder().
                cacheSize(100).fileName(fileName).
                compress().
                autoCommitBufferSize(10 * 1024).
                open();
        MVMap<String, String> map = store.openMap("test");
        store.removeMap(map);
        store.commit();
        store.close();
        store = new MVStore.Builder().
                compress().
                open();
        store.close();
    }

    private void testCompressed() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        long lastSize = 0;
        for (int level = 0; level <= 2; level++) {
            FileUtils.delete(fileName);
            MVStore.Builder builder = new MVStore.Builder().fileName(fileName);
            if (level == 1) {
                builder.compress();
            } else if (level == 2) {
                builder.compressHigh();
            }
            MVStore s = builder.open();
            MVMap<String, String> map = s.openMap("data");
            String data = new String(new char[1000]).replace((char) 0, 'x');
            for (int i = 0; i < 400; i++) {
                map.put(data + i, data);
            }
            s.close();
            long size = FileUtils.size(fileName);
            if (level > 0) {
                assertTrue(size < lastSize);
            }
            lastSize = size;
            s = new MVStore.Builder().fileName(fileName).open();
            map = s.openMap("data");
            for (int i = 0; i < 400; i++) {
                assertEquals(data, map.get(data + i));
            }
            s.close();
        }
    }

    private void testFileFormatExample() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = MVStore.open(fileName);
        MVMap<Integer, String> map = s.openMap("data");
        for (int i = 0; i < 400; i++) {
            map.put(i, "Hello");
        }
        s.commit();
        for (int i = 0; i < 100; i++) {
            map.put(0, "Hi");
        }
        s.commit();
        s.close();
        // ;MVStoreTool.dump(fileName);
    }

    private void testMaxChunkLength() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().fileName(fileName).open();
        MVMap<Integer, byte[]> map = s.openMap("data");
        map.put(0, new byte[2 * 1024 * 1024]);
        s.commit();
        map.put(1, new byte[10 * 1024]);
        s.commit();
        MVMap<String, String> meta = s.getMetaMap();
        Chunk c = Chunk.fromString(meta.get("chunk.1"));
        assertTrue(c.maxLen < Integer.MAX_VALUE);
        assertTrue(c.maxLenLive < Integer.MAX_VALUE);
        s.close();
    }

    private void testCacheInfo() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().fileName(fileName).cacheSize(2).open();
        assertEquals(2, s.getCacheSize());
        MVMap<Integer, byte[]> map;
        map = s.openMap("data");
        byte[] data = new byte[1024];
        for (int i = 0; i < 1000; i++) {
            map.put(i, data);
            s.commit();
            if (i < 50) {
                assertEquals(0, s.getCacheSizeUsed());
            } else if (i > 300) {
                assertTrue(s.getCacheSizeUsed() >= 1);
            }
        }
        s.close();
        s = new MVStore.Builder().open();
        assertEquals(0, s.getCacheSize());
        assertEquals(0, s.getCacheSizeUsed());
        s.close();
    }

    private void testVersionsToKeep() throws Exception {
        MVStore s = new MVStore.Builder().open();
        MVMap<Integer, Integer> map;
        map = s.openMap("data");
        for (int i = 0; i < 20; i++) {
            long version = s.getCurrentVersion();
            map.put(i, i);
            s.commit();
            if (version >= 6) {
                map.openVersion(version - 5);
                try {
                    map.openVersion(version - 6);
                    fail();
                } catch (IllegalArgumentException e) {
                    // expected
                }
            }
        }
    }

    private void testVersionsToKeep2() {
        MVStore s = new MVStore.Builder().autoCommitDisabled().open();
        s.setVersionsToKeep(2);
        final MVMap<Integer, String> m = s.openMap("data");
        s.commit();
        assertEquals(1, s.getCurrentVersion());
        m.put(1, "version 1");
        s.commit();
        assertEquals(2, s.getCurrentVersion());
        m.put(1, "version 2");
        s.commit();
        assertEquals(3, s.getCurrentVersion());
        m.put(1, "version 3");
        s.commit();
        m.put(1, "version 4");
        assertEquals("version 4", m.openVersion(4).get(1));
        assertEquals("version 3", m.openVersion(3).get(1));
        assertEquals("version 2", m.openVersion(2).get(1));
        new AssertThrows(IllegalArgumentException.class) {
            @Override
            public void test() throws Exception {
                m.openVersion(1);
            }
        };
        s.close();
    }

    private void testRemoveMap() throws Exception {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().
            fileName(fileName).
            open();
        MVMap<Integer, Integer> map;

        map = s.openMap("data");
        map.put(1, 1);
        assertEquals(1, map.get(1).intValue());
        s.commit();

        s.removeMap(map);
        s.commit();

        map = s.openMap("data");
        assertTrue(map.isEmpty());
        map.put(2, 2);

        s.close();
    }

    private void testIsEmpty() throws Exception {
        MVStore s = new MVStore.Builder().
                pageSplitSize(50).
                open();
        Map<Integer, byte[]> m = s.openMap("data");
        m.put(1, new byte[50]);
        m.put(2, new byte[50]);
        m.put(3, new byte[50]);
        m.remove(1);
        m.remove(2);
        m.remove(3);
        assertEquals(0, m.size());
        assertTrue(m.isEmpty());
        s.close();
    }

    private void testOffHeapStorage() throws Exception {
        OffHeapStore offHeap = new OffHeapStore();
        MVStore s = new MVStore.Builder().
                fileStore(offHeap).
                open();
        int count = 1000;
        Map<Integer, String> map = s.openMap("data");
        for (int i = 0; i < count; i++) {
            map.put(i, "Hello " + i);
            s.commit();
        }
        assertTrue(offHeap.getWriteCount() > count);
        s.close();

        s = new MVStore.Builder().
                fileStore(offHeap).
                open();
        map = s.openMap("data");
        for (int i = 0; i < count; i++) {
            assertEquals("Hello " + i, map.get(i));
        }
        s.close();
    }

    private void testNewerWriteVersion() throws Exception {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().
                encryptionKey("007".toCharArray()).
                fileName(fileName).
                open();
        s.setRetentionTime(Integer.MAX_VALUE);
        Map<String, Object> header = s.getStoreHeader();
        assertEquals("1", header.get("format").toString());
        header.put("formatRead", "1");
        header.put("format", "2");
        forceWriteStoreHeader(s);
        MVMap<Integer, String> m = s.openMap("data");
        forceWriteStoreHeader(s);
        m.put(0, "Hello World");
        s.close();
        try {
            s = new MVStore.Builder().
                    encryptionKey("007".toCharArray()).
                    fileName(fileName).
                    open();
            header = s.getStoreHeader();
            fail(header.toString());
        } catch (IllegalStateException e) {
            assertEquals(DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    DataUtils.getErrorCode(e.getMessage()));
        }
        s = new MVStore.Builder().
                encryptionKey("007".toCharArray()).
                readOnly().
                fileName(fileName).
                open();
        assertTrue(s.getFileStore().isReadOnly());
        m = s.openMap("data");
        assertEquals("Hello World", m.get(0));
        s.close();

        FileUtils.setReadOnly(fileName);
        s = new MVStore.Builder().
                encryptionKey("007".toCharArray()).
                fileName(fileName).
                open();
        assertTrue(s.getFileStore().isReadOnly());
        m = s.openMap("data");
        assertEquals("Hello World", m.get(0));
        s.close();

    }

    private void testCompactFully() throws Exception {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().
                fileName(fileName).
                autoCommitDisabled().
                open();
        MVMap<Integer, String> m;
        for (int i = 0; i < 100; i++) {
            m = s.openMap("data" + i);
            m.put(0, "Hello World");
            s.commit();
        }
        for (int i = 0; i < 100; i += 2) {
            m = s.openMap("data" + i);
            s.removeMap(m);
            s.commit();
        }
        long sizeOld = s.getFileStore().size();
        s.compactMoveChunks();
        long sizeNew = s.getFileStore().size();
        assertTrue("old: " + sizeOld + " new: " + sizeNew, sizeNew < sizeOld);
        s.close();
    }

    private void testBackgroundExceptionListener() throws Exception {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        final AtomicReference<Throwable> exRef =
                new AtomicReference<>();
        s = new MVStore.Builder().
                fileName(fileName).
                backgroundExceptionHandler(new UncaughtExceptionHandler() {

                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        exRef.set(e);
                    }

                }).
                open();
        s.setAutoCommitDelay(10);
        MVMap<Integer, String> m;
        m = s.openMap("data");
        s.getFileStore().getFile().close();
        try {
            m.put(1, "Hello");
            for (int i = 0; i < 200; i++) {
                if (exRef.get() != null) {
                    break;
                }
                sleep(10);
            }
            Throwable e = exRef.get();
            assertTrue(e != null);
            assertEquals(DataUtils.ERROR_WRITING_FAILED,
                    DataUtils.getErrorCode(e.getMessage()));
        } catch (IllegalStateException e) {
            // sometimes it is detected right away
            assertEquals(DataUtils.ERROR_CLOSED,
                    DataUtils.getErrorCode(e.getMessage()));
        }

        s.closeImmediately();
        FileUtils.delete(fileName);
    }

    private void testAtomicOperations() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, byte[]> m;
        s = new MVStore.Builder().
                fileName(fileName).
                open();
        m = s.openMap("data");

        // putIfAbsent
        assertNull(m.putIfAbsent(1, new byte[1]));
        assertEquals(1, m.putIfAbsent(1, new byte[2]).length);
        assertEquals(1, m.get(1).length);

        // replace
        assertNull(m.replace(2, new byte[2]));
        assertNull(m.get(2));
        assertEquals(1, m.replace(1, new byte[2]).length);
        assertEquals(2, m.replace(1, new byte[3]).length);
        assertEquals(3, m.replace(1, new byte[1]).length);

        // replace with oldValue
        assertFalse(m.replace(1, new byte[2], new byte[10]));
        assertTrue(m.replace(1, new byte[1], new byte[2]));
        assertTrue(m.replace(1, new byte[2], new byte[1]));

        // remove
        assertFalse(m.remove(1, new byte[2]));
        assertTrue(m.remove(1, new byte[1]));

        s.close();
        FileUtils.delete(fileName);
    }

    private void testWriteBuffer() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, byte[]> m;
        byte[] data = new byte[1000];
        long lastSize = 0;
        int len = 1000;
        for (int bs = 0; bs <= 1; bs++) {
            s = new MVStore.Builder().
                    fileName(fileName).
                    autoCommitBufferSize(bs).
                    open();
            m = s.openMap("data");
            for (int i = 0; i < len; i++) {
                m.put(i, data);
            }
            long size = s.getFileStore().size();
            assertTrue("last:" + lastSize + " now: " + size, size > lastSize);
            lastSize = size;
            s.close();
        }

        s = new MVStore.Builder().
                fileName(fileName).
                open();
        m = s.openMap("data");
        assertTrue(m.containsKey(1));

        m.put(-1, data);
        s.commit();
        m.put(-2, data);
        s.close();

        s = new MVStore.Builder().
                fileName(fileName).
                open();
        m = s.openMap("data");
        assertTrue(m.containsKey(-1));
        assertTrue(m.containsKey(-2));

        s.close();
        FileUtils.delete(fileName);
    }

    private void testWriteDelay() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, String> m;

        FileUtils.delete(fileName);
        s = new MVStore.Builder().
                autoCommitDisabled().
                fileName(fileName).open();
        m = s.openMap("data");
        m.put(1, "1");
        s.commit();
        s.close();
        s = new MVStore.Builder().
                autoCommitDisabled().
                fileName(fileName).open();
        m = s.openMap("data");
        assertEquals(1, m.size());
        s.close();

        FileUtils.delete(fileName);
        s = new MVStore.Builder().
                fileName(fileName).
                open();
        m = s.openMap("data");
        m.put(1, "Hello");
        m.put(2, "World.");
        s.commit();
        s.close();

        s = new MVStore.Builder().
                fileName(fileName).
                open();
        s.setAutoCommitDelay(2);
        m = s.openMap("data");
        assertEquals("World.", m.get(2));
        m.put(2, "World");
        s.commit();
        long v = s.getCurrentVersion();
        long time = System.nanoTime();
        m.put(3, "!");

        for (int i = 200; i > 0; i--) {
            if (s.getCurrentVersion() > v) {
                break;
            }
            long diff = System.nanoTime() - time;
            if (diff > TimeUnit.SECONDS.toNanos(1)) {
                fail("diff=" + TimeUnit.NANOSECONDS.toMillis(diff));
            }
            sleep(10);
        }
        s.closeImmediately();

        s = new MVStore.Builder().
                fileName(fileName).
                open();
        m = s.openMap("data");
        assertEquals("Hello", m.get(1));
        assertEquals("World", m.get(2));
        assertEquals("!", m.get(3));
        s.close();

        FileUtils.delete(fileName);
    }

    private void testEncryptedFile() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, String> m;

        char[] passwordChars = "007".toCharArray();
        s = new MVStore.Builder().
                fileName(fileName).
                encryptionKey(passwordChars).
                open();
        assertEquals(0, passwordChars[0]);
        assertEquals(0, passwordChars[1]);
        assertEquals(0, passwordChars[2]);
        assertTrue(FileUtils.exists(fileName));
        m = s.openMap("test");
        m.put(1, "Hello");
        assertEquals("Hello", m.get(1));
        s.close();

        passwordChars = "008".toCharArray();
        try {
            s = new MVStore.Builder().
                    fileName(fileName).
                    encryptionKey(passwordChars).open();
            fail();
        } catch (IllegalStateException e) {
            assertEquals(DataUtils.ERROR_FILE_CORRUPT,
                    DataUtils.getErrorCode(e.getMessage()));
        }
        assertEquals(0, passwordChars[0]);
        assertEquals(0, passwordChars[1]);
        assertEquals(0, passwordChars[2]);

        passwordChars = "007".toCharArray();
        s = new MVStore.Builder().
                fileName(fileName).
                encryptionKey(passwordChars).open();
        assertEquals(0, passwordChars[0]);
        assertEquals(0, passwordChars[1]);
        assertEquals(0, passwordChars[2]);
        m = s.openMap("test");
        assertEquals("Hello", m.get(1));
        s.close();

        FileUtils.setReadOnly(fileName);
        passwordChars = "007".toCharArray();
        s = new MVStore.Builder().
                fileName(fileName).
                encryptionKey(passwordChars).open();
        assertTrue(s.getFileStore().isReadOnly());
        s.close();

        FileUtils.delete(fileName);
        assertFalse(FileUtils.exists(fileName));
    }

    private void testFileFormatChange() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, Integer> m;
        s = openStore(fileName);
        s.setRetentionTime(Integer.MAX_VALUE);
        m = s.openMap("test");
        m.put(1, 1);
        Map<String, Object> header = s.getStoreHeader();
        int format = Integer.parseInt(header.get("format").toString());
        assertEquals(1, format);
        header.put("format", Integer.toString(format + 1));
        forceWriteStoreHeader(s);
        s.close();
        try {
            openStore(fileName).close();
            fail();
        } catch (IllegalStateException e) {
            assertEquals(DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    DataUtils.getErrorCode(e.getMessage()));
        }
        FileUtils.delete(fileName);
    }

    private void testRecreateMap() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<Integer, Integer> m = s.openMap("test");
        m.put(1, 1);
        s.commit();
        s.removeMap(m);
        s.close();
        s = openStore(fileName);
        m = s.openMap("test");
        assertNull(m.get(1));
        s.close();
    }

    private void testRenameMapRollback() {
        MVStore s = openStore(null);
        MVMap<Integer, Integer> map;
        map = s.openMap("hello");
        map.put(1, 10);
        long old = s.commit();
        s.renameMap(map, "world");
        map.put(2, 20);
        assertEquals("world", map.getName());
        s.rollbackTo(old);
        assertEquals("hello", map.getName());
        s.rollbackTo(0);
        assertTrue(map.isClosed());
        s.close();
    }

    private void testCustomMapType() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        SequenceMap seq = s.openMap("data", new SequenceMap.Builder());
        StringBuilder buff = new StringBuilder();
        for (long x : seq.keySet()) {
            buff.append(x).append(';');
        }
        assertEquals("1;2;3;4;5;6;7;8;9;10;", buff.toString());
        s.close();
    }

    private void testCacheSize() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, String> map;
        s = new MVStore.Builder().
                fileName(fileName).
                autoCommitDisabled().
                compress().open();
        map = s.openMap("test");
        // add 10 MB of data
        for (int i = 0; i < 1024; i++) {
            map.put(i, new String(new char[10240]));
        }
        s.close();
        int[] expectedReadsForCacheSize = {
                3407, 2590, 1924, 1440, 1330, 956, 918
        };
        for (int cacheSize = 0; cacheSize <= 6; cacheSize += 4) {
            int cacheMB = 1 + 3 * cacheSize;
            s = new MVStore.Builder().
                    fileName(fileName).
                    cacheSize(cacheMB).open();
            assertEquals(cacheMB, s.getCacheSize());
            map = s.openMap("test");
            for (int i = 0; i < 1024; i += 128) {
                for (int j = 0; j < i; j++) {
                    String x = map.get(j);
                    assertEquals(10240, x.length());
                }
            }
            long readCount = s.getFileStore().getReadCount();
            int expected = expectedReadsForCacheSize[cacheSize];
            assertTrue("reads: " + readCount + " expected: " + expected,
                    Math.abs(100 - (100 * expected / readCount)) < 5);
            s.close();
        }

    }

    private void testConcurrentOpen() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().fileName(fileName).open();
        try {
            MVStore s1 = new MVStore.Builder().fileName(fileName).open();
            s1.close();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
        try {
            MVStore s1 = new MVStore.Builder().fileName(fileName).readOnly().open();
            s1.close();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
        assertFalse(s.getFileStore().isReadOnly());
        s.close();
        s = new MVStore.Builder().fileName(fileName).readOnly().open();
        assertTrue(s.getFileStore().isReadOnly());
        s.close();
    }

    private void testFileHeader() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        s.setRetentionTime(Integer.MAX_VALUE);
        long time = System.currentTimeMillis();
        Map<String, Object> m = s.getStoreHeader();
        assertEquals("1", m.get("format").toString());
        long creationTime = (Long) m.get("created");
        assertTrue(Math.abs(time - creationTime) < 100);
        m.put("test", "123");
        forceWriteStoreHeader(s);
        s.close();
        s = openStore(fileName);
        Object test = s.getStoreHeader().get("test");
        assertFalse(test == null);
        assertEquals("123", test.toString());
        s.close();
    }

    private static void forceWriteStoreHeader(MVStore s) {
        MVMap<Integer, Integer> map = s.openMap("dummy");
        map.put(10, 100);
        // this is to ensure the file header is overwritten
        // the header is written at least every 20 commits
        for (int i = 0; i < 30; i++) {
            if (i > 5) {
                s.setRetentionTime(0);
                // ensure that the next save time is different,
                // so that blocks can be reclaimed
                // (on Windows, resolution is 10 ms)
                sleep(1);
            }
            map.put(10, 110);
            s.commit();
        }
        s.removeMap(map);
        s.commit();
    }

    private static void sleep(long ms) {
        // on Windows, need to sleep in some cases,
        // mainly because the milliseconds resolution of
        // System.currentTimeMillis is 10 ms.
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private void testFileHeaderCorruption() throws Exception {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = new MVStore.Builder().
                fileName(fileName).pageSplitSize(1000).autoCommitDisabled().open();
        s.setRetentionTime(0);
        MVMap<Integer, byte[]> map;
        map = s.openMap("test");
        map.put(0, new byte[100]);
        for (int i = 0; i < 10; i++) {
            map = s.openMap("test" + i);
            map.put(0, new byte[1000]);
            s.commit();
        }
        FileStore fs = s.getFileStore();
        long size = fs.getFile().size();
        for (int i = 0; i < 100; i++) {
            map = s.openMap("test" + i);
            s.removeMap(map);
            s.commit();
            s.compact(100, 1);
            if (fs.getFile().size() <= size) {
                break;
            }
        }
        // the last chunk is at the end
        s.setReuseSpace(false);
        map = s.openMap("test2");
        map.put(1, new byte[1000]);
        s.close();
        FilePath f = FilePath.get(fileName);
        int blockSize = 4 * 1024;
        // test corrupt file headers
        for (int i = 0; i <= blockSize; i += blockSize) {
            FileChannel fc = f.open("rw");
            if (i == 0) {
                // corrupt the last block (the end header)
                fc.write(ByteBuffer.allocate(256), fc.size() - 256);
            }
            ByteBuffer buff = ByteBuffer.allocate(4 * 1024);
            fc.read(buff, i);
            String h = new String(buff.array(), StandardCharsets.UTF_8).trim();
            int idx = h.indexOf("fletcher:");
            int old = Character.digit(h.charAt(idx + "fletcher:".length()), 16);
            int bad = (old + 1) & 15;
            buff.put(idx + "fletcher:".length(),
                    (byte) Character.forDigit(bad, 16));
            buff.rewind();
            fc.write(buff, i);
            fc.close();

            if (i == 0) {
                // if the first header is corrupt, the second
                // header should be used
                s = openStore(fileName);
                map = s.openMap("test");
                assertEquals(100, map.get(0).length);
                map = s.openMap("test2");
                assertFalse(map.containsKey(1));
                s.close();
            } else {
                // both headers are corrupt
                try {
                    s = openStore(fileName);
                    fail();
                } catch (Exception e) {
                    // expected
                }
            }
        }
    }

    private void testIndexSkip() {
        MVStore s = openStore(null, 4);
        MVMap<Integer, Integer> map = s.openMap("test");
        for (int i = 0; i < 100; i += 2) {
            map.put(i, 10 * i);
        }

        Cursor<Integer, Integer> c = map.cursor(50);
        // skip must reset the root of the cursor
        c.skip(10);
        for (int i = 70; i < 100; i += 2) {
            assertTrue(c.hasNext());
            assertEquals(i, c.next().intValue());
        }
        assertFalse(c.hasNext());

        for (int i = -1; i < 100; i++) {
            long index = map.getKeyIndex(i);
            if (i < 0 || (i % 2) != 0) {
                assertEquals(i < 0 ? -1 : -(i / 2) - 2, index);
            } else {
                assertEquals(i / 2, index);
            }
        }
        for (int i = -1; i < 60; i++) {
            Integer k = map.getKey(i);
            if (i < 0 || i >= 50) {
                assertNull(k);
            } else {
                assertEquals(i * 2, k.intValue());
            }
        }
        // skip
        c = map.cursor(0);
        assertTrue(c.hasNext());
        assertEquals(0, c.next().intValue());
        c.skip(0);
        assertEquals(2, c.next().intValue());
        c.skip(1);
        assertEquals(6, c.next().intValue());
        c.skip(20);
        assertEquals(48, c.next().intValue());

        c = map.cursor(0);
        c.skip(20);
        assertEquals(40, c.next().intValue());

        c = map.cursor(0);
        assertEquals(0, c.next().intValue());

        assertEquals(12, map.keyList().indexOf(24));
        assertEquals(24, map.keyList().get(12).intValue());
        assertEquals(-14, map.keyList().indexOf(25));
        assertEquals(map.size(), map.keyList().size());
    }

    private void testMinMaxNextKey() {
        MVStore s = openStore(null);
        MVMap<Integer, Integer> map = s.openMap("test");
        map.put(10, 100);
        map.put(20, 200);

        assertEquals(10, map.firstKey().intValue());
        assertEquals(20, map.lastKey().intValue());

        assertEquals(20, map.ceilingKey(15).intValue());
        assertEquals(20, map.ceilingKey(20).intValue());
        assertEquals(10, map.floorKey(15).intValue());
        assertEquals(10, map.floorKey(10).intValue());
        assertEquals(20, map.higherKey(10).intValue());
        assertEquals(10, map.lowerKey(20).intValue());

        final MVMap<Integer, Integer> m = map;
        assertEquals(10, m.ceilingKey(null).intValue());
        assertEquals(10, m.higherKey(null).intValue());
        assertNull(m.lowerKey(null));
        assertNull(m.floorKey(null));

        for (int i = 3; i < 20; i++) {
            s = openStore(null, 4);
            map = s.openMap("test");
            for (int j = 3; j < i; j++) {
                map.put(j * 2, j * 20);
            }
            if (i == 3) {
                assertNull(map.firstKey());
                assertNull(map.lastKey());
            } else {
                assertEquals(6, map.firstKey().intValue());
                int max = (i - 1) * 2;
                assertEquals(max, map.lastKey().intValue());

                for (int j = 0; j < i * 2 + 2; j++) {
                    if (j > max) {
                        assertNull(map.ceilingKey(j));
                    } else {
                        int ceiling = Math.max((j + 1) / 2 * 2, 6);
                        assertEquals(ceiling, map.ceilingKey(j).intValue());
                    }

                    int floor = Math.min(max, Math.max(j / 2 * 2, 4));
                    if (floor < 6) {
                        assertNull(map.floorKey(j));
                    } else {
                        map.floorKey(j);
                    }

                    int lower = Math.min(max, Math.max((j - 1) / 2 * 2, 4));
                    if (lower < 6) {
                        assertNull(map.lowerKey(j));
                    } else {
                        assertEquals(lower, map.lowerKey(j).intValue());
                    }

                    int higher = Math.max((j + 2) / 2 * 2, 6);
                    if (higher > max) {
                        assertNull(map.higherKey(j));
                    } else {
                        assertEquals(higher, map.higherKey(j).intValue());
                    }
                }
            }
        }
    }

    private void testStoreVersion() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = MVStore.open(fileName);
        assertEquals(0, s.getCurrentVersion());
        assertEquals(0, s.getStoreVersion());
        s.setStoreVersion(0);
        s.commit();
        s.setStoreVersion(1);
        s.closeImmediately();
        s = MVStore.open(fileName);
        assertEquals(1, s.getCurrentVersion());
        assertEquals(0, s.getStoreVersion());
        s.setStoreVersion(1);
        s.close();
        s = MVStore.open(fileName);
        assertEquals(2, s.getCurrentVersion());
        assertEquals(1, s.getStoreVersion());
        s.close();
    }

    private void testIterateOldVersion() {
        MVStore s;
        Map<Integer, Integer> map;
        s = new MVStore.Builder().open();
        map = s.openMap("test");
        int len = 100;
        for (int i = 0; i < len; i++) {
            map.put(i, 10 * i);
        }
        Iterator<Integer> it = map.keySet().iterator();
        s.commit();
        for (int i = 0; i < len; i += 2) {
            map.remove(i);
        }
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        assertEquals(len, count);
        s.close();
    }

    private void testObjects() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        Map<Object, Object> map;
        s = new MVStore.Builder().fileName(fileName).open();
        map = s.openMap("test");
        map.put(1,  "Hello");
        map.put("2", 200);
        map.put(new Object[1], new Object[]{1, "2"});
        s.close();

        s = new MVStore.Builder().fileName(fileName).open();
        map = s.openMap("test");
        assertEquals("Hello", map.get(1).toString());
        assertEquals(200, ((Integer) map.get("2")).intValue());
        Object[] x = (Object[]) map.get(new Object[1]);
        assertEquals(2, x.length);
        assertEquals(1, ((Integer) x[0]).intValue());
        assertEquals("2", (String) x[1]);
        s.close();
    }

    private void testExample() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);

        // open the store (in-memory if fileName is null)
        MVStore s = MVStore.open(fileName);

        // create/get the map named "data"
        MVMap<Integer, String> map = s.openMap("data");

        // add and read some data
        map.put(1, "Hello World");
        // System.out.println(map.get(1));

        // close the store (this will persist changes)
        s.close();

        s = MVStore.open(fileName);
        map = s.openMap("data");
        assertEquals("Hello World", map.get(1));
        s.close();
    }

    private void testExampleMvcc() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);

        // open the store (in-memory if fileName is null)
        MVStore s = MVStore.open(fileName);

        // create/get the map named "data"
        MVMap<Integer, String> map = s.openMap("data");

        // add some data
        map.put(1, "Hello");
        map.put(2, "World");

        // get the current version, for later use
        long oldVersion = s.getCurrentVersion();

        // from now on, the old version is read-only
        s.commit();

        // more changes, in the new version
        // changes can be rolled back if required
        // changes always go into "head" (the newest version)
        map.put(1, "Hi");
        map.remove(2);

        // access the old data (before the commit)
        MVMap<Integer, String> oldMap =
                map.openVersion(oldVersion);

        // print the old version (can be done
        // concurrently with further modifications)
        // this will print "Hello" and "World":
        // System.out.println(oldMap.get(1));
        assertEquals("Hello", oldMap.get(1));
        // System.out.println(oldMap.get(2));
        assertEquals("World", oldMap.get(2));

        // print the newest version ("Hi")
        // System.out.println(map.get(1));
        assertEquals("Hi", map.get(1));

        // close the store
        s.close();
    }

    private void testOpenStoreCloseLoop() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        for (int k = 0; k < 1; k++) {
            // long t = System.nanoTime();
            for (int j = 0; j < 3; j++) {
                MVStore s = openStore(fileName);
                Map<String, Integer> m = s.openMap("data");
                for (int i = 0; i < 3; i++) {
                    Integer x = m.get("value");
                    m.put("value", x == null ? 0 : x + 1);
                    s.commit();
                }
                s.close();
            }
            // System.out.println("open/close: " +
            //        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t));
            // System.out.println("size: " + FileUtils.size(fileName));
        }
    }

    private void testOldVersion() {
        MVStore s;
        for (int op = 0; op <= 1; op++) {
            for (int i = 0; i < 5; i++) {
                s = openStore(null);
                s.setVersionsToKeep(Integer.MAX_VALUE);
                MVMap<String, String> m;
                m = s.openMap("data");
                for (int j = 0; j < 5; j++) {
                    if (op == 1) {
                        m.put("1", "" + s.getCurrentVersion());
                    }
                    s.commit();
                }
                for (int j = 0; j < s.getCurrentVersion(); j++) {
                    MVMap<String, String> old = m.openVersion(j);
                    if (op == 1) {
                        assertEquals("" + j, old.get("1"));
                    }
                }
                s.close();
            }
        }
    }

    private void testVersion() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        s = openStore(fileName);
        s.setVersionsToKeep(100);
        s.setAutoCommitDelay(0);
        s.setRetentionTime(Integer.MAX_VALUE);
        MVMap<String, String> m = s.openMap("data");
        s.commit();
        long first = s.getCurrentVersion();
        m.put("0", "test");
        s.commit();
        m.put("1", "Hello");
        m.put("2", "World");
        for (int i = 10; i < 20; i++) {
            m.put("" + i, "data");
        }
        long old = s.getCurrentVersion();
        s.commit();
        m.put("1", "Hallo");
        m.put("2", "Welt");
        MVMap<String, String> mFirst;
        mFirst = m.openVersion(first);
        assertEquals(0, mFirst.size());
        MVMap<String, String> mOld;
        assertEquals("Hallo", m.get("1"));
        assertEquals("Welt", m.get("2"));
        mOld = m.openVersion(old);
        assertEquals("Hello", mOld.get("1"));
        assertEquals("World", mOld.get("2"));
        assertTrue(mOld.isReadOnly());
        s.getCurrentVersion();
        long old3 = s.commit();

        // the old version is still available
        assertEquals("Hello", mOld.get("1"));
        assertEquals("World", mOld.get("2"));

        mOld = m.openVersion(old3);
        assertEquals("Hallo", mOld.get("1"));
        assertEquals("Welt", mOld.get("2"));

        m.put("1",  "Hi");
        assertEquals("Welt", m.remove("2"));
        s.close();

        s = openStore(fileName);
        m = s.openMap("data");
        assertEquals("Hi", m.get("1"));
        assertEquals(null, m.get("2"));

        mOld = m.openVersion(old3);
        assertEquals("Hallo", mOld.get("1"));
        assertEquals("Welt", mOld.get("2"));

        try {
            m.openVersion(-3);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        s.close();
    }

    private void testTruncateFile() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, String> m;
        s = openStore(fileName);
        m = s.openMap("data");
        String data = new String(new char[10000]).replace((char) 0, 'x');
        for (int i = 1; i < 10; i++) {
            m.put(i, data);
            s.commit();
        }
        s.close();
        long len = FileUtils.size(fileName);
        s = openStore(fileName);
        s.setRetentionTime(0);
        // remove 75%
        m = s.openMap("data");
        for (int i = 0; i < 10; i++) {
            if (i % 4 != 0) {
                sleep(2);
                m.remove(i);
                s.commit();
            }
        }
        assertTrue(s.compact(100, 50 * 1024));
        s.close();
        long len2 = FileUtils.size(fileName);
        assertTrue("len2: " + len2 + " len: " + len, len2 < len);
    }

    private void testFastDelete() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, String> m;
        s = openStore(fileName, 700);
        m = s.openMap("data");
        for (int i = 0; i < 1000; i++) {
            m.put(i, "Hello World");
            assertEquals(i + 1, m.size());
        }
        assertEquals(1000, m.size());
        // previously (131896) we fail to account for initial root page for every map
        // there are two of them here (meta and "data"), hence lack of 256 bytes
        assertEquals(132152, s.getUnsavedMemory());
        s.commit();
        assertEquals(2, s.getFileStore().getWriteCount());
        s.close();

        s = openStore(fileName);
        m = s.openMap("data");
        m.clear();
        assertEquals(0, m.size());
        s.commit();
        // ensure only nodes are read, but not leaves
        assertEquals(45, s.getFileStore().getReadCount());
        assertTrue(s.getFileStore().getWriteCount() < 5);
        s.close();
    }

    private void testRollback() {
        MVStore s = MVStore.open(null);
        MVMap<Integer, Integer> m = s.openMap("m");
        m.put(1, -1);
        s.commit();
        for (int i = 0; i < 10; i++) {
            m.put(1, i);
            s.rollback();
            assertEquals(i - 1, m.get(1).intValue());
            m.put(1, i);
            s.commit();
        }
    }

    private void testRollbackStored() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVMap<String, String> meta;
        MVStore s = openStore(fileName);
        assertEquals(45000, s.getRetentionTime());
        s.setRetentionTime(0);
        assertEquals(0, s.getRetentionTime());
        s.setRetentionTime(45000);
        assertEquals(45000, s.getRetentionTime());
        assertEquals(0, s.getCurrentVersion());
        assertFalse(s.hasUnsavedChanges());
        MVMap<String, String> m = s.openMap("data");
        assertTrue(s.hasUnsavedChanges());
        MVMap<String, String> m0 = s.openMap("data0");
        m.put("1", "Hello");
        assertEquals(1, s.commit());
        s.rollbackTo(1);
        assertEquals(1, s.getCurrentVersion());
        assertEquals("Hello", m.get("1"));
        // so a new version is created
        m.put("1", "Hello");

        long v2 = s.commit();
        assertEquals(2, v2);
        assertEquals(2, s.getCurrentVersion());
        assertFalse(s.hasUnsavedChanges());
        assertEquals("Hello", m.get("1"));
        s.close();

        s = openStore(fileName);
        s.setRetentionTime(45000);
        assertEquals(2, s.getCurrentVersion());
        meta = s.getMetaMap();
        m = s.openMap("data");
        assertFalse(s.hasUnsavedChanges());
        assertEquals("Hello", m.get("1"));
        m0 = s.openMap("data0");
        MVMap<String, String> m1 = s.openMap("data1");
        m.put("1", "Hallo");
        m0.put("1", "Hallo");
        m1.put("1", "Hallo");
        assertEquals("Hallo", m.get("1"));
        assertEquals("Hallo", m1.get("1"));
        assertTrue(s.hasUnsavedChanges());
        s.rollbackTo(v2);
        assertFalse(s.hasUnsavedChanges());
        assertNull(meta.get("name.data1"));
        assertNull(m0.get("1"));
        assertEquals("Hello", m.get("1"));
        assertEquals(2, s.commit());
        s.close();

        s = openStore(fileName);
        s.setRetentionTime(45000);
        assertEquals(2, s.getCurrentVersion());
        meta = s.getMetaMap();
        assertTrue(meta.get("name.data") != null);
        assertTrue(meta.get("name.data0") != null);
        assertNull(meta.get("name.data1"));
        m = s.openMap("data");
        m0 = s.openMap("data0");
        assertNull(m0.get("1"));
        assertEquals("Hello", m.get("1"));
        assertFalse(m0.isReadOnly());
        m.put("1",  "Hallo");
        s.commit();
        long v3 = s.getCurrentVersion();
        assertEquals(3, v3);
        s.close();

        s = openStore(fileName);
        s.setRetentionTime(45000);
        assertEquals(3, s.getCurrentVersion());
        m = s.openMap("data");
        m.put("1",  "Hi");
        s.close();

        s = openStore(fileName);
        s.setRetentionTime(45000);
        m = s.openMap("data");
        assertEquals("Hi", m.get("1"));
        s.rollbackTo(v3);
        assertEquals("Hallo", m.get("1"));
        s.close();

        s = openStore(fileName);
        s.setRetentionTime(45000);
        m = s.openMap("data");
        assertEquals("Hallo", m.get("1"));
        s.close();
    }

    private void testRollbackInMemory() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName, 5);
        s.setAutoCommitDelay(0);
        assertEquals(0, s.getCurrentVersion());
        MVMap<String, String> m = s.openMap("data");
        s.rollbackTo(0);
        assertTrue(m.isClosed());
        assertEquals(0, s.getCurrentVersion());
        m = s.openMap("data");

        MVMap<String, String> m0 = s.openMap("data0");
        MVMap<String, String> m2 = s.openMap("data2");
        m.put("1", "Hello");
        for (int i = 0; i < 10; i++) {
            m2.put("" + i, "Test");
        }
        long v1 = s.commit();
        assertEquals(1, v1);
        assertEquals(1, s.getCurrentVersion());
        MVMap<String, String> m1 = s.openMap("data1");
        assertEquals("Test", m2.get("1"));
        m.put("1", "Hallo");
        m0.put("1", "Hallo");
        m1.put("1", "Hallo");
        m2.clear();
        assertEquals("Hallo", m.get("1"));
        assertEquals("Hallo", m1.get("1"));
        s.rollbackTo(v1);
        assertEquals(1, s.getCurrentVersion());
        for (int i = 0; i < 10; i++) {
            assertEquals("Test", m2.get("" + i));
        }
        assertEquals("Hello", m.get("1"));
        assertNull(m0.get("1"));
        assertTrue(m1.isClosed());
        assertFalse(m0.isReadOnly());
        s.close();
    }

    private void testMeta() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        s.setRetentionTime(Integer.MAX_VALUE);
        MVMap<String, String> m = s.getMetaMap();
        assertEquals("[]", s.getMapNames().toString());
        MVMap<String, String> data = s.openMap("data");
        data.put("1", "Hello");
        data.put("2", "World");
        s.commit();
        assertEquals(1, s.getCurrentVersion());

        assertEquals("[data]", s.getMapNames().toString());
        assertEquals("data", s.getMapName(data.getId()));
        assertNull(s.getMapName(s.getMetaMap().getId()));
        assertNull(s.getMapName(data.getId() + 1));

        String id = s.getMetaMap().get("name.data");
        assertEquals("name:data", m.get("map." + id));
        assertEquals("Hello", data.put("1", "Hallo"));
        s.commit();
        assertEquals("name:data", m.get("map." + id));
        assertTrue(m.get("root.1").length() > 0);
        assertTrue(m.containsKey("chunk.1"));

        assertEquals(2, s.getCurrentVersion());

        s.rollbackTo(1);
        assertEquals("Hello", data.get("1"));
        assertEquals("World", data.get("2"));

        s.close();
    }

    private void testInMemory() {
        for (int j = 0; j < 1; j++) {
            MVStore s = openStore(null);
            // s.setMaxPageSize(10);
            int len = 100;
            // TreeMap<Integer, String> m = new TreeMap<Integer, String>();
            // HashMap<Integer, String> m = New.hashMap();
            MVMap<Integer, String> m = s.openMap("data");
            for (int i = 0; i < len; i++) {
                assertNull(m.put(i, "Hello World"));
            }
            for (int i = 0; i < len; i++) {
                assertEquals("Hello World", m.get(i));
            }
            for (int i = 0; i < len; i++) {
                assertEquals("Hello World", m.remove(i));
            }
            assertEquals(null, m.get(0));
            assertEquals(0, m.size());
            s.close();
        }
    }

    private void testLargeImport() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        int len = 1000;
        for (int j = 0; j < 5; j++) {
            FileUtils.delete(fileName);
            MVStore s = openStore(fileName, 40);
            MVMap<Integer, Object[]> m = s.openMap("data",
                    new MVMap.Builder<Integer, Object[]>()
                            .valueType(new RowDataType(new DataType[] {
                                    new ObjectDataType(),
                                    StringDataType.INSTANCE,
                                    StringDataType.INSTANCE })));

            // Profiler prof = new Profiler();
            // prof.startCollecting();
            // long t = System.nanoTime();
            for (int i = 0; i < len;) {
                Object[] o = new Object[3];
                o[0] = i;
                o[1] = "Hello World";
                o[2] = "World";
                m.put(i, o);
                i++;
                if (i % 10000 == 0) {
                    s.commit();
                }
            }
            s.close();
            // System.out.println(prof.getTop(5));
            // System.out.println("store time " +
            //         TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t));
            // System.out.println("store size " +
            //         FileUtils.size(fileName));
        }
    }

    private void testBtreeStore() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        s.close();

        s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data");
        int count = 2000;
        for (int i = 0; i < count; i++) {
            assertNull(m.put(i, "hello " + i));
            assertEquals("hello " + i, m.get(i));
        }
        s.commit();
        assertEquals("hello 0", m.remove(0));
        assertNull(m.get(0));
        for (int i = 1; i < count; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.close();

        s = openStore(fileName);
        m = s.openMap("data");
        assertNull(m.get(0));
        for (int i = 1; i < count; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        for (int i = 1; i < count; i++) {
            m.remove(i);
        }
        s.commit();
        assertNull(m.get(0));
        for (int i = 0; i < count; i++) {
            assertNull(m.get(i));
        }
        s.close();
    }

    private void testCompactMapNotOpen() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName, 1000);
        MVMap<Integer, String> m = s.openMap("data");
        int factor = 100;
        for (int j = 0; j < 10; j++) {
            for (int i = j * factor; i < 10 * factor; i++) {
                m.put(i, "Hello" + j);
            }
            s.commit();
        }
        s.close();

        s = openStore(fileName);
        s.setRetentionTime(0);

        Map<String, String> meta = s.getMetaMap();
        int chunkCount1 = 0;
        for (String k : meta.keySet()) {
            if (k.startsWith("chunk.")) {
                chunkCount1++;
            }
        }
        s.compact(80, 1);
        s.compact(80, 1);

        int chunkCount2 = 0;
        for (String k : meta.keySet()) {
            if (k.startsWith("chunk.")) {
                chunkCount2++;
            }
        }
        assertTrue(chunkCount2 >= chunkCount1);

        m = s.openMap("data");
        for (int i = 0; i < 10; i++) {
            sleep(1);
            boolean result = s.compact(50, 50 * 1024);
            if (!result) {
                break;
            }
        }
        assertFalse(s.compact(50, 1024));

        int chunkCount3 = 0;
        for (String k : meta.keySet()) {
            if (k.startsWith("chunk.")) {
                chunkCount3++;
            }
        }

        assertTrue(chunkCount1 + ">" + chunkCount2 + ">" + chunkCount3,
                chunkCount3 < chunkCount1);

        for (int i = 0; i < 10 * factor; i++) {
            assertEquals("x" + i, "Hello" + (i / factor), m.get(i));
        }
        s.close();
    }

    private void testCompact() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        long initialLength = 0;
        for (int j = 0; j < 20; j++) {
            sleep(2);
            MVStore s = openStore(fileName);
            s.setRetentionTime(0);
            MVMap<Integer, String> m = s.openMap("data");
            for (int i = 0; i < 100; i++) {
                m.put(j + i, "Hello " + j);
            }
            s.compact(80, 1024);
            s.close();
            long len = FileUtils.size(fileName);
            // System.out.println("   len:" + len);
            if (initialLength == 0) {
                initialLength = len;
            } else {
                assertTrue("initial: " + initialLength + " len: " + len,
                        len <= initialLength * 3);
            }
        }
        // long len = FileUtils.size(fileName);
        // System.out.println("len0: " + len);
        MVStore s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data");
        for (int i = 0; i < 100; i++) {
            m.remove(i);
        }
        s.compact(80, 1024);
        s.close();
        // len = FileUtils.size(fileName);
        // System.out.println("len1: " + len);
        s = openStore(fileName);
        m = s.openMap("data");
        s.compact(80, 1024);
        s.close();
        // len = FileUtils.size(fileName);
        // System.out.println("len2: " + len);
    }

    private void testReuseSpace() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        long initialLength = 0;
        for (int j = 0; j < 20; j++) {
            sleep(2);
            MVStore s = openStore(fileName);
            s.setRetentionTime(0);
            MVMap<Integer, String> m = s.openMap("data");
            for (int i = 0; i < 10; i++) {
                m.put(i, "Hello");
            }
            s.commit();
            for (int i = 0; i < 10; i++) {
                assertEquals("Hello", m.get(i));
                assertEquals("Hello", m.remove(i));
            }
            s.close();
            long len = FileUtils.size(fileName);
            if (initialLength == 0) {
                initialLength = len;
            } else {
                assertTrue("len: " + len + " initial: " + initialLength + " j: " + j,
                        len <= initialLength * 5);
            }
        }
    }

    private void testRandom() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<Integer, Integer> m = s.openMap("data");
        TreeMap<Integer, Integer> map = new TreeMap<>();
        Random r = new Random(1);
        int operationCount = 1000;
        int maxValue = 30;
        Integer expected, got;
        for (int i = 0; i < operationCount; i++) {
            int k = r.nextInt(maxValue);
            int v = r.nextInt();
            boolean compareAll;
            switch (r.nextInt(3)) {
            case 0:
                log(i + ": put " + k + " = " + v);
                expected = map.put(k, v);
                got = m.put(k, v);
                if (expected == null) {
                    assertNull(got);
                } else {
                    assertEquals(expected, got);
                }
                compareAll = true;
                break;
            case 1:
                log(i + ": remove " + k);
                expected = map.remove(k);
                got = m.remove(k);
                if (expected == null) {
                    assertNull(got);
                } else {
                    assertEquals(expected, got);
                }
                compareAll = true;
                break;
            default:
                Integer a = map.get(k);
                Integer b = m.get(k);
                if (a == null || b == null) {
                    assertTrue(a == b);
                } else {
                    assertEquals(a.intValue(), b.intValue());
                }
                compareAll = false;
                break;
            }
            if (compareAll) {
                Iterator<Integer> it = m.keyIterator(null);
                Iterator<Integer> itExpected = map.keySet().iterator();
                while (itExpected.hasNext()) {
                    assertTrue(it.hasNext());
                    expected = itExpected.next();
                    got = it.next();
                    assertEquals(expected, got);
                }
                assertFalse(it.hasNext());
            }
        }
        s.close();
    }

    private void testKeyValueClasses() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<Integer, String> is = s.openMap("intString");
        is.put(1, "Hello");
        MVMap<Integer, Integer> ii = s.openMap("intInt");
        ii.put(1, 10);
        MVMap<String, Integer> si = s.openMap("stringInt");
        si.put("Test", 10);
        MVMap<String, String> ss = s.openMap("stringString");
        ss.put("Hello", "World");
        s.close();
        s = openStore(fileName);
        is = s.openMap("intString");
        assertEquals("Hello", is.get(1));
        ii = s.openMap("intInt");
        assertEquals(10, ii.get(1).intValue());
        si = s.openMap("stringInt");
        assertEquals(10, si.get("Test").intValue());
        ss = s.openMap("stringString");
        assertEquals("World", ss.get("Hello"));
        s.close();
    }

    private void testIterate() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data");
        Iterator<Integer> it = m.keyIterator(null);
        assertFalse(it.hasNext());
        for (int i = 0; i < 10; i++) {
            m.put(i, "hello " + i);
        }
        s.commit();
        it = m.keyIterator(null);
        it.next();
        assertThrows(UnsupportedOperationException.class, it).remove();

        it = m.keyIterator(null);
        for (int i = 0; i < 10; i++) {
            assertTrue(it.hasNext());
            assertEquals(i, it.next().intValue());
        }
        assertFalse(it.hasNext());
        assertNull(it.next());
        for (int j = 0; j < 10; j++) {
            it = m.keyIterator(j);
            for (int i = j; i < 10; i++) {
                assertTrue(it.hasNext());
                assertEquals(i, it.next().intValue());
            }
            assertFalse(it.hasNext());
        }
        s.close();
    }

    private void testCloseTwice() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data");
        for (int i = 0; i < 3; i++) {
            m.put(i, "hello " + i);
        }
        // closing twice should be fine
        s.close();
        s.close();
    }

    private void testSimple() {
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data");
        for (int i = 0; i < 3; i++) {
            m.put(i, "hello " + i);
        }
        s.commit();
        assertEquals("hello 0", m.remove(0));

        assertNull(m.get(0));
        for (int i = 1; i < 3; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.close();

        s = openStore(fileName);
        m = s.openMap("data");
        assertNull(m.get(0));
        for (int i = 1; i < 3; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.close();
    }

    private void testLargerThan2G() {
        if (!config.big) {
            return;
        }
        String fileName = getBaseDir() + "/" + getTestName();
        FileUtils.delete(fileName);
        MVStore store = new MVStore.Builder().cacheSize(16).
                fileName(fileName).open();
        try {
            MVMap<Integer, String> map = store.openMap("test");
            long last = System.nanoTime();
            String data = new String(new char[2500]).replace((char) 0, 'x');
            for (int i = 0;; i++) {
                map.put(i, data);
                if (i % 10000 == 0) {
                    store.commit();
                    long time = System.nanoTime();
                    if (time - last > TimeUnit.SECONDS.toNanos(2)) {
                        long mb = store.getFileStore().size() / 1024 / 1024;
                        trace(mb + "/4500");
                        if (mb > 4500) {
                            break;
                        }
                        last = time;
                    }
                }
            }
            store.commit();
            store.close();
        } finally {
            store.closeImmediately();
        }
        FileUtils.delete(fileName);
    }

    /**
     * Open a store for the given file name, using a small page size.
     *
     * @param fileName the file name (null for in-memory)
     * @return the store
     */
    protected static MVStore openStore(String fileName) {
        return openStore(fileName, 1000);
    }

    /**
     * Open a store for the given file name, using a small page size.
     *
     * @param fileName the file name (null for in-memory)
     * @param pageSplitSize the page split size
     * @return the store
     */
    protected static MVStore openStore(String fileName, int pageSplitSize) {
        MVStore store = new MVStore.Builder().
                fileName(fileName).pageSplitSize(pageSplitSize).open();
        return store;
    }

    /**
     * Log the message.
     *
     * @param msg the message
     */
    @SuppressWarnings("unused")
    protected static void log(String msg) {
        // System.out.println(msg);
    }

}
