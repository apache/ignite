/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.h2.dev.fs.FilePathZip2;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.cache.FilePathCache;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathEncrypt;
import org.h2.store.fs.FilePathRec;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.AssertThrows;
import org.h2.test.utils.FilePathDebug;
import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.IOUtils;
import org.h2.util.Task;

/**
 * Tests various file system.
 */
public class TestFileSystem extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        // test.config.traceTest = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        testFileSystem(getBaseDir() + "/fs");

        testAbsoluteRelative();
        testDirectories(getBaseDir());
        testMoveTo(getBaseDir());
        testUnsupportedFeatures(getBaseDir());
        FilePathZip2.register();
        FilePath.register(new FilePathCache());
        FilePathRec.register();
        testZipFileSystem("zip:");
        testZipFileSystem("cache:zip:");
        testZipFileSystem("zip2:");
        testZipFileSystem("cache:zip2:");
        testMemFsDir();
        testClasspath();
        FilePathDebug.register().setTrace(true);
        FilePathEncrypt.register();
        testSimpleExpandTruncateSize();
        testSplitDatabaseInZip();
        testDatabaseInMemFileSys();
        testDatabaseInJar();
        // set default part size to 1 << 10
        String f = "split:10:" + getBaseDir() + "/fs";
        FileUtils.toRealPath(f);
        testFileSystem(getBaseDir() + "/fs");
        testFileSystem("memFS:");
        testFileSystem("memLZF:");
        testFileSystem("nioMemFS:");
        testFileSystem("nioMemLZF:1:");
        // 12% compressLaterCache
        testFileSystem("nioMemLZF:12:");
        testFileSystem("rec:memFS:");
        testUserHome();
        try {
            testFileSystem("nio:" + getBaseDir() + "/fs");
            testFileSystem("cache:nio:" + getBaseDir() + "/fs");
            testFileSystem("nioMapped:" + getBaseDir() + "/fs");
            testFileSystem("encrypt:0007:" + getBaseDir() + "/fs");
            testFileSystem("cache:encrypt:0007:" + getBaseDir() + "/fs");
            if (!config.splitFileSystem) {
                testFileSystem("split:" + getBaseDir() + "/fs");
                testFileSystem("split:nioMapped:" + getBaseDir() + "/fs");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } catch (Error e) {
            e.printStackTrace();
            throw e;
        } finally {
            FileUtils.delete(getBaseDir() + "/fs");
        }
    }

    private void testZipFileSystem(String prefix) throws IOException {
        Random r = new Random(1);
        for (int i = 0; i < 5; i++) {
            testZipFileSystem(prefix, r);
        }
    }

    private void testZipFileSystem(String prefix, Random r) throws IOException {
        byte[] data = new byte[r.nextInt(16 * 1024)];
        long x = r.nextLong();
        FilePath file = FilePath.get(getBaseDir() + "/fs/readonly" + x + ".zip");
        r.nextBytes(data);
        OutputStream out = file.newOutputStream(false);
        ZipOutputStream zipOut = new ZipOutputStream(out);
        ZipEntry entry = new ZipEntry("data");
        zipOut.putNextEntry(entry);
        zipOut.write(data);
        zipOut.closeEntry();
        zipOut.close();
        out.close();
        FilePath fp = FilePath.get(
                prefix + getBaseDir() + "/fs/readonly" + x + ".zip!data");
        FileChannel fc = fp.open("r");
        StringBuilder buff = new StringBuilder();
        try {
            int pos = 0;
            for (int i = 0; i < 100; i++) {
                trace("op " + i);
                switch (r.nextInt(5)) {
                case 0: {
                    int p = r.nextInt(data.length);
                    trace("seek " + p);
                    buff.append("seek " + p + "\n");
                    fc.position(p);
                    pos = p;
                    break;
                }
                case 1: {
                    int len = r.nextInt(1000);
                    int offset = r.nextInt(100);
                    int arrayLen = len + offset;
                    len = Math.min(len, data.length - pos);
                    byte[] b1 = new byte[arrayLen];
                    byte[] b2 = new byte[arrayLen];
                    trace("readFully " + len);
                    buff.append("readFully " + len + "\n");
                    System.arraycopy(data, pos, b1, offset, len);
                    ByteBuffer byteBuff = createSlicedBuffer(b2, offset, len);
                    FileUtils.readFully(fc, byteBuff);
                    assertEquals(b1, b2);
                    pos += len;
                    break;
                }
                case 2: {
                    int len = r.nextInt(1000);
                    int offset = r.nextInt(100);
                    int arrayLen = len + offset;
                    int p = r.nextInt(data.length);
                    len = Math.min(len, data.length - p);
                    byte[] b1 = new byte[arrayLen];
                    byte[] b2 = new byte[arrayLen];
                    trace("readFully " + p + " " + len);
                    buff.append("readFully " + p + " " + len + "\n");
                    System.arraycopy(data, p, b1, offset, len);
                    ByteBuffer byteBuff = createSlicedBuffer(b2, offset, len);
                    DataUtils.readFully(fc, p, byteBuff);
                    assertEquals(b1, b2);
                    break;
                }
                case 3: {
                    trace("getFilePointer");
                    buff.append("getFilePointer\n");
                    assertEquals(pos, fc.position());
                    break;
                }
                case 4: {
                    trace("length " + data.length);
                    buff.append("length " + data.length + "\n");
                    assertEquals(data.length, fc.size());
                    break;
                }
                default:
                }
            }
            fc.close();
            file.delete();
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Exception: " + e + "\n"+ buff.toString());
        }
    }

    private void testAbsoluteRelative() {
        assertFalse(FileUtils.isAbsolute("test/abc"));
        assertTrue(FileUtils.isAbsolute("~/test/abc"));
    }

    private void testMemFsDir() throws IOException {
        FileUtils.newOutputStream("memFS:data/test/a.txt", false).close();
        assertEquals(FileUtils.newDirectoryStream("memFS:data/test").toString(),
                1, FileUtils.newDirectoryStream("memFS:data/test").size());
        FileUtils.deleteRecursive("memFS:", false);
    }

    private void testClasspath() throws IOException {
        String resource = "org/h2/test/scripts/testSimple.in.txt";
        InputStream in;
        in = getClass().getResourceAsStream("/" + resource);
        assertTrue(in != null);
        in.close();
        in = getClass().getClassLoader().getResourceAsStream(resource);
        assertTrue(in != null);
        in.close();
        in = FileUtils.newInputStream("classpath:" + resource);
        assertTrue(in != null);
        in.close();
        in = FileUtils.newInputStream("classpath:/" + resource);
        assertTrue(in != null);
        in.close();
    }

    private void testSimpleExpandTruncateSize() throws Exception {
        String f = "memFS:" + getBaseDir() + "/fs/test.data";
        FileUtils.createDirectories("memFS:" + getBaseDir() + "/fs");
        FileChannel c = FileUtils.open(f, "rw");
        c.position(4000);
        c.write(ByteBuffer.wrap(new byte[1]));
        FileLock lock = c.tryLock();
        c.truncate(0);
        if (lock != null) {
            lock.release();
        }
        c.close();
        FileUtils.deleteRecursive("memFS:", false);
    }

    private void testSplitDatabaseInZip() throws SQLException {
        String dir = getBaseDir() + "/fs";
        FileUtils.deleteRecursive(dir, false);
        Connection conn;
        Statement stat;
        conn = getConnection("jdbc:h2:split:18:"+dir+"/test");
        stat = conn.createStatement();
        stat.execute(
                "create table test(id int primary key, name varchar) " +
                "as select x, space(10000) from system_range(1, 100)");
        // stat.execute("shutdown defrag");
        conn.close();
        Backup.execute(dir + "/test.zip", dir, "", true);
        DeleteDbFiles.execute("split:" + dir, "test", true);
        conn = getConnection(
                "jdbc:h2:split:zip:"+dir+"/test.zip!/test");
        conn.createStatement().execute("select * from test where id=1");
        conn.close();
        FileUtils.deleteRecursive(dir, false);
    }

    private void testDatabaseInMemFileSys() throws SQLException {
        org.h2.Driver.load();
        deleteDb("fsMem");
        String url = "jdbc:h2:" + getBaseDir() + "/fsMem";
        Connection conn = getConnection(url, "sa", "sa");
        conn.createStatement().execute(
                "CREATE TABLE TEST AS SELECT * FROM DUAL");
        conn.createStatement().execute(
                "BACKUP TO '" + getBaseDir() + "/fsMem.zip'");
        conn.close();
        org.h2.tools.Restore.main("-file", getBaseDir() + "/fsMem.zip", "-dir",
                "memFS:");
        conn = getConnection("jdbc:h2:memFS:fsMem", "sa", "sa");
        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT * FROM TEST");
        rs.close();
        conn.close();
        deleteDb("fsMem");
        FileUtils.delete(getBaseDir() + "/fsMem.zip");
        FileUtils.delete("memFS:fsMem.mv.db");
    }

    private void testDatabaseInJar() throws Exception {
        if (getBaseDir().indexOf(':') > 0) {
            return;
        }
        if (config.networked) {
            return;
        }
        org.h2.Driver.load();
        String url = "jdbc:h2:" + getBaseDir() + "/fsJar";
        Connection conn = getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, " +
                "name varchar, b blob, c clob)");
        stat.execute("insert into test values(1, 'Hello', " +
                "SECURE_RAND(2000), space(2000))");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        byte[] b1 = rs.getBytes(3);
        String s1 = rs.getString(4);
        conn.close();
        conn = getConnection(url, "sa", "sa");
        stat = conn.createStatement();
        stat.execute("backup to '" + getBaseDir() + "/fsJar.zip'");
        conn.close();

        deleteDb("fsJar");
        for (String f : FileUtils.newDirectoryStream(
                "zip:" + getBaseDir() + "/fsJar.zip")) {
            assertFalse(FileUtils.isAbsolute(f));
            assertTrue(!FileUtils.isDirectory(f));
            assertTrue(FileUtils.size(f) > 0);
            assertTrue(f.endsWith(FileUtils.getName(f)));
            assertEquals(0, FileUtils.lastModified(f));
            FileUtils.setReadOnly(f);
            assertFalse(FileUtils.canWrite(f));
            InputStream in = FileUtils.newInputStream(f);
            int len = 0;
            while (in.read() >= 0) {
                len++;
            }
            assertEquals(len, FileUtils.size(f));
            testReadOnly(f);
        }
        String urlJar = "jdbc:h2:zip:" + getBaseDir() + "/fsJar.zip!/fsJar";
        conn = getConnection(urlJar, "sa", "sa");
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        byte[] b2 = rs.getBytes(3);
        String s2 = rs.getString(4);
        assertEquals(2000, b2.length);
        assertEquals(2000, s2.length());
        assertEquals(b1, b2);
        assertEquals(s1, s2);
        assertFalse(rs.next());
        conn.close();
        FileUtils.delete(getBaseDir() + "/fsJar.zip");
    }

    private void testReadOnly(final String f) throws IOException {
        new AssertThrows(IOException.class) {
            @Override
            public void test() throws IOException {
                FileUtils.newOutputStream(f, false);
        }};
        new AssertThrows(DbException.class) {
            @Override
            public void test() {
                FileUtils.move(f, f);
        }};
        new AssertThrows(DbException.class) {
            @Override
            public void test() {
                FileUtils.move(f, f);
        }};
        new AssertThrows(IOException.class) {
            @Override
            public void test() throws IOException {
                FileUtils.createTempFile(f, ".tmp", false, false);
        }};
        final FileChannel channel = FileUtils.open(f, "r");
        new AssertThrows(IOException.class) {
            @Override
            public void test() throws IOException {
                channel.write(ByteBuffer.allocate(1));
        }};
        new AssertThrows(IOException.class) {
            @Override
            public void test() throws IOException {
                channel.truncate(0);
        }};
        assertTrue(null == channel.tryLock());
        channel.force(false);
        channel.close();
    }

    private void testUserHome() {
        String userDir = System.getProperty("user.home").replace('\\', '/');
        assertTrue(FileUtils.toRealPath("~/test").startsWith(userDir));
        assertTrue(FileUtils.toRealPath("file:~/test").startsWith(userDir));
    }

    private void testFileSystem(String fsBase) throws Exception {
        testConcurrent(fsBase);
        testRootExists(fsBase);
        testPositionedReadWrite(fsBase);
        testSetReadOnly(fsBase);
        testParentEventuallyReturnsNull(fsBase);
        testSimple(fsBase);
        testTempFile(fsBase);
        testRandomAccess(fsBase);
    }

    private void testRootExists(String fsBase) {
        String fileName = fsBase + "/testFile";
        FilePath p = FilePath.get(fileName);
        while (p.getParent() != null) {
            p = p.getParent();
        }
        assertTrue(p.exists());
    }

    private void testSetReadOnly(String fsBase) {
        String fileName = fsBase + "/testFile";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        if (FileUtils.createFile(fileName)) {
            FileUtils.setReadOnly(fileName);
            assertFalse(FileUtils.canWrite(fileName));
            FileUtils.delete(fileName);
        }
    }

    private static void testDirectories(String fsBase) {
        final String fileName = fsBase + "/testFile";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        if (FileUtils.createFile(fileName)) {
            new AssertThrows(DbException.class) {
                @Override
                public void test() {
                    FileUtils.createDirectory(fileName);
            }};
            new AssertThrows(DbException.class) {
                @Override
                public void test() {
                    FileUtils.createDirectories(fileName + "/test");
            }};
            FileUtils.delete(fileName);
        }
    }

    private static void testMoveTo(String fsBase) {
        final String fileName = fsBase + "/testFile";
        final String fileName2 = fsBase + "/testFile2";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        if (FileUtils.createFile(fileName)) {
            FileUtils.move(fileName, fileName2);
            FileUtils.createFile(fileName);
            new AssertThrows(DbException.class) {
                @Override
                public void test() {
                    FileUtils.move(fileName2, fileName);
            }};
            FileUtils.delete(fileName);
            FileUtils.delete(fileName2);
            new AssertThrows(DbException.class) {
                @Override
                public void test() {
                    FileUtils.move(fileName, fileName2);
            }};
        }
    }

    private static void testUnsupportedFeatures(String fsBase) throws IOException {
        final String fileName = fsBase + "/testFile";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        if (FileUtils.createFile(fileName)) {
            final FileChannel channel = FileUtils.open(fileName, "rw");
            new AssertThrows(UnsupportedOperationException.class) {
                @Override
                public void test() throws IOException {
                    channel.map(MapMode.PRIVATE, 0, channel.size());
            }};
            new AssertThrows(UnsupportedOperationException.class) {
                @Override
                public void test() throws IOException {
                    channel.read(new ByteBuffer[]{ByteBuffer.allocate(10)}, 0, 0);
            }};
            new AssertThrows(UnsupportedOperationException.class) {
                @Override
                public void test() throws IOException {
                    channel.write(new ByteBuffer[]{ByteBuffer.allocate(10)}, 0, 0);
            }};
            new AssertThrows(UnsupportedOperationException.class) {
                @Override
                public void test() throws IOException {
                    channel.transferFrom(channel, 0, 0);
            }};
            new AssertThrows(UnsupportedOperationException.class) {
                @Override
                public void test() throws IOException {
                    channel.transferTo(0, 0, channel);
            }};
            new AssertThrows(UnsupportedOperationException.class) {
                @Override
                public void test() throws IOException {
                    channel.lock();
            }};
            channel.close();
            FileUtils.delete(fileName);
        }
    }

    private void testParentEventuallyReturnsNull(String fsBase) {
        FilePath p = FilePath.get(fsBase + "/testFile");
        assertTrue(p.getScheme().length() > 0);
        for (int i = 0; i < 100; i++) {
            if (p == null) {
                return;
            }
            p = p.getParent();
        }
        fail("Parent is not null: " + p);
        String path = fsBase + "/testFile";
        for (int i = 0; i < 100; i++) {
            if (path == null) {
                return;
            }
            path = FileUtils.getParent(path);
        }
        fail("Parent is not null: " + path);
    }

    private void testSimple(final String fsBase) throws Exception {
        long time = System.currentTimeMillis();
        for (String s : FileUtils.newDirectoryStream(fsBase)) {
            FileUtils.delete(s);
        }
        FileUtils.createDirectories(fsBase + "/test");
        assertTrue(FileUtils.exists(fsBase));
        FileUtils.delete(fsBase + "/test");
        FileUtils.delete(fsBase + "/test2");
        assertTrue(FileUtils.createFile(fsBase + "/test"));
        List<FilePath> p = FilePath.get(fsBase).newDirectoryStream();
        assertEquals(1, p.size());
        String can = FilePath.get(fsBase + "/test").toRealPath().toString();
        assertEquals(can, p.get(0).toString());
        assertTrue(FileUtils.canWrite(fsBase + "/test"));
        FileChannel channel = FileUtils.open(fsBase + "/test", "rw");
        byte[] buffer = new byte[10000];
        Random random = new Random(1);
        random.nextBytes(buffer);
        channel.write(ByteBuffer.wrap(buffer));
        assertEquals(10000, channel.size());
        channel.position(20000);
        assertEquals(20000, channel.position());
        assertEquals(-1, channel.read(ByteBuffer.wrap(buffer, 0, 1)));
        String path = fsBase + "/test";
        assertEquals("test", FileUtils.getName(path));
        can = FilePath.get(fsBase).toRealPath().toString();
        String can2 = FileUtils.toRealPath(FileUtils.getParent(path));
        assertEquals(can, can2);
        FileLock lock = channel.tryLock();
        if (lock != null) {
            lock.release();
        }
        assertEquals(10000, channel.size());
        channel.close();
        assertEquals(10000, FileUtils.size(fsBase + "/test"));
        channel = FileUtils.open(fsBase + "/test", "r");
        final byte[] test = new byte[10000];
        FileUtils.readFully(channel, ByteBuffer.wrap(test, 0, 10000));
        assertEquals(buffer, test);
        final FileChannel fc = channel;
        new AssertThrows(IOException.class) {
            @Override
            public void test() throws Exception {
                fc.write(ByteBuffer.wrap(test, 0, 10));
            }
        };
        new AssertThrows(NonWritableChannelException.class) {
            @Override
            public void test() throws Exception {
                fc.truncate(10);
            }
        };
        channel.close();
        long lastMod = FileUtils.lastModified(fsBase + "/test");
        if (lastMod < time - 1999) {
            // at most 2 seconds difference
            assertEquals(time, lastMod);
        }
        assertEquals(10000, FileUtils.size(fsBase + "/test"));
        List<String> list = FileUtils.newDirectoryStream(fsBase);
        assertEquals(1, list.size());
        assertTrue(list.get(0).endsWith("test"));
        IOUtils.copyFiles(fsBase + "/test", fsBase + "/test3");
        FileUtils.move(fsBase + "/test3", fsBase + "/test2");
        FileUtils.move(fsBase + "/test2", fsBase + "/test2");
        assertTrue(!FileUtils.exists(fsBase + "/test3"));
        assertTrue(FileUtils.exists(fsBase + "/test2"));
        assertEquals(10000, FileUtils.size(fsBase + "/test2"));
        byte[] buffer2 = new byte[10000];
        InputStream in = FileUtils.newInputStream(fsBase + "/test2");
        int pos = 0;
        while (true) {
            int l = in.read(buffer2, pos, Math.min(10000 - pos, 1000));
            if (l <= 0) {
                break;
            }
            pos += l;
        }
        in.close();
        assertEquals(10000, pos);
        assertEquals(buffer, buffer2);

        assertTrue(FileUtils.tryDelete(fsBase + "/test2"));
        FileUtils.delete(fsBase + "/test");
        if (fsBase.indexOf("memFS:") < 0 && fsBase.indexOf("memLZF:") < 0
            && fsBase.indexOf("nioMemFS:") < 0 && fsBase.indexOf("nioMemLZF:") < 0) {
            FileUtils.createDirectories(fsBase + "/testDir");
            assertTrue(FileUtils.isDirectory(fsBase + "/testDir"));
            if (!fsBase.startsWith("jdbc:")) {
                FileUtils.deleteRecursive(fsBase + "/testDir", false);
                assertTrue(!FileUtils.exists(fsBase + "/testDir"));
            }
        }
    }

    private void testPositionedReadWrite(String fsBase) throws IOException {
        FileUtils.deleteRecursive(fsBase + "/testFile", false);
        FileUtils.delete(fsBase + "/testFile");
        FileUtils.createDirectories(fsBase);
        assertTrue(FileUtils.createFile(fsBase + "/testFile"));
        FileChannel fc = FilePath.get(fsBase + "/testFile").open("rw");
        ByteBuffer buff = ByteBuffer.allocate(4000);
        for (int i = 0; i < 4000; i++) {
            buff.put((byte) i);
        }
        buff.flip();
        fc.write(buff, 96);
        assertEquals(0, fc.position());
        assertEquals(4096, fc.size());
        buff = ByteBuffer.allocate(4000);
        assertEquals(4000, fc.read(buff, 96));
        assertEquals(0, fc.position());
        buff.flip();
        for (int i = 0; i < 4000; i++) {
            assertEquals((byte) i, buff.get());
        }
        buff = ByteBuffer.allocate(0);
        assertTrue(fc.read(buff, 8000) <= 0);
        assertEquals(0, fc.position());
        assertTrue(fc.read(buff, 4000) <= 0);
        assertEquals(0, fc.position());
        assertTrue(fc.read(buff, 2000) <= 0);
        assertEquals(0, fc.position());
        buff = ByteBuffer.allocate(1);
        assertEquals(-1, fc.read(buff, 8000));
        assertEquals(1, fc.read(buff, 4000));
        buff.flip();
        assertEquals(1, fc.read(buff, 2000));
        fc.close();
    }

    private void testRandomAccess(String fsBase) throws Exception {
        testRandomAccess(fsBase, 1);
    }

    private void testRandomAccess(String fsBase, int seed) throws Exception {
        StringBuilder buff = new StringBuilder();
        String s = FileUtils.createTempFile(fsBase + "/tmp", ".tmp", false, false);
        File file = new File(TestBase.BASE_TEST_DIR + "/tmp");
        file.getParentFile().mkdirs();
        file.delete();
        RandomAccessFile ra = new RandomAccessFile(file, "rw");
        FileUtils.delete(s);
        FileChannel f = FileUtils.open(s, "rw");
        assertEquals(s, f.toString());
        assertEquals(-1, f.read(ByteBuffer.wrap(new byte[1])));
        f.force(true);
        Random random = new Random(seed);
        int size = getSize(100, 500);
        try {
            for (int i = 0; i < size; i++) {
                trace("op " + i);
                int pos = random.nextInt(10000);
                switch (random.nextInt(7)) {
                case 0: {
                    pos = (int) Math.min(pos, ra.length());
                    trace("seek " + pos);
                    buff.append("seek " + pos + "\n");
                    f.position(pos);
                    ra.seek(pos);
                    break;
                }
                case 1: {
                    int arrayLen = random.nextInt(1000);
                    int offset = arrayLen / 10;
                    offset = offset == 0 ? 0 : random.nextInt(offset);
                    int len = arrayLen == 0 ? 0 : random.nextInt(arrayLen - offset);
                    byte[] buffer = new byte[arrayLen];
                    ByteBuffer byteBuff = createSlicedBuffer(buffer, offset, len);
                    random.nextBytes(buffer);
                    trace("write " + offset + " len " + len);
                    buff.append("write " + offset + " " + len + "\n");
                    f.write(byteBuff);
                    ra.write(buffer, offset, len);
                    break;
                }
                case 2: {
                    trace("truncate " + pos);
                    buff.append("truncate " + pos + "\n");
                    f.truncate(pos);
                    if (pos < ra.length()) {
                        // truncate is supposed to have no effect if the
                        // position is larger than the current size
                        ra.setLength(pos);
                    }
                    assertEquals(ra.getFilePointer(), f.position());
                    break;
                }
                case 3: {
                    int len = random.nextInt(1000);
                    int offset = random.nextInt(100);
                    int arrayLen = len + offset;
                    len = (int) Math.min(len, ra.length() - ra.getFilePointer());
                    byte[] b1 = new byte[arrayLen];
                    byte[] b2 = new byte[arrayLen];
                    trace("readFully " + len);
                    buff.append("readFully " + len + "\n");
                    ra.readFully(b1, offset, len);
                    ByteBuffer byteBuff = createSlicedBuffer(b2, offset, len);
                    FileUtils.readFully(f, byteBuff);
                    assertEquals(b1, b2);
                    break;
                }
                case 4: {
                    trace("getFilePointer");
                    buff.append("getFilePointer\n");
                    assertEquals(ra.getFilePointer(), f.position());
                    break;
                }
                case 5: {
                    trace("length " + ra.length());
                    buff.append("length " + ra.length() + "\n");
                    assertEquals(ra.length(), f.size());
                    break;
                }
                case 6: {
                    trace("reopen");
                    buff.append("reopen\n");
                    f.close();
                    ra.close();
                    ra = new RandomAccessFile(file, "rw");
                    f = FileUtils.open(s, "rw");
                    assertEquals(ra.length(), f.size());
                    break;
                }
                default:
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Exception: " + e + "\n"+ buff.toString());
        } finally {
            f.close();
            ra.close();
            file.delete();
            FileUtils.delete(s);
        }
    }

    private static ByteBuffer createSlicedBuffer(byte[] buffer, int offset,
            int len) {
        ByteBuffer byteBuff = ByteBuffer.wrap(buffer);
        byteBuff.position(offset);
        // force the arrayOffset to be non-0
        byteBuff = byteBuff.slice();
        byteBuff.limit(len);
        return byteBuff;
    }

    private void testTempFile(String fsBase) throws Exception {
        int len = 10000;
        String s = FileUtils.createTempFile(fsBase + "/tmp", ".tmp", false, false);
        OutputStream out = FileUtils.newOutputStream(s, false);
        byte[] buffer = new byte[len];
        out.write(buffer);
        out.close();
        out = FileUtils.newOutputStream(s, true);
        out.write(1);
        out.close();
        InputStream in = FileUtils.newInputStream(s);
        for (int i = 0; i < len; i++) {
            assertEquals(0, in.read());
        }
        assertEquals(1, in.read());
        assertEquals(-1, in.read());
        in.close();
        out.close();
        FileUtils.delete(s);
    }

    private void testConcurrent(String fsBase) throws Exception {
        String s = FileUtils.createTempFile(fsBase + "/tmp", ".tmp", false, false);
        File file = new File(TestBase.BASE_TEST_DIR + "/tmp");
        file.getParentFile().mkdirs();
        file.delete();
        RandomAccessFile ra = new RandomAccessFile(file, "rw");
        FileUtils.delete(s);
        final FileChannel f = FileUtils.open(s, "rw");
        final int size = getSize(10, 50);
        f.write(ByteBuffer.allocate(size * 64 *  1024));
        Random random = new Random(1);
        System.gc();
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                ByteBuffer byteBuff = ByteBuffer.allocate(16);
                while (!stop) {
                    for (int pos = 0; pos < size; pos++) {
                        byteBuff.clear();
                        f.read(byteBuff, pos * 64 * 1024);
                        byteBuff.position(0);
                        int x = byteBuff.getInt();
                        int y = byteBuff.getInt();
                        assertEquals(x, y);
                        Thread.yield();
                    }
                }
            }
        };
        task.execute();
        int[] data = new int[size];
        try {
            ByteBuffer byteBuff = ByteBuffer.allocate(16);
            int operations = 10000;
            for (int i = 0; i < operations; i++) {
                byteBuff.position(0);
                byteBuff.putInt(i);
                byteBuff.putInt(i);
                byteBuff.flip();
                int pos = random.nextInt(size);
                f.write(byteBuff, pos * 64 * 1024);
                data[pos] = i;
                pos = random.nextInt(size);
                byteBuff.clear();
                f.read(byteBuff, pos * 64 * 1024);
                byteBuff.limit(16);
                byteBuff.position(0);
                int x = byteBuff.getInt();
                int y = byteBuff.getInt();
                assertEquals(x, y);
                assertEquals(data[pos], x);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Exception: " + e);
        } finally {
            task.get();
            f.close();
            ra.close();
            file.delete();
            FileUtils.delete(s);
            System.gc();
        }
    }

}
