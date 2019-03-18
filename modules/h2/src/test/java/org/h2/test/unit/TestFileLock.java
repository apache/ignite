/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import java.sql.Connection;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.TraceSystem;
import org.h2.store.FileLock;
import org.h2.store.FileLockMethod;
import org.h2.test.TestBase;

/**
 * Tests the database file locking facility. Both lock files and sockets locking
 * is tested.
 */
public class TestFileLock extends TestBase implements Runnable {

    private static volatile int locks;
    private static volatile boolean stop;
    private TestBase base;
    private int wait;
    private boolean allowSockets;

    public TestFileLock() {
        // nothing to do
    }

    TestFileLock(TestBase base, boolean allowSockets) {
        this.base = base;
        this.allowSockets = allowSockets;
    }

    private String getFile() {
        return getBaseDir() + "/test.lock";
    }

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        if (!getFile().startsWith(TestBase.BASE_TEST_DIR)) {
            return;
        }
        testFsFileLock();
        testFutureModificationDate();
        testSimple();
        test(false);
        test(true);
    }

    private void testFsFileLock() throws Exception {
        deleteDb("fileLock");
        String url = "jdbc:h2:" + getBaseDir() +
                "/fileLock;FILE_LOCK=FS;OPEN_NEW=TRUE";
        Connection conn = getConnection(url);
        assertThrows(ErrorCode.DATABASE_ALREADY_OPEN_1, this)
                .getConnection(url);
        conn.close();
    }

    private void testFutureModificationDate() throws Exception {
        File f = new File(getFile());
        f.delete();
        f.createNewFile();
        f.setLastModified(System.currentTimeMillis() + 10000);
        FileLock lock = new FileLock(new TraceSystem(null), getFile(),
                Constants.LOCK_SLEEP);
        lock.lock(FileLockMethod.FILE);
        lock.unlock();
    }

    private void testSimple() {
        FileLock lock1 = new FileLock(new TraceSystem(null), getFile(),
                Constants.LOCK_SLEEP);
        FileLock lock2 = new FileLock(new TraceSystem(null), getFile(),
                Constants.LOCK_SLEEP);
        lock1.lock(FileLockMethod.FILE);
        createClassProxy(FileLock.class);
        assertThrows(ErrorCode.DATABASE_ALREADY_OPEN_1, lock2).lock(
                FileLockMethod.FILE);
        lock1.unlock();
        lock2 = new FileLock(new TraceSystem(null), getFile(),
                Constants.LOCK_SLEEP);
        lock2.lock(FileLockMethod.FILE);
        lock2.unlock();
    }

    private void test(boolean allowSocketsLock) throws Exception {
        int threadCount = getSize(3, 5);
        wait = getSize(20, 200);
        Thread[] threads = new Thread[threadCount];
        new File(getFile()).delete();
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(new TestFileLock(this, allowSocketsLock));
            threads[i].start();
            Thread.sleep(wait + (int) (Math.random() * wait));
        }
        trace("wait");
        Thread.sleep(500);
        stop = true;
        trace("STOP file");
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        assertEquals(0, locks);
    }

    @Override
    public void run() {
        FileLock lock = null;
        while (!stop) {
            lock = new FileLock(new TraceSystem(null), getFile(), 100);
            try {
                lock.lock(allowSockets ? FileLockMethod.SOCKET
                        : FileLockMethod.FILE);
                base.trace(lock + " locked");
                locks++;
                if (locks > 1) {
                    System.err.println("ERROR! LOCKS=" + locks + " sockets=" +
                            allowSockets);
                    stop = true;
                }
                Thread.sleep(wait + (int) (Math.random() * wait));
                locks--;
                base.trace(lock + " unlock");
                lock.unlock();
                if (locks < 0) {
                    System.err.println("ERROR! LOCKS=" + locks);
                    stop = true;
                }
            } catch (Exception e) {
                // log(id+" cannot lock: " + e);
            }
            try {
                Thread.sleep(wait + (int) (Math.random() * wait));
            } catch (InterruptedException e1) {
                // ignore
            }
        }
        if (lock != null) {
            lock.unlock();
        }
    }

}
