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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.AbstractPageLockTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.PageLockLogSnapshot.LogEntry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.junit.Assert;
import org.junit.Test;

import static java.lang.System.out;
import static java.time.Duration.ofMinutes;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.DEFAULT_CAPACITY;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.BEFORE_READ_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.READ_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.READ_UNLOCK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public abstract class PageLockLogTest extends AbstractPageLockTest {
    /** */
    protected static final int STRUCTURE_ID = 123;

    /** */
    protected abstract LockLog createLogStackTracer(String name);

    /** */
    private void checkLogEntry(LogEntry logEntry, long pageId, int operation, int structureId, int holdedLocks) {
        assertEquals(pageId, logEntry.pageId);
        assertEquals(operation, logEntry.operation);
        assertEquals(structureId, logEntry.structureId);
        assertEquals(holdedLocks, logEntry.holdedLocks);
    }

    /** */
    @Test
    public void testOneReadPageLock() {
        LockLog lockLog = createLogStackTracer(Thread.currentThread().getName());

        long pageId = 1;
        long page = 2;
        long pageAddr = 3;

        PageLockLogSnapshot logDump;

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId, page);

        logDump = lockLog.dump();

        assertEquals(0, logDump.headIdx);
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, pageId, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId, page, pageAddr);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId, page, pageAddr);

        logDump = lockLog.dump();

        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, 0, 0, 0);
    }

    /** */
    @Test
    public void testTwoReadPageLock() {
        LockLog lockLog = createLogStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long page1 = 2;
        long page2 = 12;
        long pageAddr1 = 3;
        long pageAddr2 = 13;

        PageLockLogSnapshot logDump;

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId1, page1);

        logDump = lockLog.dump();

        assertEquals(0, logDump.headIdx);
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, pageId1, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId2, page2);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, pageId2, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        logDump = lockLog.dump();

        assertEquals(2, logDump.locklog.size());

        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        logDump = lockLog.dump();

        assertEquals(3, logDump.locklog.size());

        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId2, READ_UNLOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        logDump = lockLog.dump();
        
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, 0, 0, 0);
    }

    /** */
    @Test
    public void testThreeReadPageLock_1() {
        LockLog lockLog = createLogStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long pageId3 = 111;
        long page1 = 2;
        long page2 = 12;
        long page3 = 122;
        long pageAddr1 = 3;
        long pageAddr2 = 13;
        long pageAddr3 = 133;

        PageLockLogSnapshot logDump;

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId1, page1);

        logDump = lockLog.dump();

        assertEquals(0, logDump.headIdx);
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, pageId1, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId2, page2);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, pageId2, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        logDump = lockLog.dump();

        assertEquals(2, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId3, page3);

        logDump = lockLog.dump();

        assertEquals(2, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, pageId3, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        logDump = lockLog.dump();

        assertEquals(3, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId3, READ_LOCK, STRUCTURE_ID, 3);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        logDump = lockLog.dump();

        assertEquals(4, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId3, READ_LOCK, STRUCTURE_ID, 3);
        checkLogEntry(logDump.locklog.get(3), pageId3, READ_UNLOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        logDump = lockLog.dump();

        assertEquals(5, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId3, READ_LOCK, STRUCTURE_ID, 3);
        checkLogEntry(logDump.locklog.get(3), pageId3, READ_UNLOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(4), pageId2, READ_UNLOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        logDump = lockLog.dump();

        assertEquals(0, logDump.headIdx);
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, 0, 0, 0);
    }

    /** */
    @Test
    public void testThreeReadPageLock_2() {
        LockLog lockLog = createLogStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long pageId3 = 111;
        long page1 = 2;
        long page2 = 12;
        long page3 = 122;
        long pageAddr1 = 3;
        long pageAddr2 = 13;
        long pageAddr3 = 133;

        PageLockLogSnapshot logDump;

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId1, page1);

        logDump = lockLog.dump();

        assertEquals(0, logDump.headIdx);
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, pageId1, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId2, page2);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, pageId2, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        logDump = lockLog.dump();

        assertEquals(2, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        logDump = lockLog.dump();

        assertEquals(3, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId2, READ_UNLOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId3, page3);

        logDump = lockLog.dump();

        assertEquals(3, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId2, READ_UNLOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, pageId3, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        logDump = lockLog.dump();

        assertEquals(4, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId2, READ_UNLOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(3), pageId3, READ_LOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        logDump = lockLog.dump();

        assertEquals(5, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId2, READ_UNLOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(3), pageId3, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(4), pageId3, READ_UNLOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        logDump = lockLog.dump();

        assertEquals(0, logDump.headIdx);
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, 0, 0, 0);
    }

    /** */
    @Test
    public void testThreeReadPageLock_3() {
        LockLog lockLog = createLogStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long pageId3 = 111;
        long page1 = 2;
        long page2 = 12;
        long page3 = 122;
        long pageAddr1 = 3;
        long pageAddr2 = 13;
        long pageAddr3 = 133;

        PageLockLogSnapshot logDump;

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId1, page1);

        logDump = lockLog.dump();

        assertEquals(0, logDump.headIdx);
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, pageId1, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId2, page2);

        logDump = lockLog.dump();

        assertEquals(1, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, pageId2, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        logDump = lockLog.dump();

        assertEquals(2, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, 0, 0, 0);

        lockLog.onBeforeReadLock(STRUCTURE_ID, pageId3, page3);

        logDump = lockLog.dump();

        assertEquals(2, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, pageId3, BEFORE_READ_LOCK, STRUCTURE_ID);

        lockLog.onReadLock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        logDump = lockLog.dump();

        assertEquals(3, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId3, READ_LOCK, STRUCTURE_ID, 3);
        checkNextOp(logDump, 0,0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        logDump = lockLog.dump();

        assertEquals(4, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId3, READ_LOCK, STRUCTURE_ID, 3);
        checkLogEntry(logDump.locklog.get(3), pageId2, READ_UNLOCK, STRUCTURE_ID, 2);
        checkNextOp(logDump, 0,0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        logDump = lockLog.dump();

        assertEquals(5, logDump.headIdx);
        checkLogEntry(logDump.locklog.get(0), pageId1, READ_LOCK, STRUCTURE_ID, 1);
        checkLogEntry(logDump.locklog.get(1), pageId2, READ_LOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(2), pageId3, READ_LOCK, STRUCTURE_ID, 3);
        checkLogEntry(logDump.locklog.get(3), pageId2, READ_UNLOCK, STRUCTURE_ID, 2);
        checkLogEntry(logDump.locklog.get(4), pageId3, READ_UNLOCK, STRUCTURE_ID, 1);
        checkNextOp(logDump, 0,0, 0);

        lockLog.onReadUnlock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        logDump = lockLog.dump();

        assertEquals(0, logDump.headIdx);
        assertTrue(logDump.locklog.isEmpty());
        checkNextOp(logDump, 0, 0, 0);
    }

    /** */
    @Test
    public void testLogOverFlow() {
        LockLog lockLog = createLogStackTracer(Thread.currentThread().getName());

        long pageId = 1;
        long page = 2;
        long pageAddr = 3;

        PageLockLogSnapshot log;

        // Lock log should be invalid after this operation because we can get lock more that
        // log capacity, +1 for overflow.
        range(0, DEFAULT_CAPACITY + 1).forEach((i) -> {
            lockLog.onReadLock(STRUCTURE_ID, pageId, page, pageAddr);
        });

        log = lockLog.dump();

        Assert.assertTrue(lockLog.isInvalid());

        String msg = lockLog.invalidContext().msg;

        Assert.assertTrue(msg, msg.contains("Log overflow"));
    }

    /** */
    @Test
    public void testThreadlog() throws IgniteCheckedException {
        LockLog lockLog = createLogStackTracer(Thread.currentThread().getName());

        long pageId = 1;
        long page = 2;
        long pageAddr = 3;

        int cntlogs = SF.applyLB(5_000, 1_000);

        AtomicBoolean done = new AtomicBoolean();

        int maxWaitTime = 500;

        int maxdeep = 16;

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            while (!done.get()) {
                int iter = nextRandomWaitTimeout(maxdeep);

                doRunnable(iter, () -> {
                    awaitRandom(100);

                    lockLog.onBeforeReadLock(STRUCTURE_ID, pageId, page);

                    awaitRandom(100);

                    lockLog.onReadLock(STRUCTURE_ID, pageId, page, pageAddr);
                });

                try {
                    awaitRandom(maxWaitTime);
                }
                finally {
                    doRunnable(iter, () -> {
                        lockLog.onReadUnlock(STRUCTURE_ID, pageId, page, pageAddr);
                    });
                }
            }
        });

        long totalExecutionTime = 0L;

        for (int i = 0; i < cntlogs; i++) {
            awaitRandom(50);

            long time = System.nanoTime();

            PageLockLogSnapshot logDump = lockLog.dump();

            long logTime = System.nanoTime() - time;

            if (logDump.nextOp != 0)
                assertTrue(logDump.nextOpPageId != 0);

            assertTrue(logDump.time != 0);
            Assert.assertNotNull(logDump.name);

            if (logDump.headIdx > 0) {
                for (int j = 0; j < logDump.headIdx; j++)
                    Assert.assertNotNull(String.valueOf(logDump.headIdx), logDump.locklog.get(j));
            }

            Assert.assertNotNull(logDump);

            totalExecutionTime += logTime;

            assertTrue(logTime <= ofMinutes((long)(maxWaitTime + (maxWaitTime * 0.1))).toNanos());

            if (i != 0 && i % 100 == 0)
                out.println(">>> log:" + i);
        }

        done.set(true);

        f.get();

        out.println(">>> Avarage time log creation:" + (totalExecutionTime / cntlogs) + " ns");
    }
}
