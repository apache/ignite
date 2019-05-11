package org.apache.ignite.internal.processors.cache.persistence.diagnostic;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.LockStack;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.LockStackSnapshot;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static java.time.Duration.ofMinutes;
import static java.util.Arrays.stream;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.BEFORE_READ_LOCK;

public abstract class PageLockStackTest extends AbstractPageLockTest {
    protected static final int STRUCTURE_ID = 123;

    protected abstract LockStack createLockStackTracer(String name);

    @Test
    public void testOneReadPageLock() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId = 1;
        long page = 2;
        long pageAddr = 3;

        LockStackSnapshot dump;

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId, page);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(Arrays.toString(dump.pageIdLocksStack), isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(pageId, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId, page, pageAddr);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId, page, pageAddr);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testTwoReadPageLock() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long page1 = 2;
        long page2 = 12;
        long pageAddr1 = 3;
        long pageAddr2 = 13;

        LockStackSnapshot dump;

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId1, page1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(pageId1, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId2, page2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(2, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testThreeReadPageLock_1() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long pageId3 = 111;
        long page1 = 2;
        long page2 = 12;
        long page3 = 122;
        long pageAddr1 = 3;
        long pageAddr2 = 13;
        long pageAddr3 = 133;

        LockStackSnapshot dump;

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId1, page1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(pageId1, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId2, page2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(2, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId3, page3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(2, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(pageId3, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(3, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(pageId3, dump.pageIdLocksStack[2]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(2, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.pageIdLocksStack[2]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.pageIdLocksStack[2]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testThreeReadPageLock_2() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long pageId3 = 111;
        long page1 = 2;
        long page2 = 12;
        long page3 = 122;
        long pageAddr1 = 3;
        long pageAddr2 = 13;
        long pageAddr3 = 133;

        LockStackSnapshot dump;

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId1, page1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(pageId1, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId2, page2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(2, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId3, page3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId3, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(2, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId3, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testThreeReadPageLock_3() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long pageId3 = 111;
        long page1 = 2;
        long page2 = 12;
        long page3 = 122;
        long pageAddr1 = 3;
        long pageAddr2 = 13;
        long pageAddr3 = 133;

        LockStackSnapshot dump;

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId1, page1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(pageId1, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId2, page2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(2, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId3, page3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(2, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(pageId3, dump.nextOpPageId);
        Assert.assertEquals(BEFORE_READ_LOCK, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(3, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(pageId3, dump.pageIdLocksStack[2]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(3, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.pageIdLocksStack[1]);
        Assert.assertEquals(pageId3, dump.pageIdLocksStack[2]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.pageIdLocksStack[1]);
        Assert.assertEquals(0, dump.pageIdLocksStack[2]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testUnlockUnexcpected() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId = 1;
        long page = 2;
        long pageAddr = 3;

        LockStackSnapshot dump;

        // Lock stack should be invalid after this operation because we can not unlock page
        // which was not locked.
        lockStack.onReadUnlock(STRUCTURE_ID, pageId, page, pageAddr);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertTrue(lockStack.isInvalid());
        String msg = lockStack.invalidContext().msg;

        Assert.assertTrue(msg, msg.contains("Stack is empty"));

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testUnlockUnexcpectedOnNotEmptyStack() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long page1 = 2;
        long page2 = 12;
        long pageAddr1 = 3;
        long pageAddr2 = 13;

        LockStackSnapshot dump;

        lockStack.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);

        // Lock stack should be invalid after this operation because we can not unlock page
        // which was not locked.
        lockStack.onReadUnlock(STRUCTURE_ID, pageId2, page2, pageAddr2);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertTrue(lockStack.isInvalid());
        String msg = lockStack.invalidContext().msg;

        Assert.assertTrue(msg, msg.contains("Can not find pageId in stack"));

        Assert.assertEquals(1, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testUnlockUnexcpectedOnNotEmptyStackMultiLocks() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId1 = 1;
        long pageId2 = 11;
        long pageId3 = 111;
        long pageId4 = 1111;
        long page1 = 2;
        long page2 = 12;
        long page3 = 122;
        long page4 = 1222;
        long pageAddr1 = 3;
        long pageAddr2 = 13;
        long pageAddr3 = 133;
        long pageAddr4 = 1333;

        LockStackSnapshot dump;

        lockStack.onReadLock(STRUCTURE_ID, pageId1, page1, pageAddr1);
        lockStack.onReadLock(STRUCTURE_ID, pageId2, page2, pageAddr2);
        lockStack.onReadLock(STRUCTURE_ID, pageId3, page3, pageAddr3);

        // Lock stack should be invalid after this operation because we can not unlock page
        // which was not locked.
        lockStack.onReadUnlock(STRUCTURE_ID, pageId4, page4, pageAddr4);

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertTrue(lockStack.isInvalid());
        String msg = lockStack.invalidContext().msg;

        Assert.assertTrue(msg, msg.contains("Can not find pageId in stack"));

        Assert.assertEquals(3, dump.headIdx);
        Assert.assertEquals(pageId1, dump.pageIdLocksStack[0]);
        Assert.assertEquals(pageId2, dump.pageIdLocksStack[1]);
        Assert.assertEquals(pageId3, dump.pageIdLocksStack[2]);
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testStackOverFlow() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId = 1;
        long page = 2;
        long pageAddr = 3;

        LockStackSnapshot dump;

        // Lock stack should be invalid after this operation because we can get lock more that
        // stack capacity, +1 for overflow.
        range(0, lockStack.capacity() + 1).forEach((i) -> {
            lockStack.onReadLock(STRUCTURE_ID, pageId, page, pageAddr);
        });

        System.out.println(lockStack);

        dump = lockStack.dump();

        Assert.assertTrue(lockStack.isInvalid());
        Assert.assertTrue(lockStack.invalidContext().msg.contains("Stack overflow"));

        Assert.assertEquals(lockStack.capacity(), dump.headIdx);
        Assert.assertTrue(!isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testStackOperationAfterInvalid() {
        LockStack lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId = 1;
        long page = 2;
        long pageAddr = 3;

        LockStackSnapshot dump;

        // Lock stack should be invalid after this operation because we can not unlock page
        // which was not locked.
        lockStack.onReadUnlock(STRUCTURE_ID, pageId, page, pageAddr);

        dump = lockStack.dump();

        Assert.assertTrue(lockStack.isInvalid());
        String msg = lockStack.invalidContext().msg;

        Assert.assertTrue(msg, msg.contains("Stack is empty"));

        System.out.println(lockStack.invalidContext());

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onBeforeReadLock(STRUCTURE_ID, pageId, page);

        Assert.assertTrue(lockStack.isInvalid());
        msg = lockStack.invalidContext().msg;

        Assert.assertTrue(msg, msg.contains("Stack is empty"));

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadLock(STRUCTURE_ID, pageId, page, pageAddr);

        Assert.assertTrue(lockStack.isInvalid());
        msg = lockStack.invalidContext().msg;

        Assert.assertTrue(msg, msg.contains("Stack is empty"));

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);

        lockStack.onReadUnlock(STRUCTURE_ID, pageId, page, pageAddr);

        Assert.assertTrue(lockStack.isInvalid());
        msg = lockStack.invalidContext().msg;

        Assert.assertTrue(msg, msg.contains("Stack is empty"));

        Assert.assertEquals(0, dump.headIdx);
        Assert.assertTrue(isEmptyArray(dump.pageIdLocksStack));
        Assert.assertEquals(0, dump.nextOpPageId);
        Assert.assertEquals(0, dump.nextOp);
    }

    @Test
    public void testThreadDump() throws IgniteCheckedException {
        PageLockTracker<LockStackSnapshot> lockStack = createLockStackTracer(Thread.currentThread().getName());

        long pageId = 1;
        long page = 2;
        long pageAddr = 3;

        int cntDumps = 5_000;

        AtomicBoolean done = new AtomicBoolean();

        int maxWaitTime = 500;

        int maxdeep = 16;

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            while (!done.get()) {
                int deep = nextRandomWaitTimeout(maxdeep);

                randomLocks(deep, () -> {
                    awaitRandom(100);

                    lockStack.onBeforeReadLock(STRUCTURE_ID, pageId, page);

                    awaitRandom(100);

                    lockStack.onReadLock(STRUCTURE_ID, pageId, page, pageAddr);
                });

                try {
                    awaitRandom(maxWaitTime);
                }
                finally {
                    randomLocks(deep, () -> {
                        lockStack.onReadUnlock(STRUCTURE_ID, pageId, page, pageAddr);
                    });
                }
            }
        });

        long totalExecutionTime = 0L;

        for (int i = 0; i < cntDumps; i++) {
            awaitRandom(50);

            long time = System.nanoTime();

            LockStackSnapshot dump = lockStack.dump();

            long dumpTime = System.nanoTime() - time;

            if (dump.nextOp != 0)
                Assert.assertTrue(dump.nextOpPageId != 0);

            Assert.assertTrue(dump.time != 0);
            Assert.assertNotNull(dump.name);

            if (dump.headIdx > 0) {
                for (int j = 0; j < dump.headIdx; j++)
                    Assert.assertTrue(String.valueOf(dump.headIdx), dump.pageIdLocksStack[j] != 0);
            }

            Assert.assertNotNull(dump);

            totalExecutionTime += dumpTime;

            Assert.assertTrue(dumpTime <= ofMinutes((long)(maxWaitTime + (maxWaitTime * 0.1))).toNanos());

            if (i != 0 && i % 100 == 0)
                System.out.println(">>> Dump:" + i);
        }

        done.set(true);

        f.get();

        System.out.println(">>> Avarage time dump creation:" + (totalExecutionTime / cntDumps) + " ns");
    }

}