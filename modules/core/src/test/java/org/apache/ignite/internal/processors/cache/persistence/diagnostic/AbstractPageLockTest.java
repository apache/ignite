package org.apache.ignite.internal.processors.cache.persistence.diagnostic;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.LockLogSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Arrays.stream;
import static org.junit.Assert.assertEquals;

public abstract class AbstractPageLockTest {
    protected void randomLocks(int deep, Runnable r) {
        for (int i = 0; i < deep; i++)
            r.run();
    }

    protected void awaitRandom(int bound) {
        try {
            U.sleep(nextRandomWaitTimeout(bound));
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }
    }

    protected int nextRandomWaitTimeout(int bound) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        return rnd.nextInt(bound);
    }

    protected void checkNextOp(LockLogSnapshot lockLog, long nextOpPageId, long nextOp, int nextOpStructureId) {
        assertEquals(nextOpStructureId, lockLog.nextOpStructureId);
        assertEquals(nextOp, lockLog.nextOp);
        assertEquals(nextOpPageId, lockLog.nextOpPageId);
    }

    protected boolean isEmptyArray(long[] arr) {
        return stream(arr).filter(value -> value != 0).count() == 0;
    }
}
