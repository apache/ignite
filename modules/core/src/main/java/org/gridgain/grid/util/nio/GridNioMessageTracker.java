/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Message tracker.
 */
public class GridNioMessageTracker implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final GridNioSession ses;

    /** */
    private final int msgQueueLimit;

    /** */
    private final Lock lock = new ReentrantLock();

    /** */
    private final AtomicInteger msgCnt = new AtomicInteger();

    /** */
    private volatile boolean paused;

    /**
     * @param ses Session.
     * @param msgQueueLimit Message queue limit.
     */
    public GridNioMessageTracker(GridNioSession ses, int msgQueueLimit) {
        this.ses = ses;
        this.msgQueueLimit = msgQueueLimit;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        int cnt = msgCnt.decrementAndGet();

        assert cnt >= 0 : "Invalid count: " + cnt;

        if (cnt < msgQueueLimit && paused && lock.tryLock()) {
            try {
                // Double check.
                if (paused && msgCnt.get() < msgQueueLimit) {
                    ses.resumeReads();

                    paused = false;
                }
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     */
    public void onMessageReceived() {
        int cnt = msgCnt.incrementAndGet();

        if (cnt >= msgQueueLimit && !paused) {
            lock.lock();

            try {
                // Double check.
                if (!paused && msgCnt.get() >= msgQueueLimit) {
                    ses.pauseReads();

                    paused = true;
                }
            }
            finally {
                lock.unlock();
            }

            // Need to recheck since message processing threads
            // may have failed to acquire lock.
            if (paused && msgCnt.get() < msgQueueLimit && lock.tryLock()) {
                try {
                    // Double check only for pause, since count is incremented only
                    // in this method and only from one (current) thread.
                    if (paused) {
                        ses.resumeReads();

                        paused = false;
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioMessageTracker.class, this, super.toString());
    }
}
