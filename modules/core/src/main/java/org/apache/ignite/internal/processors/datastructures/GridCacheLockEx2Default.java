package org.apache.ignite.internal.processors.datastructures;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;

public abstract class GridCacheLockEx2Default implements GridCacheLockEx2 {
    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void needCheckNotRemoved() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Condition newCondition() {
        throw new UnsupportedOperationException("IgniteLock does not allow creation of nameless conditions. ");
    }

    /** {@inheritDoc} */
    @Override public IgniteCondition getOrCreateCondition(String name) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getHoldCount() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isHeldByCurrentThread() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThreads() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThread(Thread thread) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasWaiters(IgniteCondition condition) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getWaitQueueLength(IgniteCondition condition) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean isFailoverSafe() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isBroken() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** */
    public static class UpdateListener implements GridLocalEventListener {
        /** */
        private final ReentrantLock lock = new ReentrantLock();

        /** */
        private final Condition latch = lock.newCondition();

        /** */
        private int count = 0;

        /** */
        @Override public void onEvent(Event evt) {
            release();
        }

        /** */
        public void release() {
            lock.lock();
            try {
                count++;

                latch.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /** */
        IgniteException exception;

        /** */
        public void fail(IgniteException exception) {
            lock.lock();
            try {
                count++;
                this.exception = exception;

                latch.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /** */
        public void await() throws InterruptedException, IgniteException {
            lock.lock();
            try {
                if (count-- <= 0) {
                    latch.await();
                }
                if (exception != null) {
                    throw exception;
                }
            }
            finally {
                exception = null;
                lock.unlock();
            }
        }

        /** */
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException, IgniteException {
            lock.lock();
            try {
                boolean flag = true;
                if (count-- <= 0) {
                    flag = latch.await(timeout, unit);
                }

                if (flag && exception!=null)
                    throw exception;

                return flag;
            }
            finally {
                exception = null;
                lock.unlock();
            }
        }
    }
}
