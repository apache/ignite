package org.apache.ignite.internal.util;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Ilya Lantukh
 */
public class StripedCompositeReadWriteLock implements ReadWriteLock {

    private final PaddedReentrantReadWriteLock[] locks;

    private final CompositeWriteLock compositeWriteLock;

    public StripedCompositeReadWriteLock(int concurrencyLevel) {
        locks = new PaddedReentrantReadWriteLock[concurrencyLevel];

        for (int i = 0; i < concurrencyLevel; i++)
            locks[i] = new PaddedReentrantReadWriteLock();

        compositeWriteLock = new CompositeWriteLock();
    }

    @NotNull @Override public Lock readLock() {
        int idx = (int)Thread.currentThread().getId() % locks.length;
        return locks[idx].readLock();
    }

    @NotNull @Override public Lock writeLock() {
        return compositeWriteLock;
    }

    private static class PaddedReentrantReadWriteLock extends ReentrantReadWriteLock {

        private static final long serialVersionUID = 0L;

        long p0, p1, p2, p3, p4, p5, p6, p7;
    }

    private class CompositeWriteLock implements Lock {

        @Override public void lock() {
            int i = 0;
            try {
                for (; i < locks.length; i++)
                    locks[i].writeLock().lock();
            }
            catch (Throwable e) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();

                throw e;
            }
        }

        @Override public void lockInterruptibly() throws InterruptedException {
            int i = 0;
            try {
                for (; i < locks.length; i++)
                    locks[i].writeLock().lockInterruptibly();
            }
            catch (Throwable e) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();

                throw e;
            }
        }

        @Override public boolean tryLock() {
            int i = 0;

            boolean unlock = false;

            try {
                for (; i < locks.length; i++)
                    if (!locks[i].writeLock().tryLock()) {
                        unlock = true;
                        break;
                    }
            }
            catch (Throwable e) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();

                throw e;
            }

            if (unlock) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();
                return false;
            }

            return true;
        }

        @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            throw new RuntimeException("Not supported");
        }

        @Override public void unlock() {
            for (int i = locks.length - 1; i >= 0; i--)
                locks[i].writeLock().unlock();
        }

        @NotNull @Override public Condition newCondition() {
            throw new RuntimeException("Not supported");
        }
    }
}
