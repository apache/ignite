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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.retryTopologySafe;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache semaphore implementation based on AbstractQueuedSynchronizer. Current implementation supports only unfair and
 * locally fair modes. When fairness set false, this class makes no guarantees about the order in which threads acquire
 * permits. When fairness is set true, the semaphore only guarantees that local threads invoking any of the acquire
 * methods are selected to obtain permits in the order in which their invocation of those methods was processed (FIFO).
 */
public final class GridCacheSemaphoreImpl implements GridCacheSemaphoreEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return F.t2();
            }
        };

    /** Logger. */
    private IgniteLogger log;

    /** Semaphore name. */
    private String name;

    /** Removed flag. */
    private volatile boolean rmvd;

    /** Semaphore key. */
    private GridCacheInternalKey key;

    /** Semaphore projection. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheSemaphoreState> semaphoreView;

    /** Cache context. */
    private GridCacheContext ctx;

    /** Fairness flag. */
    private boolean isFair;

    /** Initial count. */
    private transient final int initCnt;

    /** Initialization guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Initialization latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Internal synchronization object. */
    private Sync sync;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheSemaphoreImpl() {
        // No-op.
        initCnt = 0;
    }

    /**
     * Synchronization implementation for semaphore.
     * Uses AQS state to represent permits. Subclassed into fair and nonfair versions.
     */
    abstract class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 1192457210091910933L;

        /** Thread map. */
        protected final ConcurrentMap<Thread, Integer> threadMap;

        /** Total number of threads currently waiting on this semaphore. */
        protected int totalWaiters;

        protected Sync(int permits) {
            setState(permits);
            threadMap = new ConcurrentHashMap<>();
        }

        /**
         * Sets the estimate of the total current number of threads waiting on this semaphore. This method should only
         * be called in {@linkplain GridCacheSemaphoreImpl#onUpdate(GridCacheSemaphoreState)}.
         *
         * @param waiters Thread count.
         */
        protected synchronized void setWaiters(int waiters) {
            totalWaiters = waiters;
        }

        /**
         * Gets the number of waiting threads.
         *
         * @return Number of thread waiting at this semaphore.
         */
        public int getWaiters() {
            return totalWaiters;
        }

        /**
         * Sets the number of permits currently available on this semaphore. This method should only be used in
         * {@linkplain GridCacheSemaphoreImpl#onUpdate(GridCacheSemaphoreState)}.
         *
         * @param permits Number of permits available at this semaphore.
         */
        final synchronized void setPermits(int permits) {
            setState(permits);
        }

        /**
         * Gets the number of permissions currently available.
         *
         * @return Number of permits available at this semaphore.
         */
        final int getPermits() {
            return getState();
        }

        /**
         * This method is used by the AQS to test if the current thread should block or not.
         *
         * @param acquires Number of permits to acquire.
         * @return Negative number if thread should block, positive if thread successfully acquires permits.
         */
        final int nonfairTryAcquireShared(int acquires) {
            for (; ; ) {
                int available = getState();

                int remaining = available - acquires;

                if (remaining < 0 || compareAndSetGlobalState(available, remaining)) {
                    if (remaining < 0) {
                        if (!threadMap.containsKey(Thread.currentThread()))
                            getAndIncWaitingCount();
                    }

                    return remaining;
                }
            }
        }

        /** {@inheritDoc} */
        @Override protected final boolean tryReleaseShared(int releases) {
            // Check if some other node updated the state.
            // This method is called with release==0 only when trying to wake through update.
            if (releases == 0)
                return true;

            for (; ; ) {
                int current = getState();

                int next = current + releases;

                if (next < current) // overflow
                    throw new Error("Maximum permit count exceeded");

                if (compareAndSetGlobalState(current, next))
                    return true;
            }
        }

        /**
         * This method is used internally to implement {@linkplain GridCacheSemaphoreImpl#drainPermits()}.
         *
         * @return Number of permits to drain.
         */
        final int drainPermits() {
            for (; ; ) {
                int current = getState();

                if (current == 0 || compareAndSetGlobalState(current, 0))
                    return current;
            }
        }

        /**
         * This method is used when thread blocks on this semaphore to synchronize the waiting thread counter across all
         * nodes.
         */
        protected void getAndIncWaitingCount() {
            try {
                CU.outTx(
                    retryTopologySafe(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            try (
                                IgniteInternalTx tx = CU.txStartInternal(ctx, semaphoreView, PESSIMISTIC, REPEATABLE_READ)
                            ) {
                                GridCacheSemaphoreState val = semaphoreView.get(key);

                                if (val == null)
                                    throw new IgniteCheckedException("Failed to find semaphore with given name: " + name);

                                int waiting = val.getWaiters();

                                sync.threadMap.put(Thread.currentThread(), waiting);

                                waiting++;

                                val.setWaiters(waiting);

                                semaphoreView.put(key, val);

                                tx.commit();

                                return true;
                            }
                            catch (Error | Exception e) {
                                U.error(log, "Failed to compare and set: " + this, e);

                                throw e;
                            }
                        }
                    }),
                    ctx
                );
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /**
         * This method is used for synchronizing the semaphore state across all nodes.
         */
        protected boolean compareAndSetGlobalState(final int expVal, final int newVal) {
            try {
                return CU.outTx(
                    retryTopologySafe(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            try (
                                IgniteInternalTx tx = CU.txStartInternal(ctx, semaphoreView,
                                    PESSIMISTIC, REPEATABLE_READ)
                            ) {
                                GridCacheSemaphoreState val = semaphoreView.get(key);

                                if (val == null)
                                    throw new IgniteCheckedException("Failed to find semaphore with given name: " +
                                        name);

                                boolean retVal = val.getCount() == expVal;

                                if (retVal) {
                                    // If current thread is queued, than this call is
                                    // the call that is going to be unblocked.
                                    if (sync.isQueued(Thread.currentThread())) {
                                        int waiting = val.getWaiters() - 1;

                                        val.setWaiters(waiting);

                                        sync.threadMap.remove(Thread.currentThread());
                                    }

                                    val.setCount(newVal);

                                    semaphoreView.put(key, val);

                                    tx.commit();
                                }

                                return retVal;
                            }
                            catch (Error | Exception e) {
                                U.error(log, "Failed to compare and set: " + this, e);

                                throw e;
                            }
                        }
                    }),
                    ctx
                );
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }

    /**
     * NonFair version.
     */
    final class NonfairSync extends Sync {
        private static final long serialVersionUID = 7983135489326435495L;

        NonfairSync(int permits) {
            super(permits);
        }

        /** {@inheritDoc} */
        @Override protected int tryAcquireShared(int acquires) {
            return nonfairTryAcquireShared(acquires);
        }
    }

    /**
     * Fair version
     */
    final class FairSync extends Sync {
        private static final long serialVersionUID = 3468129658421667L;

        FairSync(int permits) {
            super(permits);
        }

        /** {@inheritDoc} */
        @Override protected int tryAcquireShared(int acquires) {
            for (; ; ) {
                if (hasQueuedPredecessors())
                    return -1;

                int available = getState();

                int remaining = available - acquires;

                if (remaining < 0 || compareAndSetGlobalState(available, remaining)) {
                    if (remaining < 0) {
                        if (!threadMap.containsKey(Thread.currentThread()))
                            getAndIncWaitingCount();
                    }
                    return remaining;
                }
            }
        }

    }

    /**
     * Constructor.
     *
     * @param name Semaphore name.
     * @param initCnt Initial count.
     * @param key Semaphore key.
     * @param semaphoreView Semaphore projection.
     * @param ctx Cache context.
     */
    public GridCacheSemaphoreImpl(
        String name,
        int initCnt,
        boolean fair,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheSemaphoreState> semaphoreView,
        GridCacheContext ctx
    ) {
        assert name != null;
        assert key != null;
        assert semaphoreView != null;
        assert ctx != null;

        this.name = name;
        this.initCnt = initCnt;
        this.key = key;
        this.semaphoreView = semaphoreView;
        this.ctx = ctx;
        this.isFair = fair;

        log = ctx.logger(getClass());
    }

    /**
     * @throws IgniteCheckedException If operation failed.
     */
    private void initializeSemaphore() throws IgniteCheckedException {
        if (!initGuard.get() && initGuard.compareAndSet(false, true)) {
            try {
                sync = CU.outTx(
                    retryTopologySafe(new Callable<Sync>() {
                        @Override public Sync call() throws Exception {
                            try (IgniteInternalTx tx = CU.txStartInternal(ctx, semaphoreView, PESSIMISTIC, REPEATABLE_READ)) {
                                GridCacheSemaphoreState val = semaphoreView.get(key);

                                if (val == null) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to find semaphore with given name: " + name);

                                    return null;
                                }

                                final int count = val.getCount();

                                tx.commit();

                                return val.isFair() ? new FairSync(count) : new NonfairSync(count);
                            }
                        }
                    }),
                    ctx
                );

                if (log.isDebugEnabled())
                    log.debug("Initialized internal sync structure: " + sync);
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            U.await(initLatch);

            if (sync == null)
                throw new IgniteCheckedException("Internal latch has not been properly initialized.");
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public GridCacheInternalKey key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        return rmvd = true;
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(GridCacheSemaphoreState val) {
        if (sync == null)
            return;

        // Update permission count.
        sync.setPermits(val.getCount());

        // Update waiters count.
        sync.setWaiters(val.getWaiters());

        // Try to notify any waiting threads.
        sync.releaseShared(0);
    }

    /** {@inheritDoc} */
    @Override public void needCheckNotRemoved() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void acquire() throws IgniteInterruptedException {
        acquire(1);
    }

    /** {@inheritDoc} */
    @Override public void acquire(int permits) throws IgniteInterruptedException {
        A.ensure(permits >= 0, "Number of permits must be non-negative.");

        try {
            initializeSemaphore();

            sync.acquireSharedInterruptibly(permits);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acquireUninterruptibly() {
        try {
            initializeSemaphore();

            sync.acquireShared(1);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acquireUninterruptibly(int permits) {
        A.ensure(permits >= 0, "Number of permits must be non-negative.");

        try {
            initializeSemaphore();

            sync.acquireShared(permits);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int availablePermits() {
        int ret;
        try {
            initializeSemaphore();

            ret = CU.outTx(
                retryTopologySafe(new Callable<Integer>() {
                    @Override public Integer call() throws Exception {
                        try (
                            IgniteInternalTx tx = CU.txStartInternal(ctx, semaphoreView, PESSIMISTIC, REPEATABLE_READ)
                        ) {
                            GridCacheSemaphoreState val = semaphoreView.get(key);

                            if (val == null)
                                throw new IgniteException("Failed to find semaphore with given name: " + name);

                            int count = val.getCount();

                            tx.rollback();

                            return count;
                        }
                    }
                }),
                ctx
            );
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override public int drainPermits() {
        try {
            initializeSemaphore();

            return sync.drainPermits();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryAcquire() {
        try {
            initializeSemaphore();

            return sync.nonfairTryAcquireShared(1) >= 0;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryAcquire(long timeout, TimeUnit unit) throws IgniteException {
        try {
            initializeSemaphore();

            return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void release() {
        release(1);
    }

    /** {@inheritDoc} */
    @Override public void release(int permits) {
        A.ensure(permits >= 0, "Number of permits must be non-negative.");

        try {
            initializeSemaphore();

            sync.releaseShared(permits);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryAcquire(int permits) {
        A.ensure(permits >= 0, "Number of permits must be non-negative.");

        try {
            initializeSemaphore();

            return sync.nonfairTryAcquireShared(permits) >= 0;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws IgniteInterruptedException {
        A.ensure(permits >= 0, "Number of permits must be non-negative.");
        try {
            initializeSemaphore();

            return sync.tryAcquireSharedNanos(permits, unit.toNanos(timeout));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isFair() {
        return isFair;
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThreads() {
        try {
            initializeSemaphore();

            return sync.getWaiters() != 0;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int getQueueLength() {
        try {
            initializeSemaphore();

            return sync.getWaiters();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx.kernalContext());
        out.writeUTF(name);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<GridKernalContext, String> t = stash.get();

        t.set1((GridKernalContext)in.readObject());
        t.set2(in.readUTF());
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (!rmvd) {
            try {
                ctx.kernalContext().dataStructures().removeSemaphore(name);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSemaphoreImpl.class, this);
    }
}
