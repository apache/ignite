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

import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;

/**
 * Cache reentrant lock implementation based on AbstractQueuedSynchronizer.
 */
public final class GridCacheLockImpl2 implements GridCacheLockEx2 {
    /** Logger. */
    private final IgniteLogger log;

    /** Reentrant lock name. */
    private final String name;

    /** Reentrant lock key. */
    private GridCacheInternalKey key;

    /** Reentrant lock projection. */
    private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2> lockView;

    /** Cache context. */
    private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2> ctx;

    /** Internal synchronization object. */
    private final LocalSync sync;

    /** */
    private final boolean fair;

    /**
     * Constructor.
     *
     * @param name Reentrant lock name.
     * @param key Reentrant lock key.
     * @param lockView Reentrant lock projection.
     */
    @SuppressWarnings("unchecked")
    public GridCacheLockImpl2(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2> lockView, boolean fair) {

        assert name != null;
        assert key != null;
        assert lockView != null;

        this.name = name;
        this.key = key;
        this.lockView = lockView;
        this.ctx = lockView.context();
        this.fair = fair;

        log = ctx.logger(getClass());

        final UpdateListener lsnr = new UpdateListener();

        sync = new LocalSync(new GlobalSync(ctx.localNodeId(), key, lockView, lsnr, ctx), fair);

        ctx.io().addCacheHandler(ctx.cacheId(), ReleasedMessage.class,
            new IgniteBiInClosure<UUID, ReleasedMessage>() {
                @Override public void apply(UUID uuid, ReleasedMessage message) {
                    lsnr.release();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void needCheckNotRemoved() {
        // No-op.
    }

    /**
     *
     */
    public static class ReleasedMessage extends GridCacheIdMessage {
        /** */
        public ReleasedMessage() {
            // No-op.
        }

        /** */
        public ReleasedMessage(int id) {
            cacheId(id);
        }

        /** Message index. */
        public static final int CACHE_MSG_IDX = nextIndexId();

        /** */
        private static final long serialVersionUID = 181741851451L;

        /** {@inheritDoc} */
        @Override public boolean addDeploymentInfo() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return -62;
        }

        /** {@inheritDoc} */
        @Override public int lookupIndex() {
            return CACHE_MSG_IDX;
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void lock() {
        ctx.kernalContext().gateway().readLock();

        try {
            sync.lock();
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void lockInterruptibly() throws IgniteInterruptedException {
        ctx.kernalContext().gateway().readLock();

        try {
            sync.lockInterruptibly();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock() {
        ctx.kernalContext().gateway().readLock();

        try {
            return sync.tryLock();
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock(long timeout, TimeUnit unit) throws IgniteInterruptedException {
        assert unit != null;

        ctx.kernalContext().gateway().readLock();

        try {
            return sync.tryLock(timeout, unit);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void unlock() {
        ctx.kernalContext().gateway().readLock();

        try {
            sync.unlock();
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

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
    @Override public boolean isLocked() throws IgniteException {
        ctx.kernalContext().gateway().readLock();

        try {
            return sync.isLocked();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
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

    @Override public boolean isFair() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isBroken() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheInternalKey key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void close() {
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLockImpl2.class, this);
    }

    /** */
    private static class UpdateListener implements GridLocalEventListener {
        /** */
        private final ReentrantLock lock  = new ReentrantLock();

        /** */
        private final Condition latch = lock.newCondition();

        /** */
        private final AtomicInteger count = new AtomicInteger(0);

        /** */
        @Override public void onEvent(Event evt) {
            release();
        }

        /** */
        public void release() {
            lock.lock();
            try {
                count.incrementAndGet();

                latch.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /** */
        public void await() throws InterruptedException {
            lock.lock();
            try {
                if (count.getAndDecrement()<=0) {
                    latch.await();
                }
            }
            finally {
                lock.unlock();
            }
        }

        /** */
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            lock.lock();
            try {
                if (count.getAndDecrement()<=0) {
                    return latch.await(timeout, unit);
                }

                return true;
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * Sync class for acquire/relese global lock in grid. It avoid problems with thread local synchronization.
     */
    private static class GlobalSync {
        /** */
        private final AcquireProcessor acquireProcessor;

        /** */
        private final ReleaseProcessor releaseProcessor;

        /** */
        private final LockIfFreeProcessor lockIfFreeProcessor;

        /** */
        private final LockOrRemoveProcessor lockOrRemoveProcessor;

        /** */
        private final GridCacheInternalKey key;

        /** */
        private final UpdateListener listener;

        /** Reentrant lock projection. */
        private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2> lockView;

        /** Cache context. */
        private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2> ctx;

        /** */
        private final UUID nodeId;

        /** */
        private GlobalSync(UUID nodeId, GridCacheInternalKey key,
            IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2> lockView, UpdateListener listener,
            GridCacheContext<GridCacheInternalKey, GridCacheLockState2> ctx) {

            assert nodeId != null;
            assert key != null;
            assert lockView != null;
            assert listener != null;
            assert ctx != null;

            acquireProcessor = new AcquireProcessor(nodeId);
            releaseProcessor = new ReleaseProcessor(nodeId);
            lockIfFreeProcessor = new LockIfFreeProcessor(nodeId);
            lockOrRemoveProcessor = new LockOrRemoveProcessor(nodeId);

            this.key = key;
            this.lockView = lockView;
            this.listener = listener;
            this.ctx = ctx;
            this.nodeId = nodeId;
        }

        /** */
        private final GridCacheLockState2 forceGet() throws IgniteCheckedException {
            return lockView.get(key);
        }

        /** */
        private final boolean tryAcquireOrAdd() {
            try {
                return lockView.invoke(key, acquireProcessor).get();
            }
            catch (IgniteCheckedException ignored) {
                ignored.printStackTrace();
                return false;
            }
        }

        /** */
        private final boolean tryAcquire() {
            try {
                return lockView.invoke(key, lockIfFreeProcessor).get();
            }
            catch (IgniteCheckedException ignored) {
                ignored.printStackTrace();
                return false;
            }
        }

        /** */
        private final boolean acquireOrRemove() {
            try {
                return lockView.invoke(key, lockOrRemoveProcessor).get();
            }
            catch (IgniteCheckedException ignored) {
                ignored.printStackTrace();
                return false;
            }
        }

        /** */
        private final boolean waitForUpdate() {
            return waitForUpdate(30, TimeUnit.SECONDS);
        }

        /**
         *
         * @param timeout
         * @param unit
         * @return true if await finished well, false if InterruptedException has been thrown or timeout.
         */
        private final boolean waitForUpdate(long timeout, TimeUnit unit) {
            try {
                return listener.await(timeout, unit);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        /** */
        private final void acquire() {
            while (!tryAcquireOrAdd()) {
                waitForUpdate();
            }
        }

        /** */
        private final boolean acquire(long timeout, TimeUnit unit) {
            while (!tryAcquireOrAdd()) {
                if (!waitForUpdate(timeout, unit)) {
                    return acquireOrRemove();
                }
            }

            return true;
        }

        /** */
        private final boolean acquireInterruptibly() {
            while (!tryAcquire()) {
                if (!waitForUpdate()) {
                    return acquireOrRemove();
                }
            }

            return true;
        }

        /** {@inheritDoc} */
        private final void release() {
            lockView.invokeAsync(key, releaseProcessor).listen(
                new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<UUID>>>() {
                    @Override public void apply(IgniteInternalFuture<EntryProcessorResult<UUID>> future) {
                        try {
                            // invokeAsync return null if EntryProcessor return null too.
                            EntryProcessorResult<UUID> result = future.result();

                            if (result != null) {
                                final UUID nextNode = future.result().get();

                                if (nextNode != null && !nodeId.equals(nextNode))
                                    ctx.io().send(nextNode, new ReleasedMessage(ctx.cacheId()), P2P_POOL);
                            }
                        }
                        catch (IgniteCheckedException e) {
                            e.printStackTrace();
                        }
                    }
                });
        }
    }

    /** Local gateway before global lock. */
    private static class LocalSync implements Lock {
        /** */
        final ReentrantLock lock = new ReentrantLock();

        /** */
        final AtomicLong threadCount = new AtomicLong();

        /** */
        volatile boolean onGlobalLock = false;

        /** */
        final GlobalSync globalSync;

        /** */
        final static long MAX_TIME = 50_000_000L;

        /** */
        volatile long nextFinish;

        /** */
        final boolean fair;

        /** */
        public LocalSync(GlobalSync globalSync, boolean fair) {
            assert globalSync != null;

            this.globalSync = globalSync;
            this.fair = fair;
        }

        /** */
        private boolean isLocked() throws IgniteCheckedException {
            if (onGlobalLock)
                return true;

            ArrayDeque<UUID> nodes = globalSync.forceGet().nodes;

            return !(nodes == null || nodes.isEmpty());
        }

        /** */
        private boolean holdGlobalLockInterruptibly() {
            if (!onGlobalLock) {
                if (!globalSync.acquireInterruptibly())
                    return false;

                onGlobalLock = true;

                nextFinish = MAX_TIME + System.nanoTime();
            }
            return true;
        }

        /** */
        private void releaseGlobaLock() {
            if (threadCount.decrementAndGet() <= 0 || System.nanoTime() >= nextFinish) {
                globalSync.release();

                onGlobalLock = false;
            }
        }

        /** {@inheritDoc} */
        @Override public void lock() {
            threadCount.incrementAndGet();

            lock.lock();

            if (!onGlobalLock) {
                globalSync.acquire();

                onGlobalLock = true;

                nextFinish = MAX_TIME + System.nanoTime();
            }
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            try {
                releaseGlobaLock();
            }
            finally {
                lock.unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void lockInterruptibly() throws InterruptedException {
            threadCount.incrementAndGet();

            try {
                lock.lockInterruptibly();
            }
            catch (InterruptedException e) {
                releaseGlobaLock();

                throw e;
            }

            if (!holdGlobalLockInterruptibly()) {
                threadCount.decrementAndGet();

                lock.unlock();

                throw new InterruptedException();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            if (lock.tryLock()) {
                if (onGlobalLock) {
                    threadCount.incrementAndGet();

                    return true;
                }
                else {
                    if (globalSync.tryAcquire()) {
                        onGlobalLock = true;

                        threadCount.incrementAndGet();

                        return true;
                    }
                }
            }
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            assert unit != null;

            long start = System.nanoTime();

            if (lock.tryLock(time, unit)) {
                if (onGlobalLock) {
                    threadCount.incrementAndGet();

                    return true;
                }
                else {
                    long left = unit.toNanos(time) - (System.nanoTime() - start);

                    if (globalSync.acquire(left, TimeUnit.NANOSECONDS)) {
                        onGlobalLock = true;

                        threadCount.incrementAndGet();

                        return true;
                    }
                }
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public Condition newCondition() {
            throw new UnsupportedOperationException("IgniteLock does not allow creation of nameless conditions. ");
        }
    }
}
