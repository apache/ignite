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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

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

    /** Map containing condition objects. */
    private final  Map<String, AbstractQueuedSynchronizer.ConditionObject> conditionMap = new HashMap<>();

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
        IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2> lockView) {

        assert name != null;
        assert key != null;
        assert lockView != null;

        this.name = name;
        this.key = key;
        this.lockView = lockView;
        this.ctx = lockView.context();

        log = ctx.logger(getClass());

        final UpdateListener lsnr = new UpdateListener();

        sync = new LocalSync(new GlobalSync(ctx.localNodeId(), key, lockView, lsnr, ctx));

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
        //No-op.
    }

    /**
     *
     */
    public static class ReleasedMessage extends GridCacheGroupIdMessage {
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
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void lock() {
        ctx.kernalContext().gateway().readLock();

        try{
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

        try{
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

        try{
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
        try {
            return sync.isLocked();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
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
        return true;
    }

    @Override public boolean isFair() {
        return true;
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
        private volatile CountDownLatch latch = new CountDownLatch(1);

        /** */
        @Override public void onEvent(Event evt) {
            release();
        }

        /** */
        public void release() {
            latch.countDown();

            latch = new CountDownLatch(1);
        }

        /** */
        public void await() throws InterruptedException {
            latch.await();

        }
    }

    /** EntryProcessor for lock acquire operation. */
    private static class Acquirer implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2, Boolean> {
        /** */
        final UUID nodeId;

        /** */
        public Acquirer(UUID nodeId) {
            assert nodeId != null;

            this.nodeId = nodeId;
        }

        /** */
        @Override public Boolean process(MutableEntry<GridCacheInternalKey, GridCacheLockState2> entry,
            Object... objects) throws EntryProcessorException {

            assert entry != null;

            if (entry.exists()) {
                GridCacheLockState2 state = entry.getValue();

                boolean acquired = state.lockIfFree(nodeId);

                boolean updated = true;

                if (!acquired) {
                    // Another node is acquiring the lock, so we add our-self in waiting-list.
                    updated = state.addToWaitingList(nodeId);
                }

                // Write result if necessary
                if (updated)
                    entry.setValue(state);

                return acquired;
            }

            return false;
        }
    }

    /** EntryProcessor for lock release operation. */
    private static class Releaser implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2, UUID> {
        /** */
        final UUID nodeId;

        /** */
        public Releaser(UUID nodeId) {
            assert nodeId != null;

            this.nodeId = nodeId;
        }

        /** */
        @Override public UUID process(MutableEntry<GridCacheInternalKey, GridCacheLockState2> entry,
            Object... objects) throws EntryProcessorException {

            assert entry != null;

            if (entry.exists()) {
                GridCacheLockState2 state = entry.getValue();

                try {
                    state.unlock(nodeId);
                }
                catch (IllegalMonitorStateException e) {
                    throw new EntryProcessorException(e);
                }

                UUID nextNode = state.getFirstNode();

                entry.setValue(state);

                return nextNode;
            }

            return null;
        }
    }

    /** Sync class for acquire/relese global lock in grid.
     *  It avoid problems with thread local synchronization. */
    private static class GlobalSync {
        /** */
        private final Acquirer acquirer;

        /** */
        private final Releaser releaser;

        /** */
        private final GridCacheInternalKey key;

        /** */
        private final UpdateListener listener;

        /** Reentrant lock projection. */
        private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2> lockView;

        /** Cache context. */
        private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2> ctx;

        /** */
        private GlobalSync(UUID nodeId, GridCacheInternalKey key,
            IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2> view, UpdateListener listener,
            GridCacheContext<GridCacheInternalKey, GridCacheLockState2> ctx) {

            assert nodeId != null;
            assert key != null;
            assert view != null;
            assert listener != null;
            assert ctx != null;

            acquirer = new Acquirer(nodeId);
            releaser = new Releaser(nodeId);
            this.key = key;
            lockView = view;
            this.listener = listener;
            this.ctx = ctx;
        }

        /** */
        private final GridCacheLockState2 forceGet() throws IgniteCheckedException {
                return lockView.get(key);
        }

        /** */
        private final boolean tryAcquire() {
            try {
                return lockView.invoke(key, acquirer).get();
            }
            catch (IgniteCheckedException ignored) {
                return false;
            }
        }

        /** */
        private final void waitForUpdate() {
            try {
                listener.await();
            }
            catch (InterruptedException e) {
                //No-op.
            }
        }

        /** */
        private final void acquire(){
            while (!tryAcquire()) {
                waitForUpdate();
            }
        }

        /** */
        private final boolean acquireInterruptibly() {
            while (!tryAcquire()) {
                if (Thread.currentThread().isInterrupted())
                    return false;

                waitForUpdate();
            }

            return true;
        }

        /** {@inheritDoc} */
        private final void release() {
            lockView.invokeAsync(key, releaser).listen(
                new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<UUID>>>() {
                    @Override public void apply(IgniteInternalFuture<EntryProcessorResult<UUID>> future) {
                        try {
                            // invokeAsync return null if EntryProcessor return null too.
                            EntryProcessorResult<UUID> result = future.result();

                            if (result != null) {
                                final UUID nextNode = future.result().get();

                                if (nextNode != null)
                                    ctx.io().send(nextNode, new ReleasedMessage(), SYSTEM_POOL);
                            }
                        }
                        catch (IgniteCheckedException ignored) {
                            //No-op.
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
        public LocalSync(GlobalSync globalSync) {
            assert globalSync != null;

            this.globalSync = globalSync;
        }

        /** */
        private boolean isLocked() throws IgniteCheckedException {
            if (onGlobalLock)
                return true;

            LinkedList<UUID> nodes = globalSync.forceGet().nodes;

            return !(nodes == null || nodes.isEmpty());
        }

        /** */
        private boolean holdGlobalLockInterruptibly() {
            if (!onGlobalLock) {
                if (!globalSync.acquireInterruptibly())
                    return false;

                onGlobalLock = true;

                nextFinish = MAX_TIME+System.nanoTime();
            }
            return true;
        }

        /** */
        private void releaseGlobaLock() {
            if (threadCount.decrementAndGet() <= 0 || System.nanoTime()>=nextFinish) {
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

                nextFinish = MAX_TIME+System.nanoTime();
            }
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            releaseGlobaLock();

            lock.unlock();
        }

        /** {@inheritDoc} */
        @Override public void lockInterruptibly() throws InterruptedException {
            threadCount.incrementAndGet();

            try {
                lock.lockInterruptibly();
            } catch (InterruptedException e) {
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

            // TODO: make timeout better.
            long finish = System.nanoTime() + unit.toNanos(time);

            while (!tryLock()) {
                if (System.nanoTime()>=finish)
                    return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public Condition newCondition() {
            throw new UnsupportedOperationException("IgniteLock does not allow creation of nameless conditions. ");
        }
    }
}
