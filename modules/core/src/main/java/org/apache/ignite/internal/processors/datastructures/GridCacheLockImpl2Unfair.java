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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;

/**
 * Unfair implementation of reentrant lock based on a {@link IgniteInternalCache#invoke}.
 */
public final class GridCacheLockImpl2Unfair extends GridCacheLockEx2 {
    /** Reentrant lock name. */
    private final String name;

    /** Cache context. */
    private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<UUID>> ctx;

    /** Internal synchronization object. */
    private final LocalSync sync;

    /** {@code ReleasedMessage} handler. */
    private final IgniteInClosure<GridCacheIdMessage> releaser;

    /**
     * Constructor.
     *
     * @param name Reentrant lock name.
     * @param key Reentrant lock key.
     * @param lockView Reentrant lock projection.
     */
    GridCacheLockImpl2Unfair(String name, final GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<UUID>> lockView) {
        assert name != null;
        assert key != null;
        assert lockView != null;

        this.name = name;
        this.ctx = lockView.context();

        // final for passing into anonymous classes.
        final Latch listener = new Latch();

        sync = new LocalSync(new GlobalSync(ctx.localNodeId(), key, lockView, listener, ctx,
            ctx.logger(getClass())));

        releaser = new IgniteInClosure<GridCacheIdMessage>() {
            @Override public void apply(GridCacheIdMessage message) {
                assert message instanceof ReleasedMessage;
                assert key.name().equals(((ReleasedMessage)message).name);

                listener.release();
            }
        };
    }

    /** {@inheritDoc} */
    @Override void onNodeRemoved(UUID id) {
        assert id != null;

        sync.globalSync.remove(id);
    }

    /** {@inheritDoc} */
    @Override public IgniteInClosure<GridCacheIdMessage> getReleaser() {
        return releaser;
    }

    /** Message from previous node which release the lock. */
    public static final class ReleasedMessage extends GridCacheIdMessage {
        /** */
        private static final long serialVersionUID = 181741851451L;

        /** Message index. */
        private static final int CACHE_MSG_IDX = nextIndexId();

        /** Lock name. */
        String name;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public ReleasedMessage() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param id Cache id.
         * @param name Lock name.
         */
        ReleasedMessage(int id, String name) {
            assert name != null;

            this.name = name;

            cacheId(id);
        }

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

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return (byte)(super.fieldsCount() + 1);
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            writer.setBuffer(buf);

            if (!super.writeTo(buf, writer))
                return false;

            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(directType(), fieldsCount()))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 3:
                    if (!writer.writeString("name", name))
                        return false;

                    writer.incrementState();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            reader.setBuffer(buf);

            if (!reader.beforeMessageRead())
                return false;

            if (!super.readFrom(buf, reader))
                return false;

            switch (reader.state()) {
                case 3:
                    name = reader.readString("name");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
            }

            return reader.afterMessageRead(GridCacheIdMessage.class);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReleasedMessage.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
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

    /** {@inheritDoc} */
    @Override public int getHoldCount() throws IgniteException {
        return sync.reentrantCount.get();
    }

    /** {@inheritDoc} */
    @Override public boolean isHeldByCurrentThread() throws IgniteException {
        return sync.reentrantCount.get() > 0;
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
    @Override public boolean hasQueuedThreads() throws IgniteException {
        return sync.hasQueuedThreads();
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThread(Thread thread) throws IgniteException {
        assert thread != null;

        return sync.hasQueuedThread(thread);
    }

    /** {@inheritDoc} */
    @Override public boolean isFair() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLockImpl2Unfair.class, this);
    }

    /**
     * Sync class for acquire/release global lock in unfair lock. <p> It avoids problems with threads by local
     * synchronization.
     */
    private static final class GlobalSync {
        /** */
        private final AcquireUnfairProcessor acquireProcessor;

        /** */
        private final ReleaseUnfairProcessor releaseProcessor;

        /** */
        private final AcquireIfFreeUnfairProcessor lockIfFreeProcessor;

        /** */
        private final AcquireOrRemoveUnfairProcessor lockOrRemoveProcessor;

        /** Key for shared lock state. */
        private final GridCacheInternalKey key;

        /**
         * The latch for a waiting a {@code ReleasedMessage}, failed or return {@code true} by a acquire processor.
         */
        private final Latch latch;

        /** Reentrant lock projection. */
        private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<UUID>> lockView;

        /** Logger. */
        private final IgniteLogger log;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<Boolean>>> acquireListener;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<UUID>>> releaseListener;

        /** Local node id. */
        private final UUID nodeId;

        /**
         * Constructor.
         *
         * @param nodeId Local node id.
         * @param key Key for shared lock state.
         * @param lockView Reentrant lock projection.
         * @param latch The latch for a waiting a {@code ReleasedMessage}, failed or return {@code true} by a acquire
         * processor
         * @param ctx Cache context.
         * @param log Logger.
         */
        private GlobalSync(final UUID nodeId, final GridCacheInternalKey key,
            IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<UUID>> lockView,
            final Latch latch,
            final GridCacheContext<GridCacheInternalKey, ? extends GridCacheLockState2Base<UUID>> ctx,
            final IgniteLogger log) {

            assert nodeId != null;
            assert key != null;
            assert lockView != null;
            assert latch != null;
            assert ctx != null;
            assert log != null;

            this.key = key;
            this.lockView = lockView;
            this.latch = latch;
            this.nodeId = nodeId;
            this.log = log;

            acquireProcessor = new AcquireUnfairProcessor(nodeId);
            releaseProcessor = new ReleaseUnfairProcessor(nodeId);
            lockIfFreeProcessor = new AcquireIfFreeUnfairProcessor(nodeId);
            lockOrRemoveProcessor = new AcquireOrRemoveUnfairProcessor(nodeId);

            acquireListener = new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<Boolean>>>() {
                @Override public void apply(IgniteInternalFuture<EntryProcessorResult<Boolean>> future) {
                    try {
                        EntryProcessorResult<Boolean> result = future.get();

                        if (result.get())
                            latch.release();
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Acquire invoke has failed: " + e);

                        latch.fail(U.convertException(e));
                    }
                }
            };

            releaseListener = new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<UUID>>>() {
                @Override public void apply(IgniteInternalFuture<EntryProcessorResult<UUID>> future) {
                    try {
                        EntryProcessorResult<UUID> result = future.get();

                        // IgniteInternalCache#invokeAsync return null if EntryProcessor return null too.
                        // See https://issues.apache.org/jira/browse/IGNITE-5994
                        if (result != null) {
                            UUID nextNode = result.get();

                            if (nextNode != null) {
                                if (nodeId.equals(nextNode))
                                    latch.release();
                                else {
                                    ctx.io().send(nextNode,
                                        new GridCacheLockImpl2Unfair.ReleasedMessage(ctx.cacheId(), key.name()), P2P_POOL);
                                }
                            }
                        }
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Release message sending has failed: " + e);
                    }
                }
            };
        }

        /** Acquire the lock or add the local node to a waiting queue. */
        private void acquireOrAdd() {
            lockView.invokeAsync(key, acquireProcessor).listen(acquireListener);
        }

        /**
         * Acquires the lock only if it is not held by another node at the time of invocation.
         *
         * @return {@code true} if the lock was free and was acquired by the current node, or the lock was already held
         * by the current node; and {@code false} otherwise.
         */
        private boolean tryAcquire() {
            try {
                return lockView.invoke(key, lockIfFreeProcessor).get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Invoke has failed: " + e);

                return false;
            }
        }

        /**
         * Acquires the lock only if it is not held by another node at the time of invocation. And will remove the local
         * node from a waiting queue if it's not.
         *
         * @return {@code true} if the lock was free and was acquired by the current node, or the lock was already held
         * by the current node; and {@code false} otherwise.
         */
        private boolean acquireOrRemove() {
            try {
                return lockView.invoke(key, lockOrRemoveProcessor).get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Invoke has failed: " + e);

                return false;
            }
        }

        /**
         * Waiting for releasing the latch.
         *
         * @throws InterruptedException If interrupted.
         */
        private void await() throws InterruptedException {
            latch.await();
        }

        /** Waiting for releasing the latch. */
        private void awaitUninterruptibly() {
            latch.awaitUninterruptibly();
        }

        /**
         * Waiting for releasing the latch.
         *
         * @param timeout The maximum time to wait.
         * @param unit The time unit of the {@code timeout} argument.
         * @return {@code true} if await finished well, {@code false} if timeout.
         * @throws InterruptedException If interrupted.
         */
        private boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            assert unit != null;

            return latch.await(timeout, unit);
        }

        /** Acquire the global lock. */
        private void acquire() {
            acquireOrAdd();

            awaitUninterruptibly();
        }

        /**
         * Acquire the global lock.
         *
         * @param timeout The maximum time to wait.
         * @param unit The time unit of the {@code timeout} argument.
         * @return {@code true} if locked, or {@code false} if timeout.
         * @throws InterruptedException If interrupted.
         */
        private boolean acquire(long timeout, TimeUnit unit) throws InterruptedException {
            assert unit != null;

            acquireOrAdd();

            try {
                if (!await(timeout, unit)) {
                    if (!acquireOrRemove())
                        return false;
                }
            }
            catch (InterruptedException e) {
                if (!acquireOrRemove())
                    throw e;
            }

            return true;
        }

        /**
         * Acquire the global lock.
         *
         * @throws InterruptedException If interrupted.
         */
        private void acquireInterruptibly() throws InterruptedException {
            acquireOrAdd();

            try {
                await();
            }
            catch (InterruptedException e) {
                if (!acquireOrRemove())
                    throw e;
            }
        }

        /**
         * Return shared lock state directly.
         *
         * @return
         * @throws IgniteCheckedException
         */
        @Nullable private GridCacheLockState2Base<UUID> forceGet() throws IgniteCheckedException {
            return lockView.get(key);
        }

        /** Async release the global lock. */
        private void release() {
            lockView.invokeAsync(key, releaseProcessor).listen(releaseListener);
        }

        /** Async remove node from state. */
        private void remove(UUID id) {
            assert id != null;

            lockView.invokeAsync(key, new RemoveProcessor<UUID>(id)).listen(releaseListener);
        }
    }

    /**
     * Local gateway before global lock. <p> In the unfair mode the node manages passing lock across threads by itself.
     */
    private static final class LocalSync implements Lock {
        /** */
        private final ReentrantLock lock = new ReentrantLock(true);

        /** A number of threads in a local waiting queue. */
        private final AtomicLong threadCount = new AtomicLong();

        /** */
        private volatile boolean isGloballyLocked = false;

        /** A Sync object for node synchronization. */
        private final GlobalSync globalSync;

        /** Max time of local lock holding. */
        private static final long MAX_TIME = 50_000_000L;

        /** Time when we shoud release global lock after next unlock. */
        private volatile long nextFinish;

        /** A counter for reentrant ability. */
        private final ReentrantCount reentrantCount = new ReentrantCount();

        /** */
        private LocalSync(GlobalSync globalSync) {
            assert globalSync != null;

            this.globalSync = globalSync;
        }

        /** {@link IgniteLock#hasQueuedThreads()} */
        private boolean hasQueuedThreads() {
            if (reentrantCount.get() > 0)
                return true;

            return isGloballyLocked || lock.isLocked();

        }

        /** {@link IgniteLock#hasQueuedThread(Thread)} */
        private boolean hasQueuedThread(Thread thread) {
            assert thread != null;

            return lock.hasQueuedThread(thread);
        }

        /** {@link IgniteLock#isLocked()} */
        private boolean isLocked() throws IgniteCheckedException {
            if (reentrantCount.get() > 0)
                return true;

            if (isGloballyLocked || lock.isLocked())
                return true;

            GridCacheLockState2Base<UUID> state = globalSync.forceGet();

            if (state == null)
                return false;

            ArrayDeque<UUID> nodes = state.owners;

            return !(nodes == null || nodes.isEmpty() ||
                // The unlock method calls IgniteInternalCache#invokeAsync,
                // so release processor can still work on primary node.
                (globalSync.nodeId.equals(nodes.getFirst()) && nodes.size() == 1)
            );
        }

        /**
         * Release the global lock in the rare case when the local lock acquiring get a timeout or interrupted, at the
         * same time the previous lock owner releases the lock but still doesn't see a threadCount decrement in that
         * thread, then it possible to forget to release the global lock.
         */
        private void releaseGlobalLockIfNeed() {
            if (threadCount.decrementAndGet() <= 0) {
                // ReentrantLock#lock will not wait a lot of time because
                // if we see zero and the lock is not already free,
                // it means there is exist one thread which executes LocalSync#unlock right now.
                // And we need a fair mode to acquire the lock immediately after that LocalSync#unlock.
                lock.lock();

                try {
                    if (threadCount.get() <= 0 || System.nanoTime() >= nextFinish) {
                        if (isGloballyLocked) {
                            globalSync.release();

                            isGloballyLocked = false;
                        }
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        }

        /** Release the global lock if the current thread is the last in local queue or time is up. */
        private void releaseGlobalLock() {
            if (threadCount.decrementAndGet() <= 0 || System.nanoTime() >= nextFinish) {
                globalSync.release();

                isGloballyLocked = false;
            }
        }

        /** {@inheritDoc} */
        @Override public void lock() {
            if (reentrantCount.get() > 0) {
                reentrantCount.increment();

                return;
            }

            threadCount.incrementAndGet();

            lock.lock();

            if (!isGloballyLocked) {
                globalSync.acquire();

                isGloballyLocked = true;

                nextFinish = MAX_TIME + System.nanoTime();
            }

            reentrantCount.increment();
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            assert reentrantCount.get() > 0;

            if (reentrantCount.get() > 1) {
                reentrantCount.decrement();

                return;
            }

            try {
                releaseGlobalLock();
            }
            finally {
                lock.unlock();

                reentrantCount.decrement();
            }
        }

        /** {@inheritDoc} */
        @Override public void lockInterruptibly() throws InterruptedException {
            if (reentrantCount.get() > 0) {
                reentrantCount.increment();

                return;
            }

            threadCount.incrementAndGet();

            try {
                lock.lockInterruptibly();
            }
            catch (InterruptedException e) {
                releaseGlobalLockIfNeed();

                throw e;
            }

            try {
                if (!isGloballyLocked) {
                    globalSync.acquireInterruptibly();

                    isGloballyLocked = true;

                    nextFinish = MAX_TIME + System.nanoTime();
                }
            }
            catch (InterruptedException e) {
                threadCount.decrementAndGet();

                lock.unlock();

                throw e;
            }

            reentrantCount.increment();
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            if (reentrantCount.get() > 0) {
                reentrantCount.increment();

                return true;
            }

            // tryLock doesn't wait for other threads and increment the threadCount only under the lock.
            if (lock.tryLock()) {
                if (isGloballyLocked) {
                    threadCount.incrementAndGet();

                    reentrantCount.increment();

                    return true;
                }
                else {
                    if (globalSync.tryAcquire()) {
                        isGloballyLocked = true;

                        threadCount.incrementAndGet();

                        reentrantCount.increment();

                        return true;
                    }
                }
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            assert unit != null;

            if (reentrantCount.get() > 0) {
                reentrantCount.increment();

                return true;
            }

            long start = System.nanoTime();

            threadCount.incrementAndGet();

            if (lock.tryLock(time, unit)) {
                if (isGloballyLocked) {
                    reentrantCount.increment();

                    return true;
                }
                else {
                    long left = unit.toNanos(time) - (System.nanoTime() - start);

                    try {
                        if (globalSync.acquire(left, TimeUnit.NANOSECONDS)) {
                            isGloballyLocked = true;

                            reentrantCount.increment();

                            return true;
                        }
                    }
                    catch (InterruptedException e) {
                        threadCount.decrementAndGet();

                        lock.unlock();

                        throw e;
                    }
                }
            }

            releaseGlobalLockIfNeed();

            return false;
        }

        /** {@inheritDoc} */
        @Override public Condition newCondition() {
            throw new UnsupportedOperationException("IgniteLock does not allow creation of nameless conditions. ");
        }
    }
}
