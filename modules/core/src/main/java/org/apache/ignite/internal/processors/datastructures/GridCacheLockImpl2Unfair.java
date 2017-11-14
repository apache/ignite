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

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;

/**
 * Unfair implementation of reentrant lock based on IgniteCache.invoke.
 */
public final class GridCacheLockImpl2Unfair extends GridCacheLockEx2 {
    /** Reentrant lock name. */
    private final String name;

    /** Cache context. */
    private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<UUID>> ctx;

    /** Internal synchronization object. */
    private final LocalSync sync;

    /** Key for shared lock state. */
    private final GridCacheInternalKey key;

    /** ReleasedMessage handler. */
    private final IgniteInClosure<GridCacheIdMessage> releaser;

    /**
     * Constructor.
     *
     * @param name Reentrant lock name.
     * @param key Reentrant lock key.
     * @param lockView Reentrant lock projection.
     */
    public GridCacheLockImpl2Unfair(String name, GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<UUID>> lockView) {
        assert name != null;
        assert key != null;
        assert lockView != null;

        this.name = name;
        this.key = key;
        this.ctx = lockView.context();

        // final for passing into anonymous classes.
        final Latch listener = new Latch();

        sync = new LocalSync(new GlobalUnfairSync(ctx.localNodeId(), key, lockView, listener, ctx,
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
        public static final int CACHE_MSG_IDX = nextIndexId();

        /** Lock name. */
        String name;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public ReleasedMessage() {
            // No-op.
        }

        /**
         * @param id cache id.
         * @param name lock name.
         */
        public ReleasedMessage(int id, String name) {
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
    @Override public boolean isFair() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLockImpl2Unfair.class, this);
    }

    /**
     * Sync class for acquire/release global lock in grid. It avoids problems with thread local synchronization.
     */
    private static final class GlobalUnfairSync {
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

        /** */
        private final Latch latch;

        /** Reentrant lock projection. */
        private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<UUID>> lockView;

        /** Logger. */
        private final IgniteLogger log;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<Boolean>>> acquireListener;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<UUID>>> releaseListener;

        /** Local node. */
        private final UUID nodeId;

        /** */
        private GlobalUnfairSync(UUID nodeId, GridCacheInternalKey key,
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

                        // invokeAsync return null if EntryProcessor return null too.
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

        /** */
        private IgniteInternalFuture<EntryProcessorResult<Boolean>> tryAcquireOrAdd() {
            return lockView.invokeAsync(key, acquireProcessor);
        }

        /**
         * Acquires the lock only if it is not held by another node at the time
         * of invocation.
         *
         * @return {@code true} if the lock was free and was acquired by the
         *          current node, or the lock was already held by the current
         *          node; and {@code false} otherwise.
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

        /** */
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
         *
         * @return {@true} if await finished well, {@false} if InterruptedException has been thrown or timeout.
         */
        private void waitForRelease() throws InterruptedException {
            latch.await();
        }

        /** */
        private void waitForReleaseUninterruptibly() {
            latch.awaitUninterruptibly();
        }

        /**
         * @param timeout the maximum time to wait.
         * @param unit the time unit of the {@code timeout} argument.
         * @return {@true} if await finished well, {@false} if InterruptedException has been thrown or timeout.
         */
        private boolean waitForRelease(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        /** */
        private void acquire() {
            tryAcquireOrAdd().listen(acquireListener);

            waitForReleaseUninterruptibly();
        }

        /**
         *
         * @param timeout the maximum time to wait.
         * @param unit the time unit of the {@code timeout} argument.
         * @return {@true} if locked, or {@false} if InterruptedException has been thrown or timeout.
         */
        private boolean acquire(long timeout, TimeUnit unit) throws InterruptedException {
            tryAcquireOrAdd().listen(acquireListener);

            try {
                if (!waitForRelease(timeout, unit)) {
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
         *
         * @return {@true} if locked, or {@false} if InterruptedException has been thrown.
         */
        private void acquireInterruptibly() throws InterruptedException {
            tryAcquireOrAdd().listen(acquireListener);

            try {
                waitForRelease();
            }
            catch (InterruptedException e) {
                if (!acquireOrRemove())
                    throw e;
            }
        }

        /** Return shared lock state directly. */
        private GridCacheLockState2Base<UUID> forceGet() throws IgniteCheckedException {
            return lockView.get(key);
        }

        /** */
        private void release() {
            lockView.invokeAsync(key, releaseProcessor).listen(releaseListener);
        }

        /** */
        private void remove(UUID id) {
            lockView.invokeAsync(key, new RemoveProcessor<>(id)).listen(releaseListener);
        }
    }

    /** Local gateway before global lock. */
    private static final class LocalSync implements Lock {
        /** */
        private final ReentrantLock lock = new ReentrantLock();

        /** */
        private final AtomicLong threadCount = new AtomicLong();

        /** */
        private volatile boolean isGloballyLocked = false;

        /** */
        private final GlobalUnfairSync globalSync;

        /** Max time of local lock holding. */
        private static final long MAX_TIME = 50_000_000L;

        /** Time when we shoud release global lock. */
        private volatile long nextFinish;

        /** */
        private final ReentrantCount reentrantCount = new ReentrantCount();

        /** */
        private LocalSync(GlobalUnfairSync globalSync) {
            assert globalSync != null;

            this.globalSync = globalSync;
        }

        /** */
        private boolean isLocked() throws IgniteCheckedException {
            if (reentrantCount.get() > 0)
                return true;

            if (isGloballyLocked || lock.isLocked())
                return true;

            ArrayDeque<UUID> nodes = globalSync.forceGet().nodes;

            return !(nodes == null || nodes.isEmpty() ||
                // The unlock method calls invokeAsync, so release processor can still work on primary node.
                (globalSync.nodeId.equals(nodes.getFirst()) && nodes.size() == 1)
            );
        }

        /** */
        private void holdGlobalLockInterruptibly() throws InterruptedException {
            if (!isGloballyLocked) {
                globalSync.acquireInterruptibly();

                isGloballyLocked = true;

                nextFinish = MAX_TIME + System.nanoTime();
            }
        }

        /** */
        private void releaseGlobalLockIfNeed() {
            if (lock.tryLock()) {
                try {
                    if (threadCount.decrementAndGet() <= 0 || System.nanoTime() >= nextFinish) {
                        if (isGloballyLocked) {
                            globalSync.release();

                            isGloballyLocked = false;
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        /** */
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
                holdGlobalLockInterruptibly();
            } catch (InterruptedException e) {
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

            // tryLock don't wait for other threads and increment threadCount only under the lock.
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
