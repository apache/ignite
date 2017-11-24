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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;

/** Fair implementation of reentrant lock based on a {@link IgniteInternalCache#invoke}. */
public final class GridCacheLockImpl2Fair extends GridCacheLockEx2 {
    /** Reentrant lock name. */
    private final String name;

    /** Cache context. */
    private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> ctx;

    /** Internal synchronization object. */
    private final Sync sync;

    /**
     * Constructor.
     *
     * @param name Reentrant lock name.
     * @param key Key for shared lock state.
     * @param lockView Reentrant lock projection.
     */
    GridCacheLockImpl2Fair(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> lockView) {

        assert name != null;
        assert key != null;
        assert lockView != null;

        this.name = name;
        this.ctx = lockView.context();

        sync = new Sync(ctx.localNodeId(), key, lockView, ctx, ctx.logger(getClass()));
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
    @Override public void lock() throws IgniteException {
        ctx.kernalContext().gateway().readLock();

        try {
            sync.acquire();
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void lockInterruptibly() throws IgniteException {
        ctx.kernalContext().gateway().readLock();

        try {
            sync.acquireInterruptibly();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock() throws IgniteException {
        ctx.kernalContext().gateway().readLock();

        try {
            return sync.tryAcquire();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock(long timeout, TimeUnit unit) throws IgniteException {
        assert unit != null;

        ctx.kernalContext().gateway().readLock();

        try {
            return sync.tryAcquire(timeout, unit);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void unlock() throws IgniteInterruptedException {
        ctx.kernalContext().gateway().readLock();

        try {
            sync.release();
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
            if (sync.reentrantCount.get() > 0 || sync.isGloballyLocked)
                return true;

            GridCacheLockState2Base<LockOwner> state = sync.forceGet();

            if (state == null)
                return false;

            ArrayDeque<LockOwner> nodes = state.owners;

            return !(nodes == null || nodes.isEmpty() ||
                // The unlock method calls IgniteInternalCache#invokeAsync,
                // so release processor can still work on primary node.
                (sync.nodeId.equals(nodes.getFirst().nodeId) && nodes.size() == 1)
            );
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isFair() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLockImpl2Fair.class, this);
    }

    /** {@inheritDoc} */
    @Override void onNodeRemoved(UUID id) {
        assert id != null;

        sync.remove(id);
    }

    /** {@inheritDoc} */
    @Override public IgniteInClosure<GridCacheIdMessage> getReleaser() {
        return sync.getReleaser();
    }

    /** Message from previous node which release the lock. */
    public static class ReleasedThreadMessage extends GridCacheIdMessage {
        /** */
        private static final long serialVersionUID = 181741851451L;

        /** Message index. */
        private static final int CACHE_MSG_IDX = nextIndexId();

        /** Thread id. */
        private long threadId;

        /** Lock name. */
        private String name;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public ReleasedThreadMessage() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param id Cache id.
         * @param threadId Thread id.
         * @param name Lock name.
         */
        private ReleasedThreadMessage(int id, long threadId, String name) {
            assert name != null;

            this.threadId = threadId;
            this.name = name;

            cacheId(id);
        }

        /**
         * Return lock name.
         *
         * @return Lock name.
         */
        String getName() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public boolean addDeploymentInfo() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return -63;
        }

        /** {@inheritDoc} */
        @Override public int lookupIndex() {
            return CACHE_MSG_IDX;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return (byte)(super.fieldsCount() + 2);
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
                    if (!writer.writeLong("threadId", threadId))
                        return false;

                    writer.incrementState();
                case 4:
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
                    threadId = reader.readLong("threadId");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
                case 4:
                    name = reader.readString("name");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
            }

            return reader.afterMessageRead(GridCacheIdMessage.class);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReleasedThreadMessage.class, this);
        }
    }

    /**
     * Sync class for acquire/release global lock in fair mode. <p> It uses {@link ThreadLocal} to separation entry
     * processors for the different threads. It also uses {@link LockOwner} for determination lock owners.
     */
    private static final class Sync {
        /** */
        private final ThreadLocal<AcquireFairProcessor> acquireProcessor = new ThreadLocal<>();

        /** */
        private final ThreadLocal<ReleaseFairProcessor> releaseProcessor = new ThreadLocal<>();

        /** */
        private final ThreadLocal<AcquireIfFreeFairProcessor> acquireIfFreeProcessor = new ThreadLocal<>();

        /** */
        private final ThreadLocal<AcquireOrRemoveFairProcessor> acquireOrRemoveProcessor = new ThreadLocal<>();

        /** Key for shared lock state. */
        private final GridCacheInternalKey key;

        /**
         * Latches for a waiting a {@link ReleasedThreadMessage}, failed or return {@code true} by the acquire
         * processor.
         */
        private final ConcurrentHashMap<Long, Latch> latches = new ConcurrentHashMap<>();

        /** Reentrant lock projection. */
        private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> lockView;

        /** Local node. */
        private final UUID nodeId;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<LockOwner>>> releaseListener;

        /** A counter for reentrant ability. */
        private final ReentrantCount reentrantCount = new ReentrantCount();

        /** {@link ReleasedThreadMessage} handler. */
        private final IgniteInClosure<GridCacheIdMessage> releaser;

        /** Only for correct {@link GridCacheLockEx2#isLocked}. */
        private volatile boolean isGloballyLocked = false;

        /**
         * Constructor.
         *
         * @param nodeId Local node id.
         * @param key Key for shared lock state.
         * @param lockView Reentrant lock projection.
         * @param ctx Cache context.
         * @param log Logger.
         */
        private Sync(final UUID nodeId, final GridCacheInternalKey key, IgniteInternalCache<GridCacheInternalKey,
            GridCacheLockState2Base<LockOwner>> lockView,
            final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> ctx,
            final IgniteLogger log) {

            assert nodeId != null;
            assert key != null;
            assert lockView != null;
            assert ctx != null;
            assert log != null;

            this.key = key;
            this.lockView = lockView;
            this.nodeId = nodeId;

            releaseListener = new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<LockOwner>>>() {
                @Override public void apply(IgniteInternalFuture<EntryProcessorResult<LockOwner>> future) {
                    try {
                        EntryProcessorResult<LockOwner> result = future.get();

                        // IgniteInternalCache#invokeAsync return null if EntryProcessor return null too.
                        // See https://issues.apache.org/jira/browse/IGNITE-5994
                        if (result != null) {
                            LockOwner nextOwner = result.get();

                            if (nextOwner != null) {
                                if (nodeId.equals(nextOwner.nodeId)) {
                                    // The same node.
                                    latches.get(nextOwner.threadId).release();
                                }
                                else {
                                    ctx.io().send(nextOwner.nodeId,
                                        new ReleasedThreadMessage(ctx.cacheId(), nextOwner.threadId, key.name()),
                                        P2P_POOL);
                                }
                            }
                        }
                    }
                    catch (IgniteCheckedException e) {
                        // If release invoke has failed, it means primary node has failed too.
                        if (log.isDebugEnabled())
                            log.debug("Invoke has failed: " + e);
                    }
                }
            };

            releaser = new IgniteInClosure<GridCacheIdMessage>() {
                @Override public void apply(GridCacheIdMessage message0) {
                    assert message0 instanceof ReleasedThreadMessage;

                    ReleasedThreadMessage message = (ReleasedThreadMessage)message0;

                    assert key.name().equals(message.name);

                    // This line is the reason why we use a ConcurrentHashMap, but not a ThreadLocal.
                    latches.get(message.threadId).release();
                }
            };
        }

        /**
         * Return {@link ReleasedThreadMessage} handler.
         *
         * @return {@link ReleasedThreadMessage} handler.
         */
        private IgniteInClosure<GridCacheIdMessage> getReleaser() {
            return releaser;
        }

        /** Create {@link LockOwner} for the current thread and node. */
        private LockOwner getOwner() {
            return new LockOwner(nodeId, Thread.currentThread().getId());
        }

        /** Get {@link Latch} for the current thread. */
        private Latch getLatch() {
            Latch latch = latches.get(Thread.currentThread().getId());

            if (latch == null) {
                latch = new Latch();

                latches.put(Thread.currentThread().getId(), latch);
            }

            return latch;
        }

        /** Acquire the lock or add to waiting list. */
        private void acquireOrAdd() {
            final Latch latch = getLatch();

            AcquireFairProcessor processor = acquireProcessor.get();

            if (processor == null) {
                processor = new AcquireFairProcessor(getOwner());

                acquireProcessor.set(processor);
            }

            lockView.invokeAsync(key, processor).listen(new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<Boolean>>>() {
                @Override public void apply(IgniteInternalFuture<EntryProcessorResult<Boolean>> future) {
                    try {
                        EntryProcessorResult<Boolean> result = future.get();

                        if (result.get()) {
                            latch.release();
                        }
                    }
                    catch (IgniteCheckedException e) {
                        latch.fail(U.convertException(e));
                    }
                }
            });

        }

        /**
         * Acquires the lock only if it is not held by another node at the time of invocation.
         *
         * @return {@code true} if the lock was free and was acquired by the current node, or the lock was already held
         * by the current node; and {@code false} otherwise.
         */
        private boolean tryAcquire() throws IgniteCheckedException {
            if (reentrantCount.get() > 0) {
                reentrantCount.increment();

                return true;
            }

            getLatch();

            AcquireIfFreeFairProcessor processor = acquireIfFreeProcessor.get();

            if (processor == null) {
                processor = new AcquireIfFreeFairProcessor(getOwner());

                acquireIfFreeProcessor.set(processor);
            }

            boolean locked = lockView.invoke(key, processor).get();

            if (locked) {
                reentrantCount.increment();

                isGloballyLocked = true;
            }

            return locked;
        }

        /** Acquire the lock or remove from waiting list. */
        private boolean acquireOrRemove() {
            try {
                getLatch();

                AcquireOrRemoveFairProcessor processor = acquireOrRemoveProcessor.get();

                if (processor == null) {
                    processor = new AcquireOrRemoveFairProcessor(getOwner());

                    acquireOrRemoveProcessor.set(processor);
                }

                return lockView.invoke(key, processor).get();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** Waiting for releasing the latch. */
        private void awaitUninterruptibly() {
            getLatch().awaitUninterruptibly();
        }

        /**
         * Waiting for releasing the latch.
         *
         * @throws InterruptedException If interrupted.
         */
        private void await() throws InterruptedException {
            getLatch().await();
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

            return getLatch().await(timeout, unit);
        }

        /** Acquire the lock. */
        private void acquire() {
            if (reentrantCount.get() > 0) {
                reentrantCount.increment();

                return;
            }

            acquireOrAdd();

            awaitUninterruptibly();

            reentrantCount.increment();

            isGloballyLocked = true;
        }

        /**
         * Acquire the lock with timeout.
         *
         * @param timeout The maximum time to wait.
         * @param unit The time unit of the {@code timeout} argument.
         * @return {@code true} if locked, or {@code false} if timeout.
         * @throws InterruptedException If interrupted.
         */
        private boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            assert unit != null;

            if (reentrantCount.get() > 0) {
                reentrantCount.increment();

                return true;
            }

            acquireOrAdd();

            try {
                if (await(timeout, unit)) {
                    reentrantCount.increment();

                    isGloballyLocked = true;

                    return true;
                }
            }
            catch (InterruptedException e) {
                if (acquireOrRemove()) {
                    reentrantCount.increment();

                    isGloballyLocked = true;

                    return true;
                }
                else
                    throw e;
            }

            if (!acquireOrRemove())
                return false;

            reentrantCount.increment();

            isGloballyLocked = true;

            return true;
        }

        /**
         * Acquire the lock.
         *
         * @throws InterruptedException If interrupted.
         */
        private void acquireInterruptibly() throws InterruptedException {
            if (reentrantCount.get() > 0) {
                reentrantCount.increment();

                return;
            }

            acquireOrAdd();

            try {
                await();
            }
            catch (InterruptedException e) {
                if (!acquireOrRemove())
                    throw e;
            }

            reentrantCount.increment();

            isGloballyLocked = true;
        }

        /**
         * Return shared lock state directly.
         *
         * @return Shared lock state.
         * @throws IgniteCheckedException If failed.
         */
        @Nullable private GridCacheLockState2Base<LockOwner> forceGet() throws IgniteCheckedException {
            return lockView.get(key);
        }

        /** Async release the lock. */
        private void release() {
            assert reentrantCount.get() > 0;

            if (reentrantCount.get() > 1) {
                reentrantCount.decrement();

                return;
            }

            ReleaseFairProcessor processor = releaseProcessor.get();

            if (processor == null) {
                processor = new ReleaseFairProcessor(getOwner());

                releaseProcessor.set(processor);
            }

            lockView.invokeAsync(key, processor).listen(releaseListener);

            reentrantCount.decrement();

            isGloballyLocked = false;
        }

        /** Remove node from the lock state. */
        private void remove(UUID id) {
            assert id != null;

            lockView.invokeAsync(key, new RemoveProcessor<LockOwner>(id)).listen(releaseListener);
        }
    }
}
