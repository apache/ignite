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

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;

/** Reentrant lock fair implementation based on IgniteCache.invoke. */
public final class GridCacheLockImpl2Fair extends GridCacheLockEx2 {

    /** Reentrant lock name. */
    private final String name;

    /** Reentrant lock key. */
    private final GridCacheInternalKey key;

    /** Cache context. */
    private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> ctx;

    /** Internal synchronization object. */
    private final GlobalFairSync sync;

    /** */
    public GridCacheLockImpl2Fair(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> lockView) {

        assert name != null;
        assert key != null;
        assert lockView != null;

        this.name = name;
        this.key = key;
        this.ctx = lockView.context();

        IgniteLogger log = ctx.logger(getClass());

        sync = new GlobalFairSync(ctx.localNodeId(), key, lockView, ctx, log);
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
    @Override public void lockInterruptibly() throws IgniteInterruptedException, IgniteException {
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
            try {
                return sync.tryAcquire();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock(long timeout, TimeUnit unit) throws IgniteInterruptedException, IgniteException {
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
    @Override public boolean isLocked() throws IgniteException {
        try {
            if (sync.hasLocked.get())
                return true;

            ArrayDeque<LockOwner> nodes = sync.forceGet().nodes;

            return !(nodes == null || nodes.isEmpty());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isFair() {
        return true;
    }

    /** {@inheritDoc} */
    @Override void removeNode(UUID id) {
        sync.remove(id);
    }

    /** {@inheritDoc} */
    @Override public IgniteInClosure<GridCacheIdMessage> getReleaser() {
        return sync.getReleaser();
    }

    /** */
    public static class ReleasedThreadMessage extends GridCacheIdMessage {
        /** */
        private static final long serialVersionUID = 181741851451L;

        /** Message index. */
        public static final int CACHE_MSG_IDX = nextIndexId();

        /** */
        long threadId;

        /** */
        String name;

        /** */
        public ReleasedThreadMessage() {
            // No-op.
        }

        /** */
        public ReleasedThreadMessage(int id, long threadId, String name) {
            this.threadId = threadId;
            this.name = name;

            cacheId(id);
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
     * Sync class for acquire/release global lock in grid. It avoids problems with thread local synchronization.
     */
    private static final class GlobalFairSync {
        /** */
        private final ThreadLocal<AcquireFairProcessor> acquireProcessor = new ThreadLocal<>();

        /** */
        private final ThreadLocal<ReleaseFairProcessor> releaseProcessor = new ThreadLocal<>();

        /** */
        private final ThreadLocal<LockIfFreeFairProcessor> lockIfFreeProcessor = new ThreadLocal<>();

        /** */
        private final ThreadLocal<LockOrRemoveFairProcessor> lockOrRemoveProcessor = new ThreadLocal<>();

        /** */
        private final GridCacheInternalKey key;

        /** */
        private final ConcurrentHashMap<Long, Latch> listeners = new ConcurrentHashMap<>();

        /** Reentrant lock projection. */
        private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> lockView;

        /** Logger. */
        private final IgniteLogger log;

        /** */
        private final UUID nodeId;

        /** */
        private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> ctx;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<LockOwner>>> releaseListener;

        /** */
        private final ThreadLocal<Boolean> hasLocked = new ThreadLocal<Boolean>() {
            /** {@inheritDoc} */
            @Override protected Boolean initialValue() {
                return false;
            }
        };

        /** */
        private final IgniteInClosure<GridCacheIdMessage> releaser;

        /** */
        private GlobalFairSync(UUID nodeId, GridCacheInternalKey key, IgniteInternalCache<GridCacheInternalKey,
            GridCacheLockState2Base<LockOwner>> lockView,
            GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> ctx,
            final IgniteLogger log) {

            assert nodeId != null;
            assert key != null;
            assert lockView != null;
            assert ctx != null;
            assert log != null;

            this.key = key;
            this.lockView = lockView;
            this.log = log;
            this.nodeId = nodeId;
            this.ctx = ctx;

            releaseListener = new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<LockOwner>>>() {
                @Override public void apply(IgniteInternalFuture<EntryProcessorResult<LockOwner>> future) {
                    try {
                        EntryProcessorResult<LockOwner> result = future.get();

                        // invokeAsync return null if EntryProcessor return null too.
                        if (result != null) {
                            LockOwner nextOwner = result.get();

                            if (nextOwner != null) {
                                if (nodeId.equals(nextOwner.nodeId)) {
                                    // The same node.
                                    listeners.get(nextOwner.threadId).release();
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
                    listeners.get(message.threadId).release();
                }
            };
        }

        /** */
        private IgniteInClosure<GridCacheIdMessage> getReleaser() {
            return releaser;
        }

        /** */
        private LockOwner getOwner() {
            return new LockOwner(nodeId, Thread.currentThread().getId());
        }

        /** */
        private Latch getListener() {
            Latch listener = listeners.get(Thread.currentThread().getId());

            if (listener == null) {
                listener = new Latch();

                listeners.put(Thread.currentThread().getId(), listener);
            }

            return listener;
        }

        /** */
        private Latch tryAcquireOrAdd() {
            final Latch listener = getListener();

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
                            listener.release();
                        }
                    }
                    catch (IgniteCheckedException e) {
                        listener.fail(U.convertException(e));
                    }
                }
            });

            return listener;
        }

        /** */
        private boolean tryAcquire() throws IgniteCheckedException {
            if (hasLocked.get())
                return true;

            getListener();

            LockIfFreeFairProcessor processor = lockIfFreeProcessor.get();

            if (processor == null) {
                processor = new LockIfFreeFairProcessor(getOwner());

                lockIfFreeProcessor.set(processor);
            }

            boolean locked = lockView.invoke(key, processor).get();

            if (locked)
                hasLocked.set(true);

            return locked;
        }

        /** */
        private boolean acquireOrRemove() {
            try {
                getListener();

                LockOrRemoveFairProcessor processor = lockOrRemoveProcessor.get();

                if (processor == null) {
                    processor = new LockOrRemoveFairProcessor(getOwner());

                    lockOrRemoveProcessor.set(processor);
                }

                return lockView.invoke(key, processor).get();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** */
        private boolean waitForUpdate() {
            try {
                return waitForUpdate(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        /**
         * @param timeout
         * @param unit
         * @return true if await finished well, false if InterruptedException has been thrown or timeout.
         */
        private boolean waitForUpdate(long timeout, TimeUnit unit) throws InterruptedException {
            return getListener().await(timeout, unit);
        }

        /** */
        private void acquire() {
            if (hasLocked.get())
                return;

            while (true) {
                tryAcquireOrAdd();

                if (waitForUpdate())
                    break;
            }

            hasLocked.set(true);
        }

        /** */
        private boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            if (hasLocked.get())
                return true;

            while (true) {
                tryAcquireOrAdd();

                try {
                    if (waitForUpdate(timeout, unit))
                        break;
                }
                catch (InterruptedException e) {
                    if (acquireOrRemove()) {
                        hasLocked.set(true);

                        return true;
                    }
                    else
                        throw e;
                }

                return acquireOrRemove();
            }

            hasLocked.set(true);

            return true;
        }

        /** */
        private void acquireInterruptibly() throws InterruptedException {
            if (hasLocked.get())
                return;

            while (true) {
                tryAcquireOrAdd();

                try {
                    if (waitForUpdate(30, TimeUnit.SECONDS))
                        break;
                }
                catch (InterruptedException e) {
                    if (!acquireOrRemove())
                        throw e;
                }
            }

            hasLocked.set(true);
        }

        /** */
        private GridCacheLockState2Base<LockOwner> forceGet() throws IgniteCheckedException {
            return lockView.get(key);
        }

        /** */
        private void release() {
            if (!hasLocked.get())
                return;

            ReleaseFairProcessor processor = releaseProcessor.get();

            if (processor == null) {
                processor = new ReleaseFairProcessor(getOwner());

                releaseProcessor.set(processor);
            }

            lockView.invokeAsync(key, processor).listen(releaseListener);

            hasLocked.set(false);
        }

        /** */
        private void remove(UUID id) {
            lockView.invokeAsync(key, new RemoveProcessor<>(id)).listen(releaseListener);
        }
    }
}
