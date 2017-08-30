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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;

public final class GridCacheLockImpl2Fair extends GridCacheLockEx2Default {
    /** Logger. */
    private final IgniteLogger log;

    /** Reentrant lock name. */
    private final String name;

    /** Reentrant lock key. */
    private GridCacheInternalKey key;

    /** Reentrant lock projection. */
    private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<NodeThread>> lockView;

    /** Cache context. */
    private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<NodeThread>> ctx;

    /** Internal synchronization object. */
    private final GlobalFairSync sync;

    /** */
    public GridCacheLockImpl2Fair(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<NodeThread>> lockView) {

        assert name != null;
        assert key != null;
        assert lockView != null;

        this.name = name;
        this.key = key;
        this.lockView = lockView;
        this.ctx = lockView.context();
        this.log = ctx.logger(getClass());

        sync = new GlobalFairSync(ctx.localNodeId(), key, lockView, ctx, log);
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
            ArrayDeque<NodeThread> nodes = sync.forceGet().nodes;

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

    /** */
    public static class ReleasedThreadMessage extends GridCacheIdMessage {
        /** */
        long threadId;

        /** */
        public ReleasedThreadMessage() {
            // No-op.
        }

        /** */
        public ReleasedThreadMessage(int id, long threadId) {
            this.threadId = threadId;
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
            return -63;
        }

        /** {@inheritDoc} */
        @Override public int lookupIndex() {
            return CACHE_MSG_IDX;
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
            }

            return reader.afterMessageRead(GridCacheIdMessage.class);
        }
    }

    /**
     * Sync class for acquire/release global lock in grid. It avoids problems with thread local synchronization.
     */
    private static class GlobalFairSync {
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
        private final ConcurrentHashMap<Long, UpdateListener> listeners = new ConcurrentHashMap<>();

        /** Reentrant lock projection. */
        private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<NodeThread>> lockView;

        /** Logger. */
        private final IgniteLogger log;

        /** */
        private final UUID nodeId;

        /** */
        private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<NodeThread>> ctx;

        /** */
        GlobalFairSync(UUID nodeId, GridCacheInternalKey key, IgniteInternalCache<GridCacheInternalKey,
            GridCacheLockState2Base<NodeThread>> lockView,
            GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<NodeThread>> ctx,
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

            ctx.io().addCacheHandler(ctx.cacheId(), ReleasedThreadMessage.class,
                new IgniteBiInClosure<UUID, ReleasedThreadMessage>() {
                    @Override public void apply(UUID uuid, ReleasedThreadMessage message) {
                        // This line is the reason why we use a ConcurrentHashMap, not a ThreadLocal.
                        listeners.get(message.threadId).release();
                    }
                });
        }

        /** */
        private NodeThread getOwner() {
            return new NodeThread(nodeId, Thread.currentThread().getId());
        }

        /** */
        private UpdateListener getListener() {
            UpdateListener listener = listeners.get(Thread.currentThread().getId());

            if (listener == null) {
                listener = new UpdateListener();

                listeners.put(Thread.currentThread().getId(), listener);
            }

            return listener;
        }

        /** */
        private UpdateListener tryAcquireOrAdd() {
            final UpdateListener listener = getListener();

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
        private final boolean tryAcquire() throws IgniteCheckedException {
            getListener();

            LockIfFreeFairProcessor processor = lockIfFreeProcessor.get();

            if (processor == null) {
                processor = new LockIfFreeFairProcessor(getOwner());

                lockIfFreeProcessor.set(processor);
            }

            return lockView.invoke(key, processor).get();
        }

        /** */
        private final boolean acquireOrRemove() {
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
        private final boolean waitForUpdate() {
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
        private final boolean waitForUpdate(long timeout, TimeUnit unit) throws InterruptedException {
            return getListener().await(timeout, unit);
        }

        /** */
        private final void acquire() {
            while (true) {
                tryAcquireOrAdd();

                if (waitForUpdate())
                    break;
            }
        }

        /** */
        private final boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            while (true) {
                tryAcquireOrAdd();

                try {
                    if (waitForUpdate(timeout, unit))
                        break;
                }
                catch (InterruptedException e) {
                    if (acquireOrRemove())
                        return true;
                    else
                        throw e;
                }

                return acquireOrRemove();
            }

            return true;
        }

        /** */
        private final void acquireInterruptibly() throws InterruptedException {
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
        }

        GridCacheLockState2Base<NodeThread> forceGet() throws IgniteCheckedException {
            return lockView.get(key);
        }

        /** {@inheritDoc} */
        private final void release() {
            ReleaseFairProcessor processor = releaseProcessor.get();

            if (processor == null) {
                processor = new ReleaseFairProcessor(getOwner());

                releaseProcessor.set(processor);
            }

            lockView.invokeAsync(key, processor).listen(new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<NodeThread>>>() {
                @Override public void apply(IgniteInternalFuture<EntryProcessorResult<NodeThread>> future) {
                    try {
                        EntryProcessorResult<NodeThread> result = future.get();

                        // invokeAsync return null if EntryProcessor return null too.
                        if (result != null) {
                            NodeThread nextOwner = result.get();

                            if (nextOwner != null) {
                                if (nodeId.equals(nextOwner.nodeId)) {
                                    // The same node.
                                    listeners.get(nextOwner.threadId).release();
                                }
                                else {
                                    ctx.io().send(nextOwner.nodeId,
                                        new ReleasedThreadMessage(ctx.cacheId(), nextOwner.threadId),
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
            });
        }
    }
}
