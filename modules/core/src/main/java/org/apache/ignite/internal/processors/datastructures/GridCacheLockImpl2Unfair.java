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
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;

/**
 * Cache reentrant lock implementation based on IgniteCache.invoke.
 */
public final class GridCacheLockImpl2Unfair extends GridCacheLockEx2Default {
    /** Reentrant lock name. */
    private final String name;

    /** Cache context. */
    private final GridCacheContext<GridCacheInternalKey, GridCacheLockState2Base<UUID>> ctx;

    /** Internal synchronization object. */
    private final LocalSync sync;

    /** */
    private final GridCacheInternalKey key;

    /** */
    //private final IgniteInClosure<GridCacheIdMessage> releaser;

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

        /* Logger. */
        IgniteLogger log = ctx.logger(getClass());

        // final for passing into anonymous classes.
        final UpdateListener listener = new UpdateListener();

        sync = new LocalSync(new GlobalUnfairSync(ctx.localNodeId(), key, lockView, listener, ctx, log));

        System.out.println("!!!~ делаем лок на " + ctx.localNodeId()+ " в кэше "+ctx.name());

        ctx.io().addCacheHandler(ctx.cacheId(), GridCacheLockImpl2Unfair.ReleasedMessage.class,
            new IgniteBiInClosure<UUID, GridCacheLockImpl2Unfair.ReleasedMessage>() {
                @Override public void apply(UUID uuid, GridCacheLockImpl2Unfair.ReleasedMessage message) {
                    listener.release();
                }
            });
        /*releaser = new IgniteInClosure<GridCacheIdMessage>() {
            @Override public void apply(GridCacheIdMessage message) {
                assert message instanceof ReleasedMessage;
                //assert key.equals(((ReleasedMessage)message).name);

                listener.release();
            }
        };*/
    }

    /** {@inheritDoc} */
    @Override void removeAll(UUID id) {
        sync.globalSync.remove(id);
    }

    @Override public IgniteInClosure<GridCacheIdMessage> getReleaser() {
        return null; //releaser;
    }

    /** */
    public static class ReleasedMessage extends GridCacheIdMessage {
        /** */
        private static final long serialVersionUID = 181741851451L;

        /** Message index. */
        public static final int CACHE_MSG_IDX = nextIndexId();

        /** */
        //int name;

        /** */
        public ReleasedMessage() {
            // No-op.
        }

        /** */
        public ReleasedMessage(int id, String name) {
            //this.name = 1;//name.hashCode();

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
        /*@Override public byte fieldsCount() {
            return (byte)(super.fieldsCount()+1);
        }*/

        /** {@inheritDoc} */
        /*@Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
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
                    if (!writer.writeInt("name", name))
                        return false;

                    writer.incrementState();
            }

            return true;
        }*/

        /** {@inheritDoc} */
        /*@Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            reader.setBuffer(buf);

            if (!reader.beforeMessageRead())
                return false;

            if (!super.readFrom(buf, reader))
                return false;

            switch (reader.state()) {
                case 3:
                    name = reader.readInt("name");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
            }

            return reader.afterMessageRead(GridCacheIdMessage.class);
        }*/
    }

    /** {@inheritDoc} */
    @Override public void close() {
        /*System.out.println("Закрываем лок на "+ctx.localNodeId());
        if (ctx.kernalContext().dataStructures().removeReentrantLock(key))
            removeAll(ctx.localNodeId());

        System.out.println("Закрыли лок на "+ctx.localNodeId());*/
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
    private static class GlobalUnfairSync {
        /** */
        protected final AcquireUnfairProcessor acquireProcessor;

        /** */
        protected final ReleaseUnfairProcessor releaseProcessor;

        /** */
        protected final LockIfFreeUnfairProcessor lockIfFreeProcessor;

        /** */
        protected final LockOrRemoveUnfairProcessor lockOrRemoveProcessor;

        /** */
        private final GridCacheInternalKey key;

        /** */
        private final UpdateListener listener;

        /** Reentrant lock projection. */
        private final IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<UUID>> lockView;

        /** Logger. */
        private final IgniteLogger log;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<Boolean>>> acquireListener;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<UUID>>> releaseListener;

        /** */
        private final IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<RemoveProcessor.Tuple<UUID>>>> removeListener;

        /** */
        private final UUID nodeId;

        /** */
        GlobalUnfairSync(UUID nodeId, GridCacheInternalKey key,
            IgniteInternalCache<GridCacheInternalKey, GridCacheLockState2Base<UUID>> lockView,
            final UpdateListener listener,
            final GridCacheContext<GridCacheInternalKey, ? extends GridCacheLockState2Base<UUID>> ctx,
            final IgniteLogger log) {

            assert nodeId != null;
            assert key != null;
            assert lockView != null;
            assert listener != null;
            assert ctx != null;
            assert log != null;

            this.key = key;
            this.lockView = lockView;
            this.listener = listener;
            this.nodeId = nodeId;
            this.log = log;

            acquireProcessor = new AcquireUnfairProcessor(nodeId);
            releaseProcessor = new ReleaseUnfairProcessor(nodeId);
            lockIfFreeProcessor = new LockIfFreeUnfairProcessor(nodeId);
            lockOrRemoveProcessor = new LockOrRemoveUnfairProcessor(nodeId);

            acquireListener = new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<Boolean>>>() {
                @Override public void apply(IgniteInternalFuture<EntryProcessorResult<Boolean>> future) {
                    try {
                        EntryProcessorResult<Boolean> result = future.get();

                        if (result.get())
                            listener.release();
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Acquire invoke has failed: " + e);

                        listener.fail(U.convertException(e));
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
                                    listener.release();
                                else{
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

            removeListener = new IgniteInClosure<IgniteInternalFuture<EntryProcessorResult<RemoveProcessor.Tuple<UUID>>>>() {
                @Override
                public void apply(IgniteInternalFuture<EntryProcessorResult<RemoveProcessor.Tuple<UUID>>> future) {
                    try {
                        EntryProcessorResult<RemoveProcessor.Tuple<UUID>> result = future.get();

                        // invokeAsync return null if EntryProcessor return null too.
                        if (result != null) {
                            UUID nextNode = result.get().owner;

                            if (nextNode != null) {
                                System.out.println("!!!~ счас сообщим ноде "+nextNode);
                                if (nodeId.equals(nextNode))
                                    listener.release();
                                else{
                                    ctx.io().send(nextNode,
                                        new GridCacheLockImpl2Unfair.ReleasedMessage(ctx.cacheId(), key.name()), P2P_POOL);
                                }
                            } else System.out.println("!!!~ следующей ноды просто нет");
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

        /** */
        private final boolean tryAcquire() {
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
        private final boolean acquireOrRemove() {
            try {
                return lockView.invoke(key, lockOrRemoveProcessor).get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Invoke has failed: " + e);

                return false;
            }
        }

        /** */
        private final boolean waitForUpdate() {
            return waitForUpdate(30, TimeUnit.SECONDS);
        }

        /**
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
            while (true) {
                tryAcquireOrAdd().listen(acquireListener);

                if (waitForUpdate())
                    break;
            }
        }

        /** */
        private final boolean acquire(long timeout, TimeUnit unit) {
            while (true) {
                tryAcquireOrAdd().listen(acquireListener);

                if (waitForUpdate(timeout, unit))
                    break;

                return acquireOrRemove();
            }

            return true;
        }

        /** */
        private final boolean acquireInterruptibly() {
            while (true) {
                tryAcquireOrAdd().listen(acquireListener);

                if (waitForUpdate())
                    break;

                return acquireOrRemove();
            }

            return true;
        }

        /** */
        GridCacheLockState2Base<UUID> forceGet() throws IgniteCheckedException {
            return lockView.get(key);
        }

        /** */
        private final void release() {
            lockView.invokeAsync(key, releaseProcessor).listen(releaseListener);
        }

        /** */
        final void remove(UUID id) {
            lockView.invokeAsync(key, new RemoveProcessor<>(id)).listen(removeListener);
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
        final GlobalUnfairSync globalSync;

        /** */
        final static long MAX_TIME = 50_000_000L;

        /** */
        volatile long nextFinish;

        /** */
        ThreadLocal<Boolean> hasLocked = new ThreadLocal<Boolean>() {
            /** {@inheritDoc} */
            @Override protected Boolean initialValue() {
                return false;
            }
        };

        /** */
        public LocalSync(GlobalUnfairSync globalSync) {
            assert globalSync != null;

            this.globalSync = globalSync;
        }

        /** */
        private boolean isLocked() throws IgniteCheckedException {
            if (hasLocked.get())
                return true;

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
        private void releaseGlobalLock() {
            if (threadCount.decrementAndGet() <= 0 || System.nanoTime() >= nextFinish) {
                globalSync.release();

                onGlobalLock = false;
            }
        }

        /** {@inheritDoc} */
        @Override public void lock() {
            if (hasLocked.get())
                return;

            threadCount.incrementAndGet();

            lock.lock();

            if (!onGlobalLock) {
                globalSync.acquire();

                onGlobalLock = true;

                nextFinish = MAX_TIME + System.nanoTime();
            }

            hasLocked.set(true);
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            if (!hasLocked.get())
                return;

            try {
                releaseGlobalLock();
            }
            finally {
                lock.unlock();

                hasLocked.set(false);
            }
        }

        /** {@inheritDoc} */
        @Override public void lockInterruptibly() throws InterruptedException {
            if (hasLocked.get())
                return;

            threadCount.incrementAndGet();

            try {
                lock.lockInterruptibly();
            }
            catch (InterruptedException e) {
                releaseGlobalLock();

                throw e;
            }

            if (!holdGlobalLockInterruptibly()) {
                threadCount.decrementAndGet();

                lock.unlock();

                throw new InterruptedException();
            }

            hasLocked.set(true);
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            if (hasLocked.get())
                return true;

            if (lock.tryLock()) {
                if (onGlobalLock) {
                    threadCount.incrementAndGet();

                    hasLocked.set(true);

                    return true;
                }
                else {
                    if (globalSync.tryAcquire()) {
                        onGlobalLock = true;

                        threadCount.incrementAndGet();

                        hasLocked.set(true);

                        return true;
                    }
                }
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            assert unit != null;

            if (hasLocked.get())
                return true;

            long start = System.nanoTime();

            if (lock.tryLock(time, unit)) {
                if (onGlobalLock) {
                    threadCount.incrementAndGet();

                    hasLocked.set(true);

                    return true;
                }
                else {
                    long left = unit.toNanos(time) - (System.nanoTime() - start);

                    if (globalSync.acquire(left, TimeUnit.NANOSECONDS)) {
                        onGlobalLock = true;

                        threadCount.incrementAndGet();

                        hasLocked.set(true);

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
