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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Cache gateway.
 */
@GridToStringExclude
public class GridCacheGateway<K, V> {
    /** Context. */
    private final GridCacheContext<K, V> ctx;

    /** Stopped flag for dynamic caches. */
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTED);

    /** */
    private IgniteFuture<?> reconnectFut;

    /** */
    private StripedCompositeReadWriteLock rwLock =
        new StripedCompositeReadWriteLock(Runtime.getRuntime().availableProcessors());

    /**
     * @param ctx Cache context.
     */
    public GridCacheGateway(GridCacheContext<K, V> ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /**
     * Enter a cache call.
     */
    public void enter() {
        if (ctx.deploymentEnabled())
            ctx.deploy().onEnter();

        rwLock.readLock().lock();

        checkState(true, true);
    }

    /**
     * @param lock {@code True} if lock is held.
     * @param stopErr {@code True} if throw exception if stopped.
     * @return {@code True} if cache is in started state.
     */
    private boolean checkState(boolean lock, boolean stopErr) {
        State state = this.state.get();

        if (state != State.STARTED) {
            if (lock)
                rwLock.readLock().unlock();

            if (state == State.STOPPED) {
                if (stopErr)
                    throw new IllegalStateException("Cache has been stopped: " + ctx.name());
                else
                    return false;
            }
            else {
                assert reconnectFut != null;

                throw new CacheException(
                    new IgniteClientDisconnectedException(reconnectFut, "Client node disconnected: " + ctx.gridName()));
            }
        }

        return true;
    }

    /**
     * Enter a cache call.
     *
     * @return {@code True} if enter successful, {@code false} if the cache or the node was stopped.
     */
    public boolean enterIfNotStopped() {
        onEnter();

        // Must unlock in case of unexpected errors to avoid deadlocks during kernal stop.
        rwLock.readLock().lock();

        return checkState(true, false);
    }

    /**
     * Enter a cache call without lock.
     *
     * @return {@code True} if enter successful, {@code false} if the cache or the node was stopped.
     */
    public boolean enterIfNotStoppedNoLock() {
        onEnter();

        return checkState(false, false);
    }

    /**
     * Leave a cache call entered by {@link #enterNoLock} method.
     */
    public void leaveNoLock() {
        ctx.tm().resetContext();
        ctx.mvcc().contextReset();

        // Unwind eviction notifications.
        if (!ctx.shared().closed(ctx))
            CU.unwindEvicts(ctx);
    }

    /**
     * Leave a cache call entered by {@link #enter()} method.
     */
    public void leave() {
        try {
            leaveNoLock();
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * @param opCtx Cache operation context to guard.
     * @return Previous operation context set on this thread.
     */
    @Nullable public CacheOperationContext enter(@Nullable CacheOperationContext opCtx) {
        try {
            GridCacheAdapter<K, V> cache = ctx.cache();

            GridCachePreloader preldr = cache != null ? cache.preloader() : null;

            if (preldr == null)
                throw new IllegalStateException("Cache has been closed or destroyed: " + ctx.name());

            preldr.startFuture().get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for cache preloader start [cacheName=" +
                ctx.name() + "]", e);
        }

        onEnter();

        Lock lock = rwLock.readLock();

        lock.lock();

        checkState(true, true);

        // Must unlock in case of unexpected errors to avoid
        // deadlocks during kernal stop.
        try {
            return setOperationContextPerCall(opCtx);
        }
        catch (Throwable e) {
            lock.unlock();

            throw e;
        }
    }

    /**
     * @param opCtx Operation context to guard.
     * @return Previous operation context set on this thread.
     */
    @Nullable public CacheOperationContext enterNoLock(@Nullable CacheOperationContext opCtx) {
        onEnter();

        checkState(false, false);

        return setOperationContextPerCall(opCtx);
    }

    /**
     * Set thread local operation context per call.
     *
     * @param opCtx Operation context to guard.
     * @return Previous operation context set on this thread.
     */
    private CacheOperationContext setOperationContextPerCall(@Nullable CacheOperationContext opCtx) {
        CacheOperationContext prev = ctx.operationContextPerCall();

        if (prev != null || opCtx != null)
            ctx.operationContextPerCall(opCtx);

        return prev;
    }

    /**
     * @param prev Previous.
     */
    public void leave(CacheOperationContext prev) {
        try {
            leaveNoLock(prev);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * @param prev Previous.
     */
    public void leaveNoLock(CacheOperationContext prev) {
        ctx.tm().resetContext();
        ctx.mvcc().contextReset();

        // Unwind eviction notifications.
        CU.unwindEvicts(ctx);

        // Return back previous thread local operation context per call.
        ctx.operationContextPerCall(prev);
    }

    /**
     *
     */
    private void onEnter() {
        ctx.itHolder().checkWeakQueue();

        if (ctx.deploymentEnabled())
            ctx.deploy().onEnter();
    }

    /**
     *
     */
    public void stopped() {
        state.set(State.STOPPED);
    }

    /**
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut) {
        assert reconnectFut != null;

        this.reconnectFut = reconnectFut;

        state.compareAndSet(State.STARTED, State.DISCONNECTED);
    }

    /**
     *
     */
    public void writeLock(){
        rwLock.writeLock().lock();
    }

    /**
     *
     */
    public void writeUnlock() {
        rwLock.writeLock().unlock();
    }

    /**
     * @param stopped Cache stopped flag.
     */
    public void reconnected(boolean stopped) {
        State newState = stopped ? State.STOPPED : State.STARTED;

        state.compareAndSet(State.DISCONNECTED, newState);
    }

    /**
     *
     */
    public void onStopped() {
        boolean interrupted = false;

        while (true) {
            try {
                if (rwLock.writeLock().tryLock(200, TimeUnit.MILLISECONDS))
                    break;
                else
                    U.sleep(200);
            }
            catch (IgniteInterruptedCheckedException | InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();

        try {
            state.set(State.STOPPED);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     *
     */
    private enum State {
        /** */
        STARTED,

        /** */
        DISCONNECTED,

        /** */
        STOPPED
    }
}
