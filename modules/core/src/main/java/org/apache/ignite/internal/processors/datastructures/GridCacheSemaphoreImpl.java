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
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.retryTopologySafe;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache semaphore implementation based on AbstractQueuedSynchronizer. Current implementation supports only unfair
 * semaphores. If any node fails after acquiring permissions on cache semaphore, there are two different behaviors
 * controlled with the parameter failoverSafe. If this parameter is true, other nodes can reacquire permits that were
 * acquired by the failing node. In case this parameter is false, IgniteInterruptedException is called on every node
 * waiting on this semaphore.
 */
public final class GridCacheSemaphoreImpl extends AtomicDataStructureProxy<GridCacheSemaphoreState>
    implements GridCacheSemaphoreEx, IgniteChangeGlobalStateSupport, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return new IgniteBiTuple<>();
            }
        };

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
    }

    /**
     * Synchronization implementation for semaphore. Uses AQS state to represent permits.
     */
    final class Sync extends AbstractQueuedSynchronizer {
        /** */
        private static final long serialVersionUID = 1192457210091910933L;

        /** Map containing number of acquired permits for each node waiting on this semaphore. */
        private Map<UUID, Integer> nodeMap;

        /** Flag indicating that it is safe to continue after node that acquired semaphore fails. */
        final boolean failoverSafe;

        /** Flag indicating that a node failed and it is not safe to continue using this semaphore. */
        protected boolean broken = false;

        protected Sync(int permits, Map<UUID, Integer> waiters, boolean failoverSafe) {
            setState(permits);
            nodeMap = waiters;
            this.failoverSafe = failoverSafe;
        }

        /**
         * Sets a map containing number of permits acquired by each node using this semaphore. This method should only
         * be called in {@linkplain GridCacheSemaphoreImpl#onUpdate(GridCacheSemaphoreState)}.
         *
         * @param nodeMap NodeMap.
         */
        synchronized void setWaiters(Map<UUID, Integer> nodeMap) {
            this.nodeMap = nodeMap;
        }

        /**
         * Gets the number of nodes waiting at this semaphore.
         *
         * @return Number of nodes waiting at this semaphore.
         */
        int getWaiters() {
            int totalWaiters = 0;

            for (Integer i : nodeMap.values()) {
                if (i > 0)
                    totalWaiters++;
            }

            return totalWaiters;
        }

        /**
         * Get number of permits for node.
         *
         * @param nodeID Node ID.
         * @return Number of permits node has acquired at this semaphore. Can be less than 0 if more permits were
         * released than acquired on node.
         */
        int getPermitsForNode(UUID nodeID) {
            return nodeMap.containsKey(nodeID) ? nodeMap.get(nodeID) : 0;
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
         * Set a flag indicating that it is not safe to continue using this semaphore. This is the case only if one of
         * two things happened: 1. A node that previously acquired on this semaphore failed and semaphore is created in
         * non-failoversafe mode; 2. Local node failed (is closed), so any any threads on this node waiting to acquire
         * are notified, and semaphore is not safe to be used anymore.
         *
         * @return True is semaphore is not safe to be used anymore.
         */
        protected boolean isBroken() {
            return broken;
        }

        /**
         * Flag indicating that a node failed and it is not safe to continue using this semaphore. Any attempt to
         * acquire on broken semaphore will result in {@linkplain IgniteInterruptedException}.
         *
         * @param broken True if semaphore should not be used anymore.
         */
        protected void setBroken(boolean broken) {
            this.broken = broken;
        }

        /**
         * This method is used by the AQS to test if the current thread should block or not.
         *
         * @param acquires Number of permits to acquire.
         * @return Negative number if thread should block, positive if thread successfully acquires permits.
         */
        final int nonfairTryAcquireShared(int acquires) {
            for (; ; ) {
                // If broken, return immediately, exception will be thrown anyway.
                if (broken)
                    return 1;

                int available = getState();

                int remaining = available - acquires;

                if (remaining < 0 || compareAndSetGlobalState(available, remaining, false))
                    return remaining;
            }
        }

        /** {@inheritDoc} */
        @Override protected int tryAcquireShared(int acquires) {
            return nonfairTryAcquireShared(acquires);
        }

        /** {@inheritDoc} */
        @Override protected final boolean tryReleaseShared(int releases) {
            // Fail-fast path.
            if (broken)
                return true;

            // Check if some other node updated the state.
            // This method is called with release==0 only when trying to wake through update.
            if (releases == 0)
                return true;

            for (; ; ) {
                // If broken, return immediately, exception will be thrown anyway.
                if (broken)
                    return true;

                int cur = getState();

                int next = cur + releases;

                if (next < cur) // overflow
                    throw new Error("Maximum permit count exceeded");

                if (compareAndSetGlobalState(cur, next, false))
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
                // If broken, return immediately, exception will be thrown anyway.
                if (broken)
                    return 1;

                int curr = getState();

                if (curr == 0 || compareAndSetGlobalState(curr, 0, true))
                    return curr;
            }
        }

        /**
         * This method is used for synchronizing the semaphore state across all nodes.
         *
         * @param expVal Expected number of permits.
         * @param newVal New number of permits.
         * @param draining True if used for draining the permits.
         * @return True if this is the call that succeeded to change the global state.
         */
        boolean compareAndSetGlobalState(final int expVal, final int newVal, final boolean draining) {
            try {
                return retryTopologySafe(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        try (GridNearTxLocal tx = CU.txStartInternal(ctx,
                            cacheView,
                            PESSIMISTIC, REPEATABLE_READ)
                        ) {
                            GridCacheSemaphoreState val = cacheView.get(key);

                            if (val == null)
                                throw new IgniteCheckedException("Failed to find semaphore with given name: " +
                                    name);

                            // Abort if state is already broken.
                            if (val.isBroken()) {
                                tx.rollback();

                                return true;
                            }

                            boolean retVal = val.getCount() == expVal;

                            if (retVal) {
                                // If this is not a call to drain permits,
                                // Modify global permission count for the calling node.
                                if (!draining) {
                                    UUID nodeID = ctx.localNodeId();

                                    Map<UUID, Integer> map = val.getWaiters();

                                    int waitingCnt = expVal - newVal;

                                    if (map.containsKey(nodeID))
                                        waitingCnt += map.get(nodeID);

                                    map.put(nodeID, waitingCnt);

                                    val.setWaiters(map);
                                }

                                val.setCount(newVal);

                                cacheView.put(key, val);

                                tx.commit();
                            }

                            return retVal;
                        }
                        catch (Error | Exception e) {
                            if (!ctx.kernalContext().isStopping())
                                U.error(log, "Failed to compare and set: " + this, e);

                            throw e;
                        }
                    }
                });
            }
            catch (IgniteCheckedException e) {
                if (ctx.kernalContext().isStopping()) {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring failure in semaphore on node left handler (node is stopping): " + e);

                    return true;
                }
                else
                    throw U.convertException(e);
            }
        }

        /**
         * This method is used for releasing the permits acquired by failing node.
         * In case the semaphore is broken, no permits are released and semaphore is set (globally) to broken state.
         *
         * @param nodeId ID of the failing node.
         * @param broken Flag indicating that this semaphore is broken.
         * @return True if this is the call that succeeded to change the global state.
         */
        boolean releaseFailedNode(final UUID nodeId, final boolean broken) {
            try {
                return retryTopologySafe(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        try (
                            GridNearTxLocal tx = CU.txStartInternal(ctx,
                                cacheView,
                                PESSIMISTIC, REPEATABLE_READ)
                        ) {
                            GridCacheSemaphoreState val = cacheView.get(key);

                            if (val == null)
                                throw new IgniteCheckedException("Failed to find semaphore with given name: " +
                                    name);

                            // Quit early if semaphore is already broken.
                            if (val.isBroken()) {
                                tx.rollback();

                                return false;
                            }

                            // Mark semaphore as broken. No permits are released,
                            // since semaphore is useless from now on.
                            if (broken) {
                                val.setBroken(true);

                                cacheView.put(key, val);

                                tx.commit();

                                return true;
                            }

                            Map<UUID, Integer> map = val.getWaiters();

                            if (!map.containsKey(nodeId)) {
                                tx.rollback();

                                return false;
                            }

                            int numPermits = map.get(nodeId);

                            if (numPermits > 0)
                                val.setCount(val.getCount() + numPermits);

                            map.remove(nodeId);

                            val.setWaiters(map);

                            cacheView.put(key, val);

                            sync.nodeMap = map;

                            tx.commit();

                            return true;
                        }
                        catch (Error | Exception e) {
                            if (!ctx.kernalContext().isStopping())
                                U.error(log, "Failed to compare and set: " + this, e);

                            throw e;
                        }
                    }
                });
            }
            catch (IgniteCheckedException e) {
                if (ctx.kernalContext().isStopping()) {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring failure in semaphore on node left handler (node is stopping): " + e);

                    return true;
                }
                else
                    throw U.convertException(e);
            }
        }
    }

    /**
     * Constructor.
     *
     * @param name Semaphore name.
     * @param key Semaphore key.
     * @param semView Semaphore projection.
     */
    public GridCacheSemaphoreImpl(
        String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheSemaphoreState> semView
    ) {
        super(name, key, semView);
    }

    /**
     * @throws IgniteCheckedException If operation failed.
     */
    private void initializeSemaphore() throws IgniteCheckedException {
        if (!initGuard.get() && initGuard.compareAndSet(false, true)) {
            try {
                sync = retryTopologySafe(new Callable<Sync>() {
                    @Override public Sync call() throws Exception {
                        try (GridNearTxLocal tx = CU.txStartInternal(ctx,
                            cacheView, PESSIMISTIC, REPEATABLE_READ)) {
                            GridCacheSemaphoreState val = cacheView.get(key);

                            if (val == null) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to find semaphore with given name: " + name);

                                return null;
                            }

                            final int cnt = val.getCount();

                            Map<UUID, Integer> waiters = val.getWaiters();

                            final boolean failoverSafe = val.isFailoverSafe();

                            tx.commit();

                            Sync sync = new Sync(cnt, waiters, failoverSafe);

                            sync.setBroken(val.isBroken());

                            return sync;
                        }
                    }
                });

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
                throw new IgniteCheckedException("Internal semaphore has not been properly initialized.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(GridCacheSemaphoreState val) {
        if (sync == null)
            return;

        // Update broken flag.
        sync.setBroken(val.isBroken());

        // Update permission count.
        sync.setPermits(val.getCount());

        // Update waiters' counts.
        sync.setWaiters(val.getWaiters());

        // Try to notify any waiting threads.
        sync.releaseShared(0);
    }

    /** {@inheritDoc} */
    @Override public void onNodeRemoved(UUID nodeId) {
        try {
            initializeSemaphore();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to recover from failover because distributed semaphore cannot be initialized " +
                "(Ignore if this node is failing also)." );

            // Degrade gracefully, no exception is thrown
            // because other semaphores might also attempt to recover from failover.
            return;
        }

        int numPermits = sync.getPermitsForNode(nodeId);

        if (numPermits > 0) {
            // Semaphore is broken if reaches this point in non-failover safe mode.
            boolean broken = !sync.failoverSafe;

            // Release permits acquired by threads on failing node.
            sync.releaseFailedNode(nodeId, broken);

            if (broken) {
                // Interrupt every waiting thread if this semaphore is not failover safe.
                sync.setBroken(true);

                for (Thread t : sync.getSharedQueuedThreads())
                    t.interrupt();

                // Try to notify any waiting threads.
                sync.releaseShared(0);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (initGuard.get()) {
            try {
                // Wait while initialization is in progress.
                U.await(initLatch);
            }
            catch (IgniteInterruptedCheckedException e) {
                if (log.isDebugEnabled())
                    log.error("Failed waiting while initialization is completed.", e);
            }
        }
        else {
            // Preventing concurrent initialization.
            if (initGuard.compareAndSet(false, true)) {
                initLatch.countDown();

                if (log.isDebugEnabled())
                    log.debug("Semaphore wasn't initialized. Prevented further initialization.");

                return;
            }
            else {
                try {
                    // Wait while initialization is in progress.
                    U.await(initLatch);
                }
                catch (IgniteInterruptedCheckedException e) {
                    if (log.isDebugEnabled())
                        log.error("Failed waiting while initialization is completed.", e);
                }
            }
        }

        assert sync != null;

        sync.setBroken(true);

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
        ctx.kernalContext().gateway().readLock();

        A.ensure(permits >= 0, "Number of permits must be non-negative.");

        try {
            initializeSemaphore();

            sync.acquireSharedInterruptibly(permits);

            if (isBroken()) {
                Thread.interrupted(); // Clear interrupt flag.

                throw new InterruptedException();
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void acquireUninterruptibly() {
        ctx.kernalContext().gateway().readLock();

        try {
            initializeSemaphore();

            sync.acquireShared(1);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void acquireUninterruptibly(int permits) {
        ctx.kernalContext().gateway().readLock();

        A.ensure(permits >= 0, "Number of permits must be non-negative.");

        try {
            initializeSemaphore();

            sync.acquireShared(permits);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public int availablePermits() {
        ctx.kernalContext().gateway().readLock();

        int ret;
        try {
            initializeSemaphore();

            ret = retryTopologySafe(new Callable<Integer>() {
                @Override public Integer call() throws Exception {
                    try (
                        GridNearTxLocal tx = CU.txStartInternal(ctx,
                            cacheView, PESSIMISTIC, REPEATABLE_READ)
                    ) {
                        GridCacheSemaphoreState val = cacheView.get(key);

                        if (val == null)
                            throw new IgniteException("Failed to find semaphore with given name: " + name);

                        int cnt = val.getCount();

                        tx.rollback();

                        return cnt;
                    }
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override public int drainPermits() {
        ctx.kernalContext().gateway().readLock();

        try {
            initializeSemaphore();

            return sync.drainPermits();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryAcquire() {
        ctx.kernalContext().gateway().readLock();

        try {
            initializeSemaphore();

            boolean res = sync.nonfairTryAcquireShared(1) >= 0;

            if (isBroken()) {
                Thread.interrupted(); // Clear interrupt flag.

                throw new InterruptedException();
            }

            return res;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryAcquire(long timeout, TimeUnit unit) throws IgniteException {
        ctx.kernalContext().gateway().readLock();

        try {
            initializeSemaphore();

            boolean res = sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));

            if (isBroken()) {
                Thread.interrupted(); // Clear interrupt flag.

                throw new InterruptedException();
            }

            return res;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void release() {
        release(1);
    }

    /** {@inheritDoc} */
    @Override public void release(int permits) {
        ctx.kernalContext().gateway().readLock();

        A.ensure(permits >= 0, "Number of permits must be non-negative.");

        try {
            initializeSemaphore();

            sync.releaseShared(permits);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryAcquire(int permits) {
        ctx.kernalContext().gateway().readLock();

        A.ensure(permits >= 0, "Number of permits must be non-negative.");

        try {
            initializeSemaphore();

            return sync.nonfairTryAcquireShared(permits) >= 0;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws IgniteInterruptedException {
        ctx.kernalContext().gateway().readLock();

        A.ensure(permits >= 0, "Number of permits must be non-negative.");
        try {
            initializeSemaphore();

            boolean res = sync.tryAcquireSharedNanos(permits, unit.toNanos(timeout));

            if (isBroken()) {
                Thread.interrupted();

                throw new InterruptedException();
            }

            return res;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isFailoverSafe() {
        ctx.kernalContext().gateway().readLock();

        try {
            initializeSemaphore();

            return sync.failoverSafe;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThreads() {
        ctx.kernalContext().gateway().readLock();

        try {
            initializeSemaphore();

            return sync.getWaiters() != 0;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public int getQueueLength() {
        ctx.kernalContext().gateway().readLock();

        try {
            initializeSemaphore();

            return sync.getWaiters();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isBroken() {
        ctx.kernalContext().gateway().readLock();

        try {
            initializeSemaphore();

            return sync.isBroken();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
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

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<GridKernalContext, String> t = stash.get();

            IgniteSemaphore sem = IgnitionEx.localIgnite().context().dataStructures().semaphore(
                t.get2(),
                null,
                0,
                false,
                false);

            if (sem == null)
                throw new IllegalStateException("Semaphore was not found on deserialization: " + t.get2());

            return sem;
        }
        catch (IgniteCheckedException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (!rmvd) {
            try {
                ctx.kernalContext().dataStructures().removeSemaphore(name, ctx.group().name());
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
