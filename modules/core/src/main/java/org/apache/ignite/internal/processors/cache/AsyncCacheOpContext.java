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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.CdcManager;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.systemview.ScanQuerySystemView;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictManager;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.jta.CacheJtaManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;

/**
 * Context for managing async cache operations.
 */
@GridToStringExclude
public class AsyncCacheOpContext {
    /** Last asynchronous future. */
    protected ThreadLocal<FutureHolder> lastFut = new ThreadLocal<FutureHolder>() {
        @Override protected FutureHolder initialValue() {
            return new FutureHolder();
        }
    };

    /** Asynchronous operations limit semaphore. */
    private Semaphore asyncOpsSem;

    public AsyncCacheOpContext() {
        if (ctx.config().getMaxConcurrentAsyncOperations() > 0)
            asyncOpsSem = new Semaphore(ctx.config().getMaxConcurrentAsyncOperations());
    }

    /**
     * Tries to acquire asynchronous operations permit, if limited.
     *
     * @param retry Retry flag.
     * @return Failed future if waiting was interrupted.
     */
    @Nullable protected <T> IgniteInternalFuture<T> asyncOpAcquire(boolean retry) {
        try {
            if (!retry && asyncOpsSem != null)
                asyncOpsSem.acquire();

            return null;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            return new GridFinishedFuture<>(new IgniteInterruptedCheckedException("Failed to wait for asynchronous " +
                "operation permit (thread got interrupted).", e));
        }
    }

    /**
     * Releases asynchronous operations permit, if limited.
     *
     * @param retry Retry flag.
     */
    protected final void asyncOpRelease(boolean retry) {
        if (!retry && asyncOpsSem != null)
            asyncOpsSem.release();
    }

    /**
     * Awaits for previous async operation to be completed.
     */
    public void awaitLastFut() {
        FutureHolder holder = lastFut.get();

        IgniteInternalFuture fut = holder.future();

        if (fut != null && !fut.isDone()) {
            try {
                // Ignore any exception from previous async operation as it should be handled by user.
                fut.get();
            }
            catch (IgniteCheckedException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Replaces previous async operation future on transaction suspend.
     */
    public @Nullable FutureHolder suspendLastFut() {
        FutureHolder holder = lastFut.get();

        IgniteInternalFuture fut = holder.future();

        if (fut != null && !fut.isDone()) {
            lastFut.remove();

            return holder;
        }
        else
            return null;
    }

    /**
     * Replaces previous async operation future on transaction resume.
     */
    public void resumeLastFut(FutureHolder holder) {
        IgniteInternalFuture resumedFut = holder.future();

        if (resumedFut == null || resumedFut.isDone())
            return;

        FutureHolder threadHolder = lastFut.get();

        IgniteInternalFuture threadFut = threadHolder.future();

        if (threadFut != null && !threadFut.isDone()) {
            threadHolder.lock();

            try {
                GridCompoundFuture f = new GridCompoundFuture<>().add(threadFut).add(resumedFut).markInitialized();

                saveFuture(threadHolder, f, /*asyncOp*/false, /*retry*/false);
            }
            finally {
                threadHolder.unlock();
            }
        }
        else
            lastFut.set(holder);
    }

    /**
     * Saves future in thread local holder and adds listener
     * that will clear holder when future is finished.
     *
     * @param holder Future holder.
     * @param fut Future to save.
     * @param asyncOp Whether operation is instance of AsyncOp.
     * @param retry {@code true} for retry operations.
     */
    protected void saveFuture(final FutureHolder holder, IgniteInternalFuture<?> fut, final boolean asyncOp, final boolean retry) {
        assert holder != null;
        assert fut != null;
        assert holder.holdsLock();

        holder.future(fut);

        if (fut.isDone()) {
            holder.future(null);

            if (asyncOp)
                asyncOpRelease(retry);
        }
        else {
            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    if (asyncOp)
                        asyncOpRelease(retry);

                    if (!holder.tryLock())
                        return;

                    try {
                        if (holder.future() == f)
                            holder.future(null);
                    }
                    finally {
                        holder.unlock();
                    }
                }
            });
        }
    }

    /**
     * Holder for last async operation future.
     */
    public static class FutureHolder {
        /** Lock. */
        private final ReentrantLock lock = new ReentrantLock();

        /** Future. */
        private IgniteInternalFuture fut;

        /**
         * Tries to acquire lock.
         *
         * @return Whether lock was actually acquired.
         */
        public boolean tryLock() {
            return lock.tryLock();
        }

        /**
         * Acquires lock.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        public void lock() {
            lock.lock();
        }

        /**
         * Releases lock.
         */
        public void unlock() {
            lock.unlock();
        }

        /**
         * @return Whether lock is held by current thread.
         */
        public boolean holdsLock() {
            return lock.isHeldByCurrentThread();
        }

        /**
         * Gets future.
         *
         * @return Future.
         */
        public IgniteInternalFuture future() {
            return fut;
        }

        /**
         * Sets future.
         *
         * @param fut Future.
         */
        public void future(@Nullable IgniteInternalFuture fut) {
            this.fut = fut;
        }
    }
}
