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

package org.apache.ignite.internal.util.future;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteConsistencyViolationException;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 * Future composed of multiple inner futures.
 */
public class GridCompoundFuture<T, R> extends GridFutureAdapter<R> implements IgniteInClosure<IgniteInternalFuture<T>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initialization flag. */
    private static final int INIT_FLAG = 0x1;

    /** Flags updater. */
    private static final AtomicIntegerFieldUpdater<GridCompoundFuture> FLAGS_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridCompoundFuture.class, "initFlag");

    /** Listener calls updater. */
    private static final AtomicIntegerFieldUpdater<GridCompoundFuture> LSNR_CALLS_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridCompoundFuture.class, "lsnrCalls");

    /** Possible values: null (no future), IgniteInternalFuture instance (single future) or List of futures  */
    protected volatile Object futs;

    /** Reducer. */
    @GridToStringInclude
    private final IgniteReducer<T, R> rdc;

    /** Initialization flag. Updated via {@link #FLAGS_UPD}. */
    @SuppressWarnings("unused")
    private volatile int initFlag;

    /** Listener calls. Updated via {@link #LSNR_CALLS_UPD}. */
    @SuppressWarnings("unused")
    private volatile int lsnrCalls;

    /** The lock responds for a consistency of compounds. */
    private ReentrantReadWriteLock compoundsLock = new ReentrantReadWriteLock();

    /** Count of compounds in the future. */
    private volatile int size;

    /**
     * Default constructor.
     */
    public GridCompoundFuture() {
        this(null);
    }

    /**
     * @param rdc Reducer.
     */
    public GridCompoundFuture(@Nullable IgniteReducer<T, R> rdc) {
        this.rdc = rdc;
    }

    /** {@inheritDoc} */
    @Override public final void apply(IgniteInternalFuture<T> fut) {
        try {
            T t = fut.get();

            try {
                if (rdc != null && !rdc.collect(t))
                    onDone(rdc.reduce());
            }
            catch (RuntimeException e) {
                logError(null, "Failed to execute compound future reducer: " + this, e);

                // Exception in reducer is a bug, so we bypass checkComplete here.
                onDone(e);
            }
            catch (AssertionError e) {
                logError(null, "Failed to execute compound future reducer: " + this, e);

                // Bypass checkComplete because need to rethrow.
                onDone(e);

                throw e;
            }
        }
        catch (IgniteTxOptimisticCheckedException | IgniteFutureCancelledCheckedException |
            ClusterTopologyCheckedException | IgniteConsistencyViolationException e) {
            if (!processFailure(e, fut))
                onDone(e);
        }
        catch (IgniteCheckedException e) {
            if (!processFailure(e, fut)) {
                if (e instanceof NodeStoppingException)
                    logDebug(logger(), "Failed to execute compound future reducer, node stopped.");
                else
                    logError(null, "Failed to execute compound future reducer: " + this, e);

                onDone(e);
            }
        }
        catch (RuntimeException e) {
            logError(null, "Failed to execute compound future reducer: " + this, e);

            onDone(e);
        }
        catch (AssertionError e) {
            logError(null, "Failed to execute compound future reducer: " + this, e);

            // Bypass checkComplete because need to rethrow.
            onDone(e);

            throw e;
        }

        LSNR_CALLS_UPD.incrementAndGet(this);

        checkComplete();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        if (onCancelled()) {
            for (IgniteInternalFuture<T> fut : futures())
                fut.cancel();

            return true;
        }

        return false;
    }

    /**
     * Locks compound to read.
     */
    protected void compoundsReadLock() {
        compoundsLock.readLock().lock();
    }

    /**
     * Unlocks compound to read.
     */
    protected void compoundsReadUnlock() {
        compoundsLock.readLock().unlock();
    }

    /**
     * Locks compounds list and executes code in {@code supplier}, when the lock holds.
     *
     * @param supplier Closure to execute some code when the compounds are locked exclusively.
     * @return A result of the {@code supplier}.
     */
    protected Object compoundsLockedExclusively(Supplier<Object> supplier) {
        compoundsLock.writeLock().lock();
        try {
            return supplier.get();
        }
        finally {
            compoundsLock.writeLock().unlock();
        }
    }

    /**
     * Gets collection of futures.
     *
     * @return Collection of futures.
     */
    public final Collection<IgniteInternalFuture<T>> futures() {
        compoundsLock.readLock().lock();

        try {
            if (futs == null)
                return Collections.emptyList();

            if (futs instanceof IgniteInternalFuture)
                return Collections.singletonList((IgniteInternalFuture<T>)futs);

            return new ArrayList<>((Collection<IgniteInternalFuture<T>>)futs);
        }
        finally {
            compoundsLock.readLock().unlock();
        }
    }

    /**
     * Checks if this compound future should ignore this particular exception.
     *
     * @param err Exception to check.
     * @return {@code True} if this error should be ignored.
     */
    protected boolean ignoreFailure(Throwable err) {
        return false;
    }

    /**
     * Processes error thrown by some of the inner futures.
     *
     * @param err Thrown exception.
     * @param fut Failed future.
     * @return {@code True} if this error should be ignored.
     */
    protected boolean processFailure(Throwable err, IgniteInternalFuture<T> fut) {
        return ignoreFailure(err);
    }

    /**
     * Checks if there are pending futures. This is not the same as
     * {@link #isDone()} because child classes may override {@link #onDone(Object, Throwable)}
     * call and delay completion.
     *
     * @return {@code True} if there are pending futures.
     */
    protected final boolean hasPending() {
        compoundsLock.readLock().lock();

        try {
            int size0 = size;

            // Avoid iterator creation and collection copy.
            for (int i = 0; i < size0; i++) {
                IgniteInternalFuture<T> fut = future(i);

                if (!fut.isDone())
                    return true;
            }

            return false;
        }
        finally {
            compoundsLock.readLock().unlock();
        }
    }

    /**
     * Adds a future to this compound future.
     *
     * @param fut Future to add.
     */
    public final GridCompoundFuture<T, R> add(IgniteInternalFuture<T> fut) {
        assert fut != null;

        compoundsLock.writeLock().lock();

        try {
            if (futs == null)
                futs = fut;
            else if (futs instanceof IgniteInternalFuture) {
                Collection<IgniteInternalFuture> futs0 = createFuturesCollection();

                futs0.add((IgniteInternalFuture)futs);
                futs0.add(fut);

                futs = futs0;
            }
            else
                ((Collection<IgniteInternalFuture>)futs).add(fut);

            size++;
        }
        finally {
            compoundsLock.writeLock().unlock();
        }

        fut.listen(this);

        if (isCancelled()) {
            try {
                fut.cancel();
            }
            catch (IgniteCheckedException e) {
                onDone(e);
            }
        }

        return this;
    }

    /** */
    protected Collection<IgniteInternalFuture> createFuturesCollection() {
        return new ArrayList<>(4);
    }

    /**
     * Clear futures.
     */
    protected final void clear() {
        compoundsLock.writeLock().lock();

        try {
            futs = null;
            size = 0;
        }
        finally {
            compoundsLock.writeLock().unlock();
        }
    }

    /**
     * @return {@code True} if this future was initialized. Initialization happens when {@link #markInitialized()}
     * method is called on future.
     */
    public final boolean initialized() {
        return initFlag == INIT_FLAG;
    }

    /**
     * Mark this future as initialized.
     */
    public final GridCompoundFuture<T, R> markInitialized() {
        if (FLAGS_UPD.compareAndSet(this, 0, INIT_FLAG))
            checkComplete();

        return this;
    }

    /**
     * Check completeness of the future.
     */
    private void checkComplete() {
        if (initialized() && !isDone() && lsnrCalls == size) {
            try {
                onDone(rdc != null ? rdc.reduce() : null);
            }
            catch (RuntimeException e) {
                logError(logger(), "Failed to execute compound future reducer: " + this, e);

                onDone(e);
            }
            catch (AssertionError e) {
                logError(logger(), "Failed to execute compound future reducer: " + this, e);

                onDone(e);

                throw e;
            }
        }
    }

    /**
     * @param log IgniteLogger.
     * @param msg ShortMessage.
     * @param e Exception.
     */
    protected void logError(IgniteLogger log, String msg, Throwable e) {
        U.error(log, msg, e);
    }

    /**
     * @param log IgniteLogger.
     * @param msg ShortMessage.
     */
    protected void logDebug(IgniteLogger log, String msg) {
        if (log != null && log.isDebugEnabled())
            log.debug(msg);
    }

    /**
     * Returns future at the specified position in this list.
     *
     * @param idx Index of the element to return
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected IgniteInternalFuture<T> future(int idx) {
        assert futs != null && idx >= 0 && idx < size;

        if (futs instanceof IgniteInternalFuture) {
            assert idx == 0;

            return (IgniteInternalFuture<T>)futs;
        }
        else
            return ((List<IgniteInternalFuture>)futs).get(idx);
    }

    /**
     * @return Futures size.
     */
    protected final int futuresCountNoLock() {
        return size;
    }

    /**
     * @return {@code True} if has at least one future.
     */
    protected final boolean hasFutures() {
        return futs != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCompoundFuture.class, this,
            "done", isDone(),
            "cancelled", isCancelled(),
            "err", error(),
            "futs",
            F.viewReadOnly(futures(), new C1<IgniteInternalFuture<T>, String>() {
                @Override public String apply(IgniteInternalFuture<T> f) {
                    return Boolean.toString(f.isDone());
                }
            })
        );
    }
}
