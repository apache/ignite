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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Future composed of multiple inner futures.
 */
public class GridCompoundFuture<T, R> extends GridFutureAdapter<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Futures. */
    private final ConcurrentLinkedDeque8<IgniteInternalFuture<T>> futs = new ConcurrentLinkedDeque8<>();

    /** Pending futures. */
    private final Collection<IgniteInternalFuture<T>> pending = new ConcurrentLinkedDeque8<>();

    /** Listener call count. */
    private final AtomicInteger lsnrCalls = new AtomicInteger();

    /** Finished flag. */
    private final AtomicBoolean finished = new AtomicBoolean();

    /** Reducer. */
    @GridToStringInclude
    private IgniteReducer<T, R> rdc;

    /** Initialize flag. */
    private AtomicBoolean init = new AtomicBoolean(false);

    /** Result with a flag to control if reducer has been called. */
    private AtomicMarkableReference<R> res = new AtomicMarkableReference<>(null, false);

    /** Exceptions to ignore. */
    private Class<? extends Throwable>[] ignoreChildFailures;

    /** Error. */
    private AtomicReference<Throwable> err = new AtomicReference<>();

    /**
     *
     */
    public GridCompoundFuture() {
        // No-op.
    }

    /**
     * @param rdc Reducer.
     */
    public GridCompoundFuture(@Nullable IgniteReducer<T, R> rdc) {
        this.rdc = rdc;
    }

    /**
     * @param rdc Reducer to add.
     * @param futs Futures to add.
     */
    public GridCompoundFuture(
        @Nullable IgniteReducer<T, R> rdc,
        @Nullable Iterable<IgniteInternalFuture<T>> futs
    ) {
        this.rdc = rdc;

        addAll(futs);

        markInitialized();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        if (onCancelled()) {
            for (IgniteInternalFuture<T> fut : futs)
                fut.cancel();

            return true;
        }

        return false;
    }

    /**
     * Gets collection of futures.
     *
     * @return Collection of futures.
     */
    public Collection<IgniteInternalFuture<T>> futures() {
        return futs;
    }

    /**
     * Gets pending (unfinished) futures.
     *
     * @return Pending futures.
     */
    public Collection<IgniteInternalFuture<T>> pending() {
        return pending;
    }

    /**
     * @param ignoreChildFailures Flag indicating whether compound future should ignore child futures failures.
     */
    @SafeVarargs
    public final void ignoreChildFailures(Class<? extends Throwable>... ignoreChildFailures) {
        this.ignoreChildFailures = ignoreChildFailures;
    }

    /**
     * Checks if there are pending futures. This is not the same as
     * {@link #isDone()} because child classes may override {@link #onDone(Object, Throwable)}
     * call and delay completion.
     *
     * @return {@code True} if there are pending futures.
     */
    public boolean hasPending() {
        return !pending.isEmpty();
    }

    /**
     * @return {@code True} if this future was initialized. Initialization happens when
     *      {@link #markInitialized()} method is called on future.
     */
    public boolean initialized() {
        return init.get();
    }

    /**
     * Adds a future to this compound future.
     *
     * @param fut Future to add.
     */
    public void add(IgniteInternalFuture<T> fut) {
        assert fut != null;

        pending.add(fut);
        futs.add(fut);

        fut.listen(new Listener());

        if (isCancelled())
            try {
                fut.cancel();
            }
            catch (IgniteCheckedException e) {
                onDone(e);
            }
    }

    /**
     * Adds futures to this compound future.
     *
     * @param futs Futures to add.
     */
    public void addAll(@Nullable IgniteInternalFuture<T>... futs) {
        addAll(F.asList(futs));
    }

    /**
     * Adds futures to this compound future.
     *
     * @param futs Futures to add.
     */
    public void addAll(@Nullable Iterable<IgniteInternalFuture<T>> futs) {
        if (futs != null)
            for (IgniteInternalFuture<T> fut : futs)
                add(fut);
    }

    /**
     * Gets optional reducer.
     *
     * @return Optional reducer.
     */
    @Nullable public IgniteReducer<T, R> reducer() {
        return rdc;
    }

    /**
     * Sets optional reducer.
     *
     * @param rdc Optional reducer.
     */
    public void reducer(@Nullable IgniteReducer<T, R> rdc) {
        this.rdc = rdc;
    }

    /**
     * Mark this future as initialized.
     */
    public void markInitialized() {
        if (init.compareAndSet(false, true))
            // Check complete to make sure that we take care
            // of all the ignored callbacks.
            checkComplete();
    }

    /**
     * Check completeness of the future.
     */
    private void checkComplete() {
        Throwable err = this.err.get();

        boolean ignore = ignoreFailure(err);

        if (init.get() && (res.isMarked() || lsnrCalls.get() == futs.sizex() || (err != null && !ignore))
            && finished.compareAndSet(false, true)) {
            try {
                if (err == null && rdc != null && !res.isMarked())
                    res.compareAndSet(null, rdc.reduce(), false, true);
            }
            catch (RuntimeException e) {
                U.error(null, "Failed to execute compound future reducer: " + this, e);

                onDone(e);

                return;
            }
            catch (AssertionError e) {
                U.error(null, "Failed to execute compound future reducer: " + this, e);

                onDone(e);

                throw e;
            }

            onDone(res.getReference(), ignore ? null : err);
        }
    }

    /**
     * Checks if this compound future should ignore this particular exception.
     *
     * @param err Exception to check.
     * @return {@code True} if this error should be ignored.
     */
    private boolean ignoreFailure(@Nullable Throwable err) {
        if (err == null)
            return true;

        if (ignoreChildFailures != null) {
            for (Class<? extends Throwable> ignoreCls : ignoreChildFailures) {
                if (ignoreCls.isAssignableFrom(err.getClass()))
                    return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCompoundFuture.class, this,
            "done", isDone(),
            "cancelled", isCancelled(),
            "err", error(),
            "futs",
                F.viewReadOnly(futs, new C1<IgniteInternalFuture<T>, String>() {
                    @Override public String apply(IgniteInternalFuture<T> f) {
                        return Boolean.toString(f.isDone());
                    }
                })
        );
    }

    /**
     * Listener for futures.
     */
    private class Listener implements IgniteInClosure<IgniteInternalFuture<T>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture<T> fut) {
            pending.remove(fut);

            try {
                T t = fut.get();

                try {
                    if (rdc != null && !rdc.collect(t) && !res.isMarked())
                        res.compareAndSet(null, rdc.reduce(), false, true);
                }
                catch (RuntimeException e) {
                    U.error(null, "Failed to execute compound future reducer: " + this, e);

                    // Exception in reducer is a bug, so we bypass checkComplete here.
                    onDone(e);
                }
                catch (AssertionError e) {
                    U.error(null, "Failed to execute compound future reducer: " + this, e);

                    // Bypass checkComplete because need to rethrow.
                    onDone(e);

                    throw e;
                }
            }
            catch (IgniteTxOptimisticCheckedException | IgniteFutureCancelledCheckedException |
                ClusterTopologyCheckedException e) {
                err.compareAndSet(null, e);
            }
            catch (IgniteCheckedException e) {
                if (!ignoreFailure(e))
                    U.error(null, "Failed to execute compound future reducer: " + this, e);

                err.compareAndSet(null, e);
            }
            catch (RuntimeException e) {
                U.error(null, "Failed to execute compound future reducer: " + this, e);

                err.compareAndSet(null, e);
            }
            catch (AssertionError e) {
                U.error(null, "Failed to execute compound future reducer: " + this, e);

                // Bypass checkComplete because need to rethrow.
                onDone(e);

                throw e;
            }

            lsnrCalls.incrementAndGet();

            checkComplete();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Compound future listener: " + GridCompoundFuture.this;
        }
    }
}