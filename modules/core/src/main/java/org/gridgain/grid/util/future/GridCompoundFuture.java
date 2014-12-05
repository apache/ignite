/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Future composed of multiple inner futures.
 */
public class GridCompoundFuture<T, R> extends GridFutureAdapter<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Futures. */
    private final ConcurrentLinkedDeque8<IgniteFuture<T>> futs = new ConcurrentLinkedDeque8<>();

    /** Pending futures. */
    private final Collection<IgniteFuture<T>> pending = new ConcurrentLinkedDeque8<>();

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
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCompoundFuture() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    public GridCompoundFuture(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param ctx Context.
     * @param rdc Reducer.
     */
    public GridCompoundFuture(GridKernalContext ctx, @Nullable IgniteReducer<T, R> rdc) {
        super(ctx);

        this.rdc = rdc;
    }

    /**
     * @param ctx Context.
     * @param rdc Reducer to add.
     * @param futs Futures to add.
     */
    public GridCompoundFuture(GridKernalContext ctx, @Nullable IgniteReducer<T, R> rdc,
        @Nullable Iterable<IgniteFuture<T>> futs) {
        super(ctx);

        this.rdc = rdc;

        addAll(futs);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws GridException {
        if (onCancelled()) {
            for (IgniteFuture<T> fut : futs)
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
    public Collection<IgniteFuture<T>> futures() {
        return futs;
    }

    /**
     * Gets pending (unfinished) futures.
     *
     * @return Pending futures.
     */
    public Collection<IgniteFuture<T>> pending() {
        return pending;
    }

    /**
     * @param ignoreChildFailures Flag indicating whether compound future should ignore child futures failures.
     */
    public void ignoreChildFailures(Class<? extends Throwable>... ignoreChildFailures) {
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
    public void add(IgniteFuture<T> fut) {
        assert fut != null;

        pending.add(fut);
        futs.add(fut);

        fut.listenAsync(new Listener());

        if (isCancelled())
            try {
                fut.cancel();
            }
            catch (GridException e) {
                onDone(e);
            }
    }

    /**
     * Adds futures to this compound future.
     *
     * @param futs Futures to add.
     */
    public void addAll(@Nullable IgniteFuture<T>... futs) {
        addAll(F.asList(futs));
    }

    /**
     * Adds futures to this compound future.
     *
     * @param futs Futures to add.
     */
    public void addAll(@Nullable Iterable<IgniteFuture<T>> futs) {
        if (futs != null)
            for (IgniteFuture<T> fut : futs)
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
                U.error(log, "Failed to execute compound future reducer: " + this, e);

                onDone(e);

                return;
            }
            catch (AssertionError e) {
                U.error(log, "Failed to execute compound future reducer: " + this, e);

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
                F.viewReadOnly(futs, new C1<IgniteFuture<T>, String>() {
                    @Override public String apply(IgniteFuture<T> f) {
                        return Boolean.toString(f.isDone());
                    }
                })
        );
    }

    /**
     * Listener for futures.
     */
    private class Listener implements IgniteInClosure<IgniteFuture<T>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void apply(IgniteFuture<T> fut) {
            pending.remove(fut);

            try {
                T t = fut.get();

                try {
                    if (rdc != null && !rdc.collect(t) && !res.isMarked())
                        res.compareAndSet(null, rdc.reduce(), false, true);
                }
                catch (RuntimeException e) {
                    U.error(log, "Failed to execute compound future reducer: " + this, e);

                    // Exception in reducer is a bug, so we bypass checkComplete here.
                    onDone(e);
                }
                catch (AssertionError e) {
                    U.error(log, "Failed to execute compound future reducer: " + this, e);

                    // Bypass checkComplete because need to rethrow.
                    onDone(e);

                    throw e;
                }
            }
            catch (GridCacheTxOptimisticException e) {
                if (log.isDebugEnabled())
                    log.debug("Optimistic failure [fut=" + GridCompoundFuture.this + ", err=" + e + ']');

                err.compareAndSet(null, e);
            }
            catch (GridTopologyException e) {
                if (log.isDebugEnabled())
                    log.debug("Topology exception [fut=" + GridCompoundFuture.this + ", err=" + e + ']');

                err.compareAndSet(null, e);
            }
            catch (IgniteFutureCancelledException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to execute compound future reducer [lsnr=" + this + ", e=" + e + ']');

                err.compareAndSet(null, e);
            }
            catch (GridException e) {
                if (!ignoreFailure(e))
                    U.error(log, "Failed to execute compound future reducer: " + this, e);

                err.compareAndSet(null, e);
            }
            catch (RuntimeException e) {
                U.error(log, "Failed to execute compound future reducer: " + this, e);

                err.compareAndSet(null, e);
            }
            catch (AssertionError e) {
                U.error(log, "Failed to execute compound future reducer: " + this, e);

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
