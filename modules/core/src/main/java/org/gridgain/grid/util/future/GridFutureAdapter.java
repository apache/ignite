/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Future adapter.
 */
public class GridFutureAdapter<R> extends AbstractQueuedSynchronizer implements GridFuture<R>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static GridLogger log;

    /** Synchronous notification flag. */
    private static final boolean SYNC_NOTIFY = U.isFutureNotificationSynchronous(true);

    /** Concurrent notification flag. */
    private static final boolean CONCUR_NOTIFY = U.isFutureNotificationConcurrent(false);

    /** Initial state. */
    private static final int INIT = 0;

    /** Cancelled state. */
    private static final int CANCELLED = 1;

    /** Done state. */
    private static final int DONE = 2;

    /** Result. */
    @GridToStringInclude
    private R res;

    /** Error. */
    private Throwable err;

    /** Future start time. */
    private final long startTime = U.currentTimeMillis();

    /** Future end time. */
    private volatile long endTime;

    /** Set to {@code false} on deserialization whenever incomplete future is serialized. */
    private boolean valid = true;

    /** Asynchronous listeners. */
    private Collection<GridInClosure<? super GridFuture<R>>> lsnrs;

    /** Context. */
    protected GridKernalContext ctx;

    /** Synchronous notification flag. */
    private volatile boolean syncNotify = SYNC_NOTIFY;

    /** Concurrent notification flag. */
    private volatile boolean concurNotify = CONCUR_NOTIFY;

    /** */
    private final Object mux = new Object();

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridFutureAdapter() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     */
    public GridFutureAdapter(GridKernalContext ctx) {
        this(ctx, SYNC_NOTIFY);
    }

    /**
     * @param syncNotify Synchronous notify flag.
     * @param ctx Kernal context.
     */
    public GridFutureAdapter(GridKernalContext ctx, boolean syncNotify) {
        assert ctx != null;

        this.syncNotify = syncNotify;

        this.ctx = ctx;

        log = U.logger(ctx, logRef, GridFutureAdapter.class);
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        long endTime = this.endTime;

        return endTime == 0 ? U.currentTimeMillis() - startTime : endTime - startTime;
    }

    /**
     * @return Future end time.
     */
    public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public boolean concurrentNotify() {
        return concurNotify;
    }

    /** {@inheritDoc} */
    @Override public void concurrentNotify(boolean concurNotify) {
        this.concurNotify = concurNotify;
    }

    /** {@inheritDoc} */
    @Override public boolean syncNotify() {
        return syncNotify;
    }

    /** {@inheritDoc} */
    @Override public void syncNotify(boolean syncNotify) {
        this.syncNotify = syncNotify;
    }

    /**
     * Checks that future is in usable state.
     */
    protected void checkValid() {
        if (!valid)
            throw new IllegalStateException("Incomplete future was serialized and cannot " +
                "be used after deserialization.");
    }

    /**
     * @return Valid flag.
     */
    protected boolean isValid() {
        return valid;
    }

    /**
     * @return Value of error.
     */
    protected Throwable error() {
        checkValid();

        return err;
    }

    /**
     * @return Value of result.
     */
    protected R result() {
        checkValid();

        return res;
    }

    /** {@inheritDoc} */
    @Override public R get() throws GridException {
        checkValid();

        try {
            if (endTime == 0)
                acquireSharedInterruptibly(0);

            if (getState() == CANCELLED)
                throw new GridFutureCancelledException("Future was cancelled: " + this);

            if (err != null)
                throw U.cast(err);

            return res;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout) throws GridException {
        // Do not replace with static import, as it may not compile.
        return get(timeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws GridException {
        A.ensure(timeout >= 0, "timeout cannot be negative: " + timeout);
        A.notNull(unit, "unit");

        checkValid();

        try {
            return get0(unit.toNanos(timeout));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException("Got interrupted while waiting for future to complete.", e);
        }
    }

    /**
     * @param nanosTimeout Timeout (nanoseconds).
     * @return Result.
     * @throws InterruptedException If interrupted.
     * @throws GridFutureTimeoutException If timeout reached before computation completed.
     * @throws GridException If error occurred.
     */
    @Nullable protected R get0(long nanosTimeout) throws InterruptedException, GridException {
        if (endTime == 0 && !tryAcquireSharedNanos(0, nanosTimeout))
            throw new GridFutureTimeoutException("Timeout was reached before computation completed.");

        if (getState() == CANCELLED)
            throw new GridFutureCancelledException("Future was cancelled: " + this);

        if (err != null)
            throw U.cast(err);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void listenAsync(@Nullable final GridInClosure<? super GridFuture<R>> lsnr) {
        if (lsnr != null) {
            checkValid();

            boolean done = isDone();

            if (!done) {
                synchronized (mux) {
                    done = isDone(); // Double check.

                    if (!done) {
                        if (lsnrs == null)
                            lsnrs = new ArrayList<>();

                        lsnrs.add(lsnr);
                    }
                }
            }

            if (done) {
                try {
                    if (syncNotify)
                        notifyListener(lsnr);
                    else {
                        ctx.closure().runLocalSafe(new GPR() {
                            @Override public void run() {
                                notifyListener(lsnr);
                            }
                        }, true);
                    }
                }
                catch (IllegalStateException ignore) {
                    U.warn(null, "Future notification will not proceed because grid is stopped: " + ctx.gridName());
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stopListenAsync(@Nullable GridInClosure<? super GridFuture<R>>... lsnr) {
        synchronized (mux) {
            if (lsnrs == null)
                return;

            if (lsnr == null || lsnr.length == 0)
                lsnrs.clear();
            else {
                // Iterate through the whole list, removing all occurrences, if any.
                for (Iterator<GridInClosure<? super GridFuture<R>>> it = lsnrs.iterator(); it.hasNext();) {
                    GridInClosure<? super GridFuture<R>> l1 = it.next();

                    for (GridInClosure<? super GridFuture<R>> l2 : lsnr)
                        // Must be l1.equals(l2), not l2.equals(l1), because of the way listeners are added.
                        if (l1.equals(l2))
                            it.remove();
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public <T> GridFuture<T> chain(final GridClosure<? super GridFuture<R>, T> doneCb) {
        return new ChainFuture<>(ctx, syncNotify, this, doneCb);
    }

    /**
     * Notifies all registered listeners.
     */
    private void notifyListeners() {
        final Collection<GridInClosure<? super GridFuture<R>>> lsnrs0;

        synchronized (mux) {
            lsnrs0 = lsnrs;

            if (lsnrs0 == null)
                return;

            lsnrs = null;
        }

        assert !lsnrs0.isEmpty();

        if (concurNotify) {
            for (final GridInClosure<? super GridFuture<R>> lsnr : lsnrs0)
                ctx.closure().runLocalSafe(new GPR() {
                    @Override public void run() {
                        notifyListener(lsnr);
                    }
                }, true);
        }
        else {
            // Always notify in the thread different from start thread.
            if (!syncNotify) {
                ctx.closure().runLocalSafe(new GPR() {
                    @Override public void run() {
                        // Since concurrent notifications are off, we notify
                        // all listeners in one thread.
                        for (GridInClosure<? super GridFuture<R>> lsnr : lsnrs0)
                            notifyListener(lsnr);
                    }
                }, true);
            }
            else
                for (GridInClosure<? super GridFuture<R>> lsnr : lsnrs0)
                    notifyListener(lsnr);
        }
    }

    /**
     * Notifies single listener.
     *
     * @param lsnr Listener.
     */
    private void notifyListener(GridInClosure<? super GridFuture<R>> lsnr) {
        assert lsnr != null;

        try {
            lsnr.apply(this);
        }
        catch (IllegalStateException e) {
            U.warn(null, "Failed to notify listener (is grid stopped?) [grid=" + ctx.gridName() +
                ", lsnr=" + lsnr + ", err=" + e.getMessage() + ']');
        }
        catch (RuntimeException | Error e) {
            U.error(log, "Failed to notify listener: " + lsnr, e);

            throw e;
        }
    }

    /**
     * Default no-op implementation that always returns {@code false}.
     * Futures that do support cancellation should override this method
     * and call {@link #onCancelled()} callback explicitly if cancellation
     * indeed did happen.
     */
    @Override public boolean cancel() throws GridException {
        checkValid();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        // Don't check for "valid" here, as "done" flag can be read
        // even in invalid state.
        return endTime != 0;
    }

    /**
     * @return Checks is future is completed with exception.
     */
    public boolean isFailed() {
        // Must read endTime first.
        return endTime != 0 && err != null;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        checkValid();

        return getState() == CANCELLED;
    }

    /**
     * Callback to notify that future is finished with {@code null} result.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone() {
        return onDone(null, null);
    }

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @param res Result.
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone(@Nullable R res) {
        return onDone(res, null);
    }

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @param err Error.
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone(@Nullable Throwable err) {
        return onDone(null, err);
    }

    /**
     * Callback to notify that future is finished. Note that if non-{@code null} exception is passed in
     * the result value will be ignored.
     *
     * @param res Optional result.
     * @param err Optional error.
     * @return {@code True} if result was set by this call.
     */
    public boolean onDone(@Nullable R res, @Nullable Throwable err) {
        return onDone(res, err, false);
    }

    /**
     * @param res Result.
     * @param err Error.
     * @param cancel {@code True} if future is being cancelled.
     * @return {@code True} if result was set by this call.
     */
    private boolean onDone(@Nullable R res, @Nullable Throwable err, boolean cancel) {
        checkValid();

        boolean notify = false;

        try {
            if (compareAndSetState(INIT, cancel ? CANCELLED : DONE)) {
                this.res = res;
                this.err = err;

                notify = true;

                releaseShared(0);

                return true;
            }

            return false;
        }
        finally {
            if (notify)
                notifyListeners();
        }
    }

    /**
     * Callback to notify that future is cancelled.
     *
     * @return {@code True} if cancel flag was set by this call.
     */
    public boolean onCancelled() {
        return onDone(null, null, true);
    }

    /** {@inheritDoc} */
    @Override protected final int tryAcquireShared(int ignore) {
        return endTime != 0 ? 1 : -1;
    }

    /** {@inheritDoc} */
    @Override protected final boolean tryReleaseShared(int ignore) {
        endTime = U.currentTimeMillis();

        // Always signal after setting final done status.
        return true;
    }

    /**
     * @return String representation of state.
     */
    private String state() {
        int s = getState();

        return s == INIT ? "INIT" : s == CANCELLED ? "CANCELLED" : "DONE";
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        int state0 = getState();

        out.writeByte(state0);
        out.writeBoolean(syncNotify);
        out.writeBoolean(concurNotify);

        // Don't write any further if not done, as deserialized future
        // will be invalid anyways.
        if (state0 != INIT) {
            try {
                acquireSharedInterruptibly(0);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IOException("Thread has been interrupted.", e);
            }

            out.writeObject(res);
            out.writeObject(err);
            out.writeObject(ctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int state0 = in.readByte();

        setState(state0);

        syncNotify = in.readBoolean();
        concurNotify = in.readBoolean();

        if (state0 == INIT)
            valid = false;
        else {
            res = (R)in.readObject();
            err = (Throwable)in.readObject();
            ctx = (GridKernalContext)in.readObject();

            // Prevent any thread from being locked on deserialized future.
            // This will also set 'endTime'.
            releaseShared(0);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFutureAdapter.class, this, "state", state());
    }

    /**
     *
     */
    private static class ChainFuture<R, T> extends GridFutureAdapter<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private GridFutureAdapter<R> fut;

        /** */
        private GridClosure<? super GridFuture<R>, T> doneCb;

        /**
         *
         */
        public ChainFuture() {
            // No-op.
        }

        /**
         * @param ctx Context.
         * @param syncNotify Sync notify flag.
         * @param fut Future.
         * @param doneCb Closure.
         */
        ChainFuture(GridKernalContext ctx, boolean syncNotify,
            GridFutureAdapter<R> fut, GridClosure<? super GridFuture<R>, T> doneCb) {
            super(ctx, syncNotify);

            this.fut = fut;
            this.doneCb = doneCb;

            fut.listenAsync(new GridFutureChainListener<>(ctx, this, doneCb));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ChainFuture[orig=" + fut + ", doneCb=" + doneCb + ']';
        }
    }
}
