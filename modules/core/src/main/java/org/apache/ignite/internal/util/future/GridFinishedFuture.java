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

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * Future that is completed at creation time.
 */
public class GridFinishedFuture<T> implements IgniteFuture<T>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Synchronous notification flag. */
    private static final boolean SYNC_NOTIFY = IgniteSystemProperties.getBoolean(GG_FUT_SYNC_NOTIFICATION, true);

    /** Complete value. */
    private T t;

    /** Error. */
    private Throwable err;

    /** Context. */
    protected GridKernalContext ctx;

    /** Start time. */
    private final long startTime = U.currentTimeMillis();

    /** Synchronous notification flag. */
    private volatile boolean syncNotify = SYNC_NOTIFY;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridFinishedFuture() {
        // No-op.
    }

    /**
     * Creates finished future with complete value.
     *
     * @param ctx Context.
     */
    public GridFinishedFuture(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;

        t = null;
        err = null;
    }

    /**
     * Creates finished future with complete value.
     *
     * @param ctx Context.
     * @param t Finished value.
     */
    public GridFinishedFuture(GridKernalContext ctx, T t) {
        assert ctx != null;

        this.ctx = ctx;
        this.t = t;

        err = null;
    }

    /**
     * @param ctx Context.
     * @param err Future error.
     */
    public GridFinishedFuture(GridKernalContext ctx, Throwable err) {
        assert ctx != null;

        this.ctx = ctx;
        this.err = err;

        t = null;
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean concurrentNotify() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void concurrentNotify(boolean concurNotify) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void syncNotify(boolean syncNotify) {
        this.syncNotify = syncNotify;
    }

    /** {@inheritDoc} */
    @Override public boolean syncNotify() {
        return syncNotify;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public T get() throws IgniteCheckedException {
        if (err != null)
            throw U.cast(err);

        return t;
    }

    /** {@inheritDoc} */
    @Override public T get(long timeout) throws IgniteCheckedException {
        return get();
    }

    /** {@inheritDoc} */
    @Override public T get(long timeout, TimeUnit unit) throws IgniteCheckedException {
        return get();
    }

    /** {@inheritDoc} */
    @Override public void listenAsync(final IgniteInClosure<? super IgniteFuture<T>> lsnr) {
        if (ctx == null)
            throw new IllegalStateException("Cannot attach listener to deserialized future (context is null): " + this);

        if (lsnr != null) {
            if (syncNotify)
                lsnr.apply(this);
            else
                ctx.closure().runLocalSafe(new GPR() {
                    @Override public void run() {
                        lsnr.apply(GridFinishedFuture.this);
                    }
                }, true);
        }
    }

    /** {@inheritDoc} */
    @Override public void stopListenAsync(@Nullable IgniteInClosure<? super IgniteFuture<T>>... lsnr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> chain(final IgniteClosure<? super IgniteFuture<T>, R> doneCb) {
        GridFutureAdapter<R> fut = new GridFutureAdapter<R>(ctx, syncNotify) {
            @Override public String toString() {
                return "ChainFuture[orig=" + GridFinishedFuture.this + ", doneCb=" + doneCb + ']';
            }
        };

        listenAsync(new GridFutureChainListener<>(ctx, fut, doneCb));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(t);
        out.writeObject(err);
        out.writeObject(ctx);
        out.writeBoolean(syncNotify);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        t = (T)in.readObject();
        err = (Throwable)in.readObject();
        ctx = (GridKernalContext)in.readObject();
        syncNotify = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFinishedFuture.class, this);
    }
}
