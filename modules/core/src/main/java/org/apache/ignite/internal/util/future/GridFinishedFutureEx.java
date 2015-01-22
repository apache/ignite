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
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Future that is completed at creation time. This future is different from
 * {@link GridFinishedFuture} as it does not take context as a parameter and
 * performs notifications in the same thread.
 */
public class GridFinishedFutureEx<T> implements IgniteFuture<T>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Complete value. */
    private T t;

    /** Error. */
    private Throwable err;

    /** Start time. */
    private final long startTime = U.currentTimeMillis();

    /**
     * Created finished future with {@code null} value.
     */
    public GridFinishedFutureEx() {
        this(null, null);
    }

    /**
     * Creates finished future with complete value.
     *
     * @param t Finished value.
     */
    public GridFinishedFutureEx(T t) {
        this(t, null);
    }

    /**
     * @param err Future error.
     */
    public GridFinishedFutureEx(Throwable err) {
        this(null, err);
    }

    /**
     * Creates finished future with complete value and error.
     *
     * @param t Finished value.
     * @param err Future error.
     */
    public GridFinishedFutureEx(T t, Throwable err) {
        this.err = err;

        this.t = t;
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
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean syncNotify() {
        return false;
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
    @Override public <R> IgniteFuture<R> chain(IgniteClosure<? super IgniteFuture<T>, R> doneCb) {
        try {
            return new GridFinishedFutureEx<>(doneCb.apply(this));
        }
        catch (GridClosureException e) {
            return new GridFinishedFutureEx<>(U.unwrap(e));
        }
        catch (RuntimeException | Error e) {
            U.warn(null, "Failed to notify chained future [doneCb=" + doneCb + ", err=" + e.getMessage() + ']');

            throw e;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void listenAsync(IgniteInClosure<? super IgniteFuture<T>> lsnr) {
        if (lsnr != null)
            lsnr.apply(this);
    }

    /** {@inheritDoc} */
    @Override public void stopListenAsync(@Nullable IgniteInClosure<? super IgniteFuture<T>>... lsnr) {
        // No-op.
    }

    /**
     * @return {@code True} if future failed.
     */
    protected boolean failed() {
        return err != null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(t);
        out.writeObject(err);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        t = (T)in.readObject();
        err = (Throwable)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFinishedFutureEx.class, this);
    }
}
