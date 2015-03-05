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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.util.concurrent.*;

/**
 * Future that is completed at creation time.
 */
public class GridFinishedFuture<T> implements IgniteInternalFuture<T> {
    /** Complete value. */
    private T t;

    /** Error. */
    private Throwable err;

    /** Start time. */
    private final long startTime = U.currentTimeMillis();

    /**
     * Creates finished future with complete value.
     */
    public GridFinishedFuture() {
        // No-op.
    }

    /**
     * Creates finished future with complete value.
     *
     * @param t Finished value.
     */
    public GridFinishedFuture(T t) {
        this.t = t;
    }

    /**
     * @param err Future error.
     */
    public GridFinishedFuture(Throwable err) {
        this.err = err;
    }

    /**
     * @return Value of error.
     */
    protected Throwable error() {
        return err;
    }

    /**
     * @return Value of result.
     */
    protected T result() {
        return t;
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
    @Override public void listen(final IgniteInClosure<? super IgniteInternalFuture<T>> lsnr) {
        if (lsnr != null)
            lsnr.apply(this);
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteInternalFuture<R> chain(final IgniteClosure<? super IgniteInternalFuture<T>, R> doneCb) {
        GridFutureAdapter<R> fut = new GridFutureAdapter<R>() {
            @Override public String toString() {
                return "ChainFuture[orig=" + GridFinishedFuture.this + ", doneCb=" + doneCb + ']';
            }
        };

        listen(new GridFutureChainListener<>(fut, doneCb));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFinishedFuture.class, this);
    }
}
