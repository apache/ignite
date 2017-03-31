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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of public API future.
 */
public class IgniteFutureImpl<V> implements IgniteFuture<V> {
    /** */
    protected final IgniteInternalFuture<V> fut;

    /** Default executor. */
    protected Executor dfltExec;

    /**
     * @param fut Future.
     * @param ctx Kernal context.
     */
    public IgniteFutureImpl(IgniteInternalFuture<V> fut, GridKernalContext ctx) {
        assert fut != null;
        assert ctx != null;

        this.fut = fut;
        this.dfltExec = ctx.getExecutorService();
    }

    /**
     * @param fut Internal future.
     * @param exec Default executor for async operations.
     */
    protected IgniteFutureImpl(IgniteInternalFuture<V> fut, Executor exec) {
        assert fut != null;
        assert exec != null;

        this.fut = fut;
        this.dfltExec = exec;
    }

    /**
     * @return Internal future.
     */
    public IgniteInternalFuture<V> internalFuture() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return fut.startTime();
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        return fut.duration();
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return fut.isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return fut.isDone();
    }

    /** {@inheritDoc} */
    @Override public void listen(IgniteInClosure<? super IgniteFuture<V>> lsnr) {
        A.notNull(lsnr, "lsnr");

        fut.listen(new InternalFutureListener(lsnr));
    }

    /** {@inheritDoc} */
    @Override public void listenAsync(IgniteInClosure<? super IgniteFuture<V>> lsnr, @Nullable Executor exec) {
        A.notNull(lsnr, "lsnr");

        fut.listenAsync(new InternalFutureListener(lsnr), exec == null ? dfltExec : exec);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> chain(final IgniteClosure<? super IgniteFuture<V>, T> doneCb) {
        return new IgniteFutureImpl<>(chainInternal(doneCb), dfltExec);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> chainAsync(IgniteClosure<? super IgniteFuture<V>, T> doneCb, @Nullable Executor exec) {
        return new IgniteFutureImpl<>(chainInternalAsync(doneCb, exec == null ? dfltExec : exec), dfltExec);
    }

    /**
     * @param doneCb Done callback.
     * @return Internal future
     */
    protected  <T> IgniteInternalFuture<T> chainInternal(final IgniteClosure<? super IgniteFuture<V>, T> doneCb) {
        return fut.chain(new C1<IgniteInternalFuture<V>, T>() {
            @Override public T apply(IgniteInternalFuture<V> fut) {
                assert IgniteFutureImpl.this.fut == fut;

                try {
                    return doneCb.apply(IgniteFutureImpl.this);
                }
                catch (Exception e) {
                    throw new GridClosureException(e);
                }
            }
        });
    }

    /**
     * @param doneCb Done callback.
     * @param exec Executor.
     */
    protected <T> IgniteInternalFuture<T> chainInternalAsync(final IgniteClosure<? super IgniteFuture<V>, T> doneCb, Executor exec) {
        return fut.chainAsync(new C1<IgniteInternalFuture<V>, T>() {
            @Override public T apply(IgniteInternalFuture<V> fut) {
                assert IgniteFutureImpl.this.fut == fut;

                try {
                    return doneCb.apply(IgniteFutureImpl.this);
                }
                catch (Exception e) {
                    throw new GridClosureException(e);
                }
            }
        }, exec);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteException {
        try {
            return fut.cancel();
        }
        catch (IgniteCheckedException e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V get() {
        try {
            return fut.get();
        }
        catch (IgniteCheckedException e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(long timeout) {
        try {
            return fut.get(timeout);
        }
        catch (IgniteCheckedException e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(long timeout, TimeUnit unit) {
        try {
            return fut.get(timeout, unit);
        }
        catch (IgniteCheckedException e) {
            throw convertException(e);
        }
    }

    /**
     * Convert internal exception to public exception.
     *
     * @param e Internal exception.
     * @return Public excpetion.
     */
    protected RuntimeException convertException(IgniteCheckedException e) {
        return U.convertException(e);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteFuture [orig=" + fut + ']';
    }

    /**
     *
     */
    private class InternalFutureListener implements IgniteInClosure<IgniteInternalFuture<V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteInClosure<? super IgniteFuture<V>> lsnr;

        /**
         * @param lsnr Wrapped listener.
         */
        private InternalFutureListener(IgniteInClosure<? super IgniteFuture<V>> lsnr) {
            assert lsnr != null;

            this.lsnr = lsnr;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return lsnr.hashCode();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public boolean equals(Object obj) {
            if (obj == null || !obj.getClass().equals(InternalFutureListener.class))
                return false;

            InternalFutureListener lsnr0 = (InternalFutureListener)obj;

            return lsnr.equals(lsnr0.lsnr);
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture<V> fut) {
            assert IgniteFutureImpl.this.fut == fut;

            lsnr.apply(IgniteFutureImpl.this);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return lsnr.toString();
        }
    }
}