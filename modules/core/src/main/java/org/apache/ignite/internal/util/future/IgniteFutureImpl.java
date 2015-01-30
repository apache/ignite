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
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/**
 * Implementation of public API future.
 */
public class IgniteFutureImpl<V> implements IgniteFuture<V> {
    /** */
    protected final IgniteInternalFuture<V> fut;

    /**
     * @param fut Future.
     */
    public IgniteFutureImpl(IgniteInternalFuture<V> fut) {
        assert fut != null;

        this.fut = fut;
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
    @Override public void syncNotify(boolean syncNotify) {
        fut.syncNotify(syncNotify);
    }

    /** {@inheritDoc} */
    @Override public boolean syncNotify() {
        return fut.syncNotify();
    }

    /** {@inheritDoc} */
    @Override public void concurrentNotify(boolean concurNotify) {
        fut.concurrentNotify(concurNotify);
    }

    /** {@inheritDoc} */
    @Override public boolean concurrentNotify() {
        return fut.concurrentNotify();
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
    @Override public void listenAsync(@Nullable final IgniteInClosure<? super IgniteFuture<V>> lsnr) {
        if (lsnr != null)
            fut.listenAsync(new InternalFutureListener(lsnr));
    }

    /** {@inheritDoc} */
    @Override public void stopListenAsync(IgniteInClosure<? super IgniteFuture<V>>... lsnrs) {
        for (IgniteInClosure<? super IgniteFuture<V>> lsnr : lsnrs) {
            if (lsnr != null)
                fut.stopListenAsync(new InternalFutureListener(lsnr));
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> chain(final IgniteClosure<? super IgniteFuture<V>, T> doneCb) {
        IgniteInternalFuture<T> fut0 = fut.chain(new C1<IgniteInternalFuture<V>, T>() {
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

        return new IgniteFutureImpl<>(fut0);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteException {
        try {
            return fut.cancel();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        return mayInterruptIfRunning && cancel();
    }

    /** {@inheritDoc} */
    @Override public V get() {
        try {
            return fut.get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(long timeout) {
        try {
            return fut.get(timeout);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(long timeout, TimeUnit unit) {
        try {
            return fut.get(timeout, unit);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
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
