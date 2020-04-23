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

package org.apache.ignite.internal.client.thin;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Implementation of {@link Future} for thin-client operations.
 */
class ClientFutureImpl<R> implements Future<R> {
    /** Delegate. */
    private final GridFutureAdapter<R> delegate;

    /**
     * Default constructor.
     *
     * @param delegate Delegate internal future.
     */
    ClientFutureImpl(GridFutureAdapter<R> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public R get() throws ExecutionException, CancellationException, InterruptedException {
        try {
            return delegate.get();
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new InterruptedException(e.getMessage());
        }
        catch (IgniteFutureCancelledCheckedException e) {
            throw new CancellationException(e.getMessage());
        }
        catch (Exception e) {
            throw new ExecutionException(unwrapException(e));
        }
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit)
        throws ExecutionException, TimeoutException, CancellationException, InterruptedException {
        try {
            return delegate.get(timeout, unit);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new InterruptedException(e.getMessage());
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            throw new TimeoutException(e.getMessage());
        }
        catch (IgniteFutureCancelledCheckedException e) {
            throw new CancellationException(e.getMessage());
        }
        catch (Exception e) {
            throw new ExecutionException(unwrapException(e));
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return delegate.isDone();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel(boolean mayInterruptIfRunning) throws ClientException {
        try {
            return mayInterruptIfRunning ? delegate.cancel() : delegate.onCancelled();
        }
        catch (IgniteCheckedException e) {
            throw unwrapException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return delegate.isCancelled();
    }

    /**
     * Unwraps checked exception to client exception.
     *
     * @param e Exception.
     */
    private static ClientException unwrapException(Exception e) {
        Throwable e0 = e instanceof IgniteCheckedException && e.getCause() != null ? e.getCause() : e;

        if (e0 instanceof ClientException)
            return (ClientException)e0;

        return new ClientException(e0);
    }
}
