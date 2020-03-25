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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFuture;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;

/**
 * Implementation of {@link ClientFuture}.
 */
class ClientFutureImpl<R>  implements ClientFuture<R> {
    /** Delegate. */
    private final IgniteInternalFuture<R> delegate;

    /**
     * Default constructor.
     *
     * @param delegate Delegate internal future.
     */
    ClientFutureImpl(IgniteInternalFuture<R> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public R get() throws ClientException, CancellationException, InterruptedException {
        try {
            return delegate.get();
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new InterruptedException(e.getMessage());
        }
        catch (IgniteCheckedException e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws ClientException, TimeoutException, InterruptedException {
        try {
            return delegate.get(timeout, unit);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new InterruptedException(e.getMessage());
        }
        catch (IgniteFutureTimeoutException e) {
            throw new TimeoutException(e.getMessage());
        }
        catch (IgniteCheckedException e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return delegate.isDone();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws ClientException {
        try {
            return delegate.cancel();
        }
        catch (IgniteCheckedException ignore) {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return delegate.isCancelled();
    }

    /**
     * Converts checked exception to corresponding unchecked exception.
     *
     * @param e Exception.
     */
    private static RuntimeException convertException(IgniteCheckedException e) {
        if (e instanceof IgniteFutureCancelledCheckedException)
            return new CancellationException(e.getMessage());
        else if (e.getCause() instanceof ClientException)
            return (RuntimeException)e.getCause();
        else if (e.getCause() != null)
            return new ClientException(e.getMessage(), e.getCause());
        else
            return new ClientException(e.getMessage(), e);
    }
}