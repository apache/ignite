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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Adapter from Ignite internal future to Java {@link Future} with an ability to convert result.
 */
public class IgniteJavaFuture<X, Y> implements Future<Y> {
    /** */
    private final IgniteInternalFuture<X> fut;

    /**
     * @param fut Future.
     */
    public IgniteJavaFuture(IgniteInternalFuture<X> fut) {
        this.fut = fut;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        try {
            return fut.cancel();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return fut.isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return fut.isDone();
    }

    /**
     * @param x Initial result.
     * @return Final result.
     */
    @SuppressWarnings("unchecked")
    protected Y convert(X x) {
        return (Y)x;
    }

    /** {@inheritDoc} */
    @Override public Y get() throws InterruptedException, ExecutionException {
        try {
            return convert(fut.get());
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new InterruptedException(e.getMessage());
        }
        catch (IgniteCheckedException e) {
            throw new ExecutionException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Y get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return convert(fut.get(timeout, unit));
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            throw new TimeoutException(e.getMessage());
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new InterruptedException(e.getMessage());
        }
        catch (IgniteCheckedException e) {
            throw new ExecutionException(e);
        }
    }
}
