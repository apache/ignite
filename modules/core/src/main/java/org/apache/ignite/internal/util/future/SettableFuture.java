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

import java.util.concurrent.*;

/**
 * Simple implementation of {@link Future}
 */
public class SettableFuture<T> implements Future<T> {
    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** Result of computation. */
    private T res;

    /** Exception threw during the computation. */
    private ExecutionException err;

    /** {@inheritDoc} */
    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return latch.getCount() == 0;
    }

    /** {@inheritDoc} */
    @Override public T get() throws InterruptedException, ExecutionException {
        latch.await();

        if (err != null)
            throw err;

        return res;
    }

    /** {@inheritDoc} */
    @Override public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
        TimeoutException {

        if (!latch.await(timeout, unit))
            throw new TimeoutException();

        if (err != null)
            throw err;

        return res;
    }

    /**
     * Computation is done successful.
     *
     * @param res Result of computation.
     */
    public void set(T res) {
        this.res = res;

        latch.countDown();
    }

    /**
     * Computation failed.
     *
     * @param throwable Error.
     */
    public void setException(Throwable throwable) {
        err = new ExecutionException(throwable);

        latch.countDown();
    }
}
