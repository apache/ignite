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

package org.apache.ignite.ml.environment.parallelism;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.jetbrains.annotations.NotNull;

/**
 * All task should be processed by default thread pool.
 */
public class DefaultParallelismStrategy implements ParallelismStrategy {
    /** Strategy Pool. */
    private ExecutorService pool = Executors.newWorkStealingPool(4);

    /** {@inheritDoc} */
    @Override public <T> Promise<T> submit(IgniteSupplier<T> task) {
        return new FutureWrapper<>(pool.submit(task::get));
    }

    /**
     * Wrapper for future class.
     *
     * @param <T>
     */
    public static class FutureWrapper<T> implements Promise<T> {
        private final Future<T> f;

        /**
         * Create an instance of FutureWrapper.
         *
         * @param f Future.
         */
        public FutureWrapper(Future<T> f) {
            this.f = f;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            return f.cancel(mayInterruptIfRunning);
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return f.isCancelled();
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return f.isDone();
        }

        /** {@inheritDoc} */
        @Override public T get() throws InterruptedException, ExecutionException {
            return f.get();
        }

        /** {@inheritDoc} */
        @Override public T get(long timeout,
            @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

            return f.get(timeout, unit);
        }
    }
}
