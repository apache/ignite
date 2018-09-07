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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.jetbrains.annotations.NotNull;

/**
 * All tasks should be processed in one thread.
 */
public class NoParallelismStrategy implements ParallelismStrategy {
    /** Instance. */
    public static final ParallelismStrategy INSTANCE = new NoParallelismStrategy();

    /** */
    private NoParallelismStrategy() {

    }

    /** {@inheritDoc} */
    @Override public <T>  Promise<T> submit(IgniteSupplier<T> task) {
        return new Stub<>(task.get());
    }

    /**
     * Stub for Future interface implementation.
     *
     * @param <T> Type of result.
     */
    public static class Stub<T> implements Promise<T> {
        private T result;

        /**
         * Create an instance of Stub
         *
         * @param result Execution result.
         */
        public Stub(T result) {
            this.result = result;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel(boolean mayInterruptIfRunning) {
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
        @Override public T get() throws InterruptedException, ExecutionException {
            return result;
        }

        /** {@inheritDoc} */
        @Override public T get(long timeout,
            @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

            return result;
        }
    }
}
