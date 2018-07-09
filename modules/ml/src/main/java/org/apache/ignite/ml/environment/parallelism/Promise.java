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

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;

public interface Promise<T> extends Future<T> {
    public default T unsafeGet() {
        try {
            return get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public default Optional<T> getOpt() {
        try {
            return Optional.of(get());
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }
    }

    public class FutureWrapper<T> implements Promise<T> {
        private final Future<T> fut;

        public FutureWrapper(Future<T> fut) {
            this.fut = fut;
        }

        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            return fut.cancel(mayInterruptIfRunning);
        }

        @Override public boolean isCancelled() {
            return fut.isCancelled();
        }

        @Override public boolean isDone() {
            return fut.isDone();
        }

        @Override public T get() throws InterruptedException, ExecutionException {
            return fut.get();
        }

        @Override public T get(long timeout,
            @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

            return fut.get(timeout, unit);
        }
    }

    public class Stub<T> implements Promise<T> {
        private T result;

        public Stub(T result) {
            this.result = result;
        }

        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override public boolean isCancelled() {
            return false;
        }

        @Override public boolean isDone() {
            return true;
        }

        @Override public T get() throws InterruptedException, ExecutionException {
            return result;
        }

        @Override public T get(long timeout,
            @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

            return result;
        }
    }
}
