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
package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.NotNull;

public class ChainedFuture<K, V> implements Future<V> {
    private final Future<K> innerFuture;
    private final IgniteClosure<K, V> transformer;
    private volatile V transformedValue;

    public ChainedFuture(Future<K> innerFuture, IgniteClosure<K, V> transformer) {
        this.innerFuture = innerFuture;
        this.transformer = transformer;
    }

    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        return innerFuture.cancel(mayInterruptIfRunning);
    }

    @Override public boolean isCancelled() {
        return innerFuture.isCancelled();
    }

    @Override public boolean isDone() {
        return innerFuture.isDone();
    }

    @Override public V get() throws InterruptedException, ExecutionException {
        if (transformedValue == null) {
            synchronized (this) {
                if (transformedValue == null) {
                    final K k = innerFuture.get();
                    this.transformedValue = transformer.apply(k);
                }
            }
        }
        return transformedValue;
    }

    @Override public V get(long timeout,
        @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (transformedValue == null) {
            synchronized (this) {
                if (transformedValue == null) {
                    final K k = innerFuture.get(timeout, unit);
                    this.transformedValue = transformer.apply(k);
                }
            }
        }
        return transformedValue;
    }
}
