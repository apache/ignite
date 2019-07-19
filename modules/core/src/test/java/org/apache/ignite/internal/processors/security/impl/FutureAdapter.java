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

package org.apache.ignite.internal.processors.security.impl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.NotNull;

/** . */
public class FutureAdapter<T> implements Future<T> {
    /** . */
    private final IgniteFuture<T> igniteFut;

    /** . */
    public FutureAdapter(IgniteFuture<T> igniteFut) {
        this.igniteFut = igniteFut;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        return igniteFut.cancel();
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return igniteFut.isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return igniteFut.isDone();
    }

    /** {@inheritDoc} */
    @Override public T get() throws InterruptedException, ExecutionException {
        return igniteFut.get();
    }

    /** {@inheritDoc} */
    @Override public T get(long timeout,
        @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return igniteFut.get(timeout, unit);
    }
}
