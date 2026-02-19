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

package org.apache.ignite.internal.thread.context.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.thread.context.function.ContextAwareCallable;
import org.apache.ignite.internal.thread.context.function.ContextAwareRunnable;

/** */
public class ContextAwareExecutorService<E extends ExecutorService> implements ExecutorService {
    /** */
    protected E delegate;

    /** */
    public ContextAwareExecutorService() {
        // No-op.
    }

    /** */
    protected ContextAwareExecutorService(E delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        delegate.shutdown();
    }

    /** {@inheritDoc} */
    @Override public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        return delegate.isShutdown();
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        return delegate.isTerminated();
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public <T> Future<T> submit(Callable<T> task) {
        return delegate.submit(ContextAwareCallable.wrapIfContextNotEmpty(task));
    }

    /** {@inheritDoc} */
    @Override public <T> Future<T> submit(Runnable task, T result) {
        return delegate.submit(ContextAwareRunnable.wrapIfContextNotEmpty(task), result);
    }

    /** {@inheritDoc} */
    @Override public Future<?> submit(Runnable task) {
        return delegate.submit(ContextAwareRunnable.wrapIfContextNotEmpty(task));
    }

    /** {@inheritDoc} */
    @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(ContextAwareCallable.wrapIfContextNotEmpty(tasks));
    }

    /** {@inheritDoc} */
    @Override public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> tasks,
        long timeout,
        TimeUnit unit
    ) throws InterruptedException {
        return delegate.invokeAll(ContextAwareCallable.wrapIfContextNotEmpty(tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(ContextAwareCallable.wrapIfContextNotEmpty(tasks));
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(
        Collection<? extends Callable<T>> tasks,
        long timeout,
        TimeUnit unit
    ) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(ContextAwareCallable.wrapIfContextNotEmpty(tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public void execute(Runnable command) {
        delegate.execute(ContextAwareRunnable.wrapIfContextNotEmpty(command));
    }
}
