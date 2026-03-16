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

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareCallable;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareRunnable;

/**
 * Represents wrapper over an arbitrary {@link ScheduledExecutorService} that automatically captures
 * the {@link OperationContextSnapshot} of {@link OperationContext} bound to the thread from which the task is submitted
 * for execution. It automatically restores captured {@link OperationContextSnapshot} when the task is executed.
 *
 * @see OperationContextAwareExecutorService
 * @see OperationContext
 * @see OperationContextSnapshot
 * @see OperationContextAwareRunnable
 * @see OperationContextAwareCallable
 */
public abstract class OperationContextAwareScheduledExecutorService extends OperationContextAwareExecutorService<ScheduledExecutorService>
    implements ScheduledExecutorService {
    /** */
    protected OperationContextAwareScheduledExecutorService(ScheduledExecutorService delegate) {
        super(delegate);
    }

    /** {@inheritDoc} */
    @Override public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return delegate.schedule(OperationContextAwareRunnable.wrapIfContextNotEmpty(command), delay, unit);
    }

    /** {@inheritDoc} */
    @Override public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return delegate.schedule(OperationContextAwareCallable.wrapIfContextNotEmpty(callable), delay, unit);
    }

    /** {@inheritDoc} */
    @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return delegate.scheduleAtFixedRate(
            OperationContextAwareRunnable.wrapIfContextNotEmpty(command),
            initialDelay,
            period,
            unit
        );
    }

    /** {@inheritDoc} */
    @Override public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return delegate.scheduleWithFixedDelay(
            OperationContextAwareRunnable.wrapIfContextNotEmpty(command),
            initialDelay,
            delay,
            unit
        );
    }
}
