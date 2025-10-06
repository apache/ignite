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

package org.apache.ignite.internal.thread.context.function;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.internal.thread.context.Context;
import org.apache.ignite.internal.thread.context.ContextAwareWrapper;
import org.apache.ignite.internal.thread.context.ContextSnapshot;
import org.apache.ignite.internal.thread.context.Scope;

/** */
public class ContextAwareCallable<T> extends ContextAwareWrapper<Callable<T>> implements Callable<T> {
    /** */
    private ContextAwareCallable(Callable<T> delegate, ContextSnapshot snapshot) {
        super(delegate, snapshot);
    }

    /** {@inheritDoc} */
    @Override public T call() throws Exception {
        try (Scope ignored = Context.restoreSnapshot(snapshot)) {
            return delegate.call();
        }
    }

    /**
     * Creates a wrapper that stores a specified {@link Callable} along with the Snapshot of the Context bound
     * to the thread when this method is called. Captured Context will be restored before {@link Callable} execution,
     * potentially in another thread.
     */
    public static <T> Callable<T> wrap(Callable<T> delegate) {
        return wrap(delegate, ContextAwareCallable::new);
    }

    /**
     * Creates a wrapper that stores a specified {@link Callable} along with the Snapshot of the Context bound
     * to the thread when this method is called. Captured Context will be restored before {@link Callable} execution,
     * potentially in another thread.
     * If Context holds no data when this method is called, it does nothing and returns original {@link Callable}.
     */
    public static <T> Callable<T> wrapIfContextNotEmpty(Callable<T> delegate) {
        return wrap(delegate, ContextAwareCallable::new, true);
    }

    /** The same as {@link #wrap(Callable)} but wraps each specified {@link Callable}. */
    public static <T> Collection<Callable<T>> wrap(Collection<? extends Callable<T>> tasks) {
        return tasks == null ? null : tasks.stream().map(ContextAwareCallable::wrap).collect(Collectors.toList());
    }

    /** The same as {@link #wrapIfContextNotEmpty(Callable)} but wraps each specified {@link Callable}. */
    public static <T> Collection<Callable<T>> wrapIfContextNotEmpty(Collection<? extends Callable<T>> tasks) {
        return tasks == null ? null : tasks.stream().map(ContextAwareCallable::wrapIfContextNotEmpty).collect(Collectors.toList());
    }
}
