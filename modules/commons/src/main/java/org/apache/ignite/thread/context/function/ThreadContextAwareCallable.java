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

package org.apache.ignite.thread.context.function;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.thread.context.Scope;
import org.apache.ignite.thread.context.ThreadContext;
import org.apache.ignite.thread.context.ThreadContextAwareWrapper;
import org.apache.ignite.thread.context.ThreadContextSnapshot;

/** */
public class ThreadContextAwareCallable<T> extends ThreadContextAwareWrapper<Callable<T>> implements Callable<T> {
    /** */
    private ThreadContextAwareCallable(Callable<T> delegate, ThreadContextSnapshot snapshot) {
        super(delegate, snapshot);
    }

    /** {@inheritDoc} */
    @Override public T call() throws Exception {
        try (Scope ignored = ThreadContext.withSnapshot(snapshot)) {
            return delegate.call();
        }
    }

    /** */
    public static <T> Callable<T> wrap(Callable<T> delegate) {
        return wrap(delegate, ThreadContextAwareCallable::new);
    }

    /** */
    public static <T> Collection<? extends Callable<T>> wrap(Collection<? extends Callable<T>> tasks) {
        return tasks == null ? null : tasks.stream().map(ThreadContextAwareCallable::wrap).collect(Collectors.toList());
    }
}
