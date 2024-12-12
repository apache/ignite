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

package org.apache.ignite.internal.processors.security.thread;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.security.AbstractSecurityAwareWrapper;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;

/**
 * Represents a {@link Callable} wrapper that executes the original {@link Callable} with the security context
 * current at the time the wrapper was created.
 */
class SecurityAwareCallable<T> extends AbstractSecurityAwareWrapper<Callable<T>> implements Callable<T> {
    /** */
    private SecurityAwareCallable(IgniteSecurity security, Callable<T> delegate) {
        super(security, delegate);
    }

    /** {@inheritDoc} */
    @Override public T call() throws Exception {
        try (OperationSecurityContext ignored = security.withContext(secCtx)) {
            return delegate.call();
        }
    }

    /** */
    static <A> Callable<A> wrap(IgniteSecurity sec, Callable<A> delegate) {
        if (delegate == null || sec.isDefaultContext() || delegate instanceof AbstractSecurityAwareWrapper)
            return delegate;

        return new SecurityAwareCallable<>(sec, delegate);
    }

    /** */
    static <A> Collection<? extends Callable<A>> wrap(
        IgniteSecurity sec,
        Collection<? extends Callable<A>> tasks
    ) {
        if (tasks == null || sec.isDefaultContext())
            return tasks;

        return tasks.stream().map(t -> t == null ? null : new SecurityAwareCallable<>(sec, t)).collect(Collectors.toList());
    }
}
