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
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a {@link Callable} wrapper that executes the original {@link Callable} with the security context
 * current at the time the wrapper was created.
 */
public class SecurityAwareCallable<T> implements Callable<T> {
    /** Original callable. */
    private final Callable<T> delegate;

    /** */
    private final IgniteSecurity security;

    /** */
    private final @Nullable SecurityContext secCtx;

    /** */
    public SecurityAwareCallable(IgniteSecurity security, Callable<T> delegate) {
        assert security.enabled();
        assert delegate != null;

        this.delegate = delegate;
        this.security = security;
        secCtx = security.securityContext();
    }

    /** {@inheritDoc} */
    @Override public T call() throws Exception {
        if (secCtx == null)
            return delegate.call();

        try (OperationSecurityContext ignored = security.withContext(secCtx)) {
            return delegate.call();
        }
    }

    /** */
    public static <A> Collection<? extends Callable<A>> toSecurityAware(
        IgniteSecurity sec,
        Collection<? extends Callable<A>> tasks
    ) {
        return tasks.stream().map(t -> new SecurityAwareCallable<>(sec, t)).collect(Collectors.toList());
    }
}
