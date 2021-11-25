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

import java.util.concurrent.Executor;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.plugin.extensions.communication.IoPool;
import org.jetbrains.annotations.NotNull;

/** Wrapper of {@link IoPool} that executes tasks in security context that was actual when task was added to pool queue. */
public class SecurityAwareIoPool implements IoPool {
    /** */
    private final IgniteSecurity security;

    /** */
    private final IoPool delegate;

    /** */
    private final Executor executor;

    /** */
    public SecurityAwareIoPool(IgniteSecurity security, IoPool delegate) {
        assert security.enabled();
        assert delegate != null;

        this.security = security;
        this.delegate = delegate;

        final Executor delegateExecutor = delegate.executor();

        executor = delegateExecutor == null ? null : new Executor() {
            @Override public void execute(@NotNull Runnable cmd) {
                delegateExecutor.execute(SecurityAwareRunnable.of(SecurityAwareIoPool.this.security, cmd));
            }
        };
    }

    /** {@inheritDoc} */
    @Override public byte id() {
        return delegate.id();
    }

    /** {@inheritDoc} */
    @Override public Executor executor() {
        return executor;
    }
}
