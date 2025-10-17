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

import java.util.concurrent.Executor;
import org.apache.ignite.internal.thread.context.function.ThreadContextAwareRunnable;
import org.jetbrains.annotations.NotNull;

/** */
public class ThreadContextAwareExecutor implements Executor {
    /** */
    private final Executor delegate;

    /** */
    private ThreadContextAwareExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable command) {
        delegate.execute(ThreadContextAwareRunnable.wrap(command));
    }

    /**
     * Creates executor wrapper that automatically captures scoped thread context attributes for the thread that
     * invokes task execution. Capturing attribute values will be restored before task execution, potentially in another
     * thread.
     */
    public static Executor wrap(Executor delegate) {
        return delegate == null ? null : new ThreadContextAwareExecutor(delegate);
    }
}
