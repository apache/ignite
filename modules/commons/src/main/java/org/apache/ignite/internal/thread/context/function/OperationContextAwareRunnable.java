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

import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAwareWrapper;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.apache.ignite.internal.thread.context.Scope;

/** */
public class OperationContextAwareRunnable extends OperationContextAwareWrapper<Runnable> implements Runnable {
    /** */
    public OperationContextAwareRunnable(Runnable delegate, OperationContextSnapshot snapshot) {
        super(delegate, snapshot);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (Scope ignored = OperationContext.restoreSnapshot(snapshot)) {
            delegate.run();
        }
    }

    /**
     * Creates a wrapper that stores a specified {@link Runnable} along with the {@link OperationContextSnapshot} of {@link OperationContext}
     * bound to the thread when this method is called. Captured {@link OperationContextSnapshot} will be restored before
     * {@link Runnable} execution, potentially in another thread.
     */
    public static Runnable wrap(Runnable delegate) {
        return wrap(delegate, OperationContextAwareRunnable::new);
    }

    /**
     * Creates a wrapper that stores a specified {@link Runnable} along with the {@link OperationContextSnapshot} of {@link OperationContext}
     * bound to the thread when this method is called. Captured {@link OperationContextSnapshot} will be restored before
     * {@link Runnable} execution, potentially in another thread.
     * If {@link OperationContext} holds no data when this method is called, it does nothing and returns original {@link Runnable}.
     */
    public static Runnable wrapIfContextNotEmpty(Runnable delegate) {
        return wrap(delegate, OperationContextAwareRunnable::new, true);
    }
}
