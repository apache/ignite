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

import org.apache.ignite.internal.thread.context.Context;
import org.apache.ignite.internal.thread.context.ContextAwareWrapper;
import org.apache.ignite.internal.thread.context.ContextSnapshot;
import org.apache.ignite.internal.thread.context.Scope;

/** */
public class ContextAwareRunnable extends ContextAwareWrapper<Runnable> implements Runnable {
    /** */
    public ContextAwareRunnable(Runnable delegate, ContextSnapshot snapshot) {
        super(delegate, snapshot);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (Scope ignored = Context.restoreSnapshot(snapshot)) {
            delegate.run();
        }
    }

    /**
     * Creates a wrapper that stores a specified {@link Runnable} along with the Snapshot of the Context bound
     * to the thread when this method is called. Captured Context will be restored before {@link Runnable} execution,
     * potentially in another thread.
     */
    public static Runnable wrap(Runnable delegate) {
        return wrap(delegate, ContextAwareRunnable::new);
    }

    /**
     * Creates a wrapper that stores a specified {@link Runnable} along with the Snapshot of the Context bound
     * to the thread when this method is called. Captured Context will be restored before {@link Runnable} execution,
     * potentially in another thread.
     * If Context holds no data when this method is called, it does nothing and returns original {@link Runnable}.
     */
    public static Runnable wrapIfContextNotEmpty(Runnable delegate) {
        return wrap(delegate, ContextAwareRunnable::new, true);
    }
}
