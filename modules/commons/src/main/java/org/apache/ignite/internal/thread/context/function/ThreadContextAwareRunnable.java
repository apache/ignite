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

import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.thread.context.ThreadContext;
import org.apache.ignite.internal.thread.context.ThreadContextAwareWrapper;
import org.apache.ignite.internal.thread.context.ThreadContextSnapshot;

/** */
public class ThreadContextAwareRunnable extends ThreadContextAwareWrapper<Runnable> implements Runnable {
    /** */
    public ThreadContextAwareRunnable(Runnable delegate, ThreadContextSnapshot snapshot) {
        super(delegate, snapshot);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (Scope ignored = ThreadContext.withSnapshot(snapshot)) {
            delegate.run();
        }
    }

    /** */
    public static Runnable wrap(Runnable delegate) {
        return wrap(delegate, ThreadContextAwareRunnable::new);
    }

    /** */
    public static Runnable wrapIfActiveAttributesPresent(Runnable delegate) {
        return wrap(delegate, ThreadContextAwareRunnable::new, true);
    }
}
