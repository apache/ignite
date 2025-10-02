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

package org.apache.ignite.internal.thread.context;

import java.util.function.BiFunction;
import org.apache.ignite.internal.IgniteInternalWrapper;

/** */
public abstract class ThreadContextAwareWrapper<T> implements IgniteInternalWrapper<T> {
    /** */
    protected final T delegate;

    /** */
    protected final ThreadContextSnapshot snapshot;

    /** */
    @Override public T delegate() {
        return delegate;
    }

    /** */
    protected ThreadContextAwareWrapper(T delegate, ThreadContextSnapshot snapshot) {
        this.delegate = delegate;
        this.snapshot = snapshot;
    }

    /** */
    protected static <T, R extends T> R wrap(T delegate, BiFunction<T, ThreadContextSnapshot, R> wrapper) {
        return delegate == null || delegate instanceof ThreadContextAwareWrapper
            ? (R)delegate
            : wrapper.apply(delegate, ThreadContext.createSnapshot());
    }
}
