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

import java.util.function.BiFunction;
import org.apache.ignite.internal.IgniteInternalWrapper;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.jetbrains.annotations.Nullable;

/** Represents wrapper containing an arbitrary object along with {@link OperationContextSnapshot}. */
public class OperationContextAwareWrapper<T> implements IgniteInternalWrapper<T> {
    /** */
    protected final T delegate;

    /** */
    protected final @Nullable OperationContextSnapshot snapshot;

    /** */
    public OperationContextAwareWrapper(T delegate, OperationContextSnapshot snapshot) {
        assert delegate != null;

        this.delegate = delegate;
        this.snapshot = snapshot;
    }

    /** {@inheritDoc} */
    @Override public T delegate() {
        return delegate;
    }

    /** */
    public OperationContextSnapshot contextSnapshot() {
        return snapshot;
    }

    /** */
    public static <T> T wrap(T delegate, BiFunction<T, OperationContextSnapshot, T> wrapper) {
        return wrap(delegate, wrapper, false);
    }

    /** */
    public static <T> T wrap(T delegate, BiFunction<T, OperationContextSnapshot, T> wrapper, boolean ignoreEmptyContext) {
        if (delegate == null || delegate instanceof OperationContextAwareWrapper)
            return delegate;

        OperationContextSnapshot snapshot = OperationContext.createSnapshot();

        if (ignoreEmptyContext && snapshot == null)
            return delegate;

        return wrapper.apply(delegate, snapshot);
    }
}
