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

import java.util.function.BiConsumer;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.thread.context.ThreadContext;
import org.apache.ignite.internal.thread.context.ThreadContextAwareWrapper;
import org.apache.ignite.internal.thread.context.ThreadContextSnapshot;
import org.jetbrains.annotations.NotNull;

/** */
public class ThreadContextAwareBiConsumer<T, U> extends ThreadContextAwareWrapper<BiConsumer<T, U>> implements BiConsumer<T, U> {
    /** */
    public ThreadContextAwareBiConsumer(BiConsumer<T, U> delegate, ThreadContextSnapshot snapshot) {
        super(delegate, snapshot);
    }

    /** {@inheritDoc} */
    @Override public void accept(T t, U u) {
        try (Scope ignored = ThreadContext.withSnapshot(snapshot)) {
            delegate.accept(t, u);
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public BiConsumer<T, U> andThen(@NotNull BiConsumer<? super T, ? super U> after) {
        return BiConsumer.super.andThen(wrap(after));
    }

    /** */
    public static <T, U> BiConsumer<T, U> wrap(BiConsumer<T, U> delegate) {
        return wrap(delegate, ThreadContextAwareBiConsumer::new);
    }
}
