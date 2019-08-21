/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Manager for {@link Span} instances.
 */
public interface SpanManager {
    /**
     * Creates Span with given name.
     *
     * @param name Name.
     */
    public default Span create(@NotNull String name) {
        return create(name, (Span)null);
    }
    /**
     * Creates Span given name and explicit parent.
     *
     * @param name Name.
     * @param parentSpan Parent span.
     */
    public Span create(@NotNull String name, @Nullable Span parentSpan);
    /**
     * Creates Span with given name and parent from serialized context.
     *
     * @param name Name.
     * @param serializedSpanBytes Serialized span bytes.
     */
    public Span create(@NotNull String name, @Nullable byte[] serializedSpanBytes);
    /**
     * Serializes span to byte array to send context over network.
     *
     * @param span Span.
     */
    public byte[] serialize(@NotNull Span span);
}
