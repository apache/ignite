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

import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesHandler;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Noop implementation of {@link Tracing}.
 */
public class NoopTracing implements Tracing {
    /** */
    private static final TracingSpi NOOP_SPI = new NoopTracingSpi();
    /** */
    private static final TraceableMessagesHandler MSG_HND = new TraceableMessagesHandler(NOOP_SPI, new NullLogger());

    /** {@inheritDoc} */
    @Override public TraceableMessagesHandler messages() {
        return MSG_HND;
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable Span parentSpan) {
        return NOOP_SPI.create(name, parentSpan);
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        return NOOP_SPI.create(name, serializedSpanBytes);
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        return NOOP_SPI.serialize(span);
    }
}
