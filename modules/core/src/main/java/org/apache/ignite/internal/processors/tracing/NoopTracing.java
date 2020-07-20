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

package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.internal.processors.tracing.configuration.NoopTracingConfigurationManager;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesHandler;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Noop implementation of {@link Tracing}.
 */
public class NoopTracing implements Tracing {
    /** Noop serialized span. */
    public static final byte[] NOOP_SERIALIZED_SPAN = new byte[0];

    /** Traceable messages handler. */
    private final TraceableMessagesHandler msgHnd;

    /**
     * Constructor.
     */
    public NoopTracing() {
        msgHnd = new TraceableMessagesHandler(this, new NullLogger());
    }

    /** {@inheritDoc} */
    @Override public TraceableMessagesHandler messages() {
        return msgHnd;
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType spanType, @Nullable Span parentSpan) {
        return NoopSpan.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType spanType, @Nullable byte[] serializedParentSpan) {
        return NoopSpan.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Span create(
        @NotNull SpanType spanType,
        @Nullable Span parentSpan,
        @Nullable String label) {
        return NoopSpan.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        return NOOP_SERIALIZED_SPAN;
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationManager configuration() {
        return NoopTracingConfigurationManager.INSTANCE;
    }
}
