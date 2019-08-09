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

import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Noop and null-safe implementation of Tracing SPI.
 */
public class NoopTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    /** Noop span. */
    private static final Span NOOP_SPAN = new NoopSpan();

    /** Noop serialized span. */
    private static final byte[] NOOP_SERIALIZED_SPAN = new byte[0];

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable Span parentSpan) {
        return NOOP_SPAN;
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        return NOOP_SPAN;
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        return NOOP_SERIALIZED_SPAN;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }
}
