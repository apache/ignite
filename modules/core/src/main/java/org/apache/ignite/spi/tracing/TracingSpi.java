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

package org.apache.ignite.spi.tracing;

import org.apache.ignite.spi.IgniteSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing SPI interface.
 */
public interface TracingSpi extends IgniteSpi {
    /**
     * Creates Span with given name.
     *
     * @param name Name of span to create.
     */
    default SpiSpecificSpan create(@NotNull String name) {
        return create(name, (SpiSpecificSpan)null);
    }

    /**
     * Creates Span given name and explicit parent.
     *
     * @param name Name of span to create.
     * @param parentSpan Parent span.
     * @return Created span.
     */
    SpiSpecificSpan create(@NotNull String name, @Nullable SpiSpecificSpan parentSpan);

    /**
     * Creates Span given name and explicit parent.
     *
     * @param name Name of span to create.
     * @param serializedSpan Parent span as serialized bytes.
     * @return Created span.
     * @throws Exception If failed to deserialize patent span.
     */
    SpiSpecificSpan create(@NotNull String name, @Nullable byte[] serializedSpan) throws Exception;

    /**
     * Creates Span given name and explicit parent.
     *
     * @param name Name of span to create.
     * @param parentSpan Parent span.
     * @param samplingRate Number between 0 and 1 that more or less reflects the probability of sampling specific trace.
     * 0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
     * @return Created span.
     */
    @NotNull SpiSpecificSpan create(
        @NotNull String name,
        @Nullable SpiSpecificSpan parentSpan,
        double samplingRate);

    /**
     * Serializes span to byte array to send context over network.
     *
     * @param span Span.
     */
    byte[] serialize(@NotNull SpiSpecificSpan span);

    /**
     * @return type of tracing spi as {@link TracingSpiType} instance.
     */
    TracingSpiType type();
}
