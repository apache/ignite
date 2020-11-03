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
public interface TracingSpi<S extends SpiSpecificSpan> extends IgniteSpi {
    /**
     * Creates Span given name and explicit parent.
     *
     * @param name Name of span to create.
     * @param serializedSpan Parent span as serialized bytes.
     * @return Created span.
     * @throws Exception If failed to deserialize patent span.
     */
    S create(@NotNull String name, @Nullable byte[] serializedSpan) throws Exception;

    /**
     * Creates Span given name and explicit parent.
     *
     * @param name Name of span to create.
     * @param parentSpan Parent span.
     */
    @NotNull S create(
        @NotNull String name,
        @Nullable S parentSpan);

    /**
     * Serializes span to byte array to send context over network.
     *
     * @param span Span.
     */
    byte[] serialize(@NotNull S span);

    /**
     * @return type of tracing spi as byte.
     */
    byte type();
}
