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

package org.apache.ignite.internal.processors.tracing.configuration;

import java.util.Collections;
import java.util.Map;

import org.apache.ignite.IgniteException;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Noop tracing configuration manager.
 * To be used mainly with {@link org.apache.ignite.internal.processors.tracing.NoopTracing}.
 */
public final class NoopTracingConfigurationManager implements TracingConfigurationManager {
    /** */
    public static final NoopTracingConfigurationManager INSTANCE = new NoopTracingConfigurationManager();

    /** {@inheritDoc} */
    @Override public void set(@NotNull TracingConfigurationCoordinates coordinates,
        @NotNull TracingConfigurationParameters parameters) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationParameters get(
        @NotNull TracingConfigurationCoordinates coordinates) {
        return TracingConfigurationManager.NOOP_CONFIGURATION;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Map<TracingConfigurationCoordinates, TracingConfigurationParameters> getAll(
        @Nullable Scope scope
    ) {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void reset(@NotNull TracingConfigurationCoordinates coordinates) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void resetAll(@Nullable Scope scope) throws IgniteException {
        // No-op.
    }
}
