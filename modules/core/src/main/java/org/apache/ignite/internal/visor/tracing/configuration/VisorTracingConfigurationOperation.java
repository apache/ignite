/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.tracing.configuration;

import org.jetbrains.annotations.Nullable;

/**
 * Tracing configuration operation options.
 */
public enum VisorTracingConfigurationOperation {
    /** Get specific tracing configuration. */
    GET,

    /** Get tracing configuration. */
    GET_ALL,

    /** Reset specific tracing configuration to default. */
    RESET,

    /**
     * Reset all scope specific tracing configurations to default,
     * or reset all tracing configurations to default if scope is not specified.
     */
    RESET_ALL,

    /** Set new tracing configuration. */
    SET;

    /** Enumerated values. */
    private static final VisorTracingConfigurationOperation[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static VisorTracingConfigurationOperation fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
