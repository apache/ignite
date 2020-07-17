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

package org.apache.ignite.internal.commandline.tracing.configuration;

import org.apache.ignite.internal.commandline.configuration.VisorTracingConfigurationOperation;
import org.jetbrains.annotations.Nullable;

/**
 * Set of tracing configuration commands.
 */
public enum TracingConfigurationSubcommand {
    /** Get specific tracing configuration. */
    GET("get", VisorTracingConfigurationOperation.GET),

    /** Get tracing configuration. */
    GET_ALL("get_all", VisorTracingConfigurationOperation.GET_ALL),

    /** Reset specific tracing configuration to default. */
    RESET("reset", VisorTracingConfigurationOperation.RESET),

    /**
     * Reset all scope specific tracing configuration to default,
     * or reset all tracing configurations to default if scope is not specified.
     */
    RESET_ALL("reset_all", VisorTracingConfigurationOperation.RESET_ALL),

    /** Set new tracing configuration. */
    SET("set", VisorTracingConfigurationOperation.SET);

    /** Enumerated values. */
    private static final TracingConfigurationSubcommand[] VALS = values();

    /** Name. */
    private final String name;

    /** Corresponding visor tracing configuration operation. */
    private final VisorTracingConfigurationOperation visorOperation;

    /**
     * @param name Name.
     * @param visorOperation Corresponding visor tracing configuration operation.
     */
    TracingConfigurationSubcommand(String name, VisorTracingConfigurationOperation visorOperation) {
        this.name = name;
        this.visorOperation = visorOperation;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static TracingConfigurationSubcommand of(String text) {
        for (TracingConfigurationSubcommand cmd : TracingConfigurationSubcommand.values()) {
            if (cmd.text().equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /**
     * @return Name.
     */
    public String text() {
        return name;
    }

    /**
     * @return {@link VisorTracingConfigurationOperation} which is associated with tracing configuration subcommand.
     */
    public VisorTracingConfigurationOperation visorBaselineOperation() {
        return visorOperation;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static TracingConfigurationSubcommand fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
