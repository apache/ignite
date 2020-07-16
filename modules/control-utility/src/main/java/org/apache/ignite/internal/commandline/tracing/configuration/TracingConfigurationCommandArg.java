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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.spi.tracing.Scope;

/**
 * {@link CommandList#TRACING_CONFIGURATION} command arguments.
 */
public enum TracingConfigurationCommandArg implements CommandArg {
    /**
     * Specify the {@link Scope} of a trace's root span to which some specific tracing configuration will be applied.
     */
    SCOPE("--scope"),

    /** Specify the label of a traced operation. It's an optional attribute. */
    LABEL("--label"),

    /**
     * Number between 0 and 1 that more or less reflects the probability of sampling specific trace. 0 and 1 have
     * special meaning here, 0 means never 1 means always. Default value is 0 (never).
     */
    SAMPLING_RATE("--sampling-rate"),

    /**
     * Set of {@link Scope} that defines which sub-traces will be included in given trace. In other words, if child's
     * span scope is equals to parent's scope or it belongs to the parent's span included scopes, then given child span
     * will be attached to the current trace, otherwise it'll be skipped. See {@link
     * Span#isChainable(Scope)} for more details.
     */
    INCLUDED_SCOPES("--included-scopes");

    /** Arg name. */
    private final String name;

    /**
     * Creates a new instance of tracing configuration argument.
     *
     * @param name command name.
     */
    TracingConfigurationCommandArg(String name) {
        this.name = name;
    }

    /**
     * @return List of arguments.
     */
    public static Set<String> args() {
        return Arrays.stream(TracingConfigurationCommandArg.values())
            .map(TracingConfigurationCommandArg::argName)
            .collect(Collectors.toSet());
    }

    /** {inheritDoc} */
    @Override public String argName() {
        return name;
    }
}
