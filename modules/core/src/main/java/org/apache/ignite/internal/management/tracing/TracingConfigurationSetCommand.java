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

package org.apache.ignite.internal.management.tracing;

import java.util.Set;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.management.api.Parameter;
import org.apache.ignite.spi.tracing.Scope;

/**
 *
 */
public class TracingConfigurationSetCommand implements ExperimentalCommand {
    /** */
    @Parameter(optional = true)
    private Scope scope;

    /** */
    @Parameter(optional = true)
    private boolean label;

    /** */
    @Parameter(optional = true, example = "Decimal value between 0 and 1, where 0 means never and 1 means always. " +
        "More or less reflects the probability of sampling specific trace.")
    private Double samplingRate;

    /** */
    @Parameter(optional = true, example = "Set of scopes with comma as separator  DISCOVERY|EXCHANGE|COMMUNICATION|TX|SQL")
    private Set<Scope> includedScopes;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Set new tracing configuration. " +
            "If both --scope and --label are specified then add or override label specific configuration, " +
            "if only --scope is specified, then override scope specific configuration. " +
            "Print applied configuration";
    }
}
