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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.tracing.TracingConfigurationCommand.TracingConfigurationCommandArg;
import org.apache.ignite.internal.visor.tracing.configuration.VisorTracingConfigurationTask;
import org.apache.ignite.internal.visor.tracing.configuration.VisorTracingConfigurationTaskResult;

import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** */
public abstract class AbstractTracingConfigurationCommand implements
    ComputeCommand<TracingConfigurationCommandArg, VisorTracingConfigurationTaskResult> {
    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(Map<UUID, GridClientNode> nodes, TracingConfigurationCommandArg arg) {
        return coordinatorOrNull(nodes);
    }

    /** {@inheritDoc} */
    @Override public Class<VisorTracingConfigurationTask> taskClass() {
        return VisorTracingConfigurationTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        TracingConfigurationCommandArg arg,
        VisorTracingConfigurationTaskResult res,
        Consumer<String> printer
    ) {
        res.print(printer);
    }
}
