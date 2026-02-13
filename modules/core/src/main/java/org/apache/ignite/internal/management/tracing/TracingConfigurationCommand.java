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
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.tracing.TracingConfigurationCommand.TracingConfigurationCommandArg;

import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** */
public class TracingConfigurationCommand extends CommandRegistryImpl<TracingConfigurationCommandArg, TracingConfigurationTaskResult>
    implements ComputeCommand<TracingConfigurationCommandArg, TracingConfigurationTaskResult> {
    /** */
    public TracingConfigurationCommand() {
        super(
            new TracingConfigurationGetAllCommand(),
            new TracingConfigurationGetCommand(),
            new TracingConfigurationResetAllCommand(),
            new TracingConfigurationResetCommand(),
            new TracingConfigurationSetCommand()
        );
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Print tracing configuration";
    }

    /** {@inheritDoc} */
    @Override public Class<TracingConfigurationGetAllCommandArg> argClass() {
        return TracingConfigurationGetAllCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<TracingConfigurationTask> taskClass() {
        return TracingConfigurationTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, TracingConfigurationCommandArg arg) {
        return coordinatorOrNull(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        TracingConfigurationCommandArg arg,
        TracingConfigurationTaskResult res,
        Consumer<String> printer
    ) {
        res.print(printer);
    }

    /** */
    public abstract static class TracingConfigurationCommandArg extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class TracingConfigurationResetAllCommandArg extends TracingConfigurationGetAllCommandArg {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class TracingConfigurationResetCommandArg extends TracingConfigurationGetCommandArg {
        /** */
        private static final long serialVersionUID = 0;
    }
}
