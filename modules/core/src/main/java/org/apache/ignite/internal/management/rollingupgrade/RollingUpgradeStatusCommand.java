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

package org.apache.ignite.internal.management.rollingupgrade;

import java.util.Collection;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** Command to get status of rolling upgrade mode. */
@IgniteExperimental
public class RollingUpgradeStatusCommand implements ComputeCommand<NoArg, RollingUpgradeTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Get status of rolling upgrade mode";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<RollingUpgradeStatusTask> taskClass() {
        return RollingUpgradeStatusTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(NoArg arg, RollingUpgradeTaskResult res, Consumer<String> printer) {
        printer.accept("Rolling upgrade status: " + (res.targetVersion() != null ? "enabled" : "disabled"));

        if (res.targetVersion() != null) {
            printer.accept("Current version: " + res.currentVersion());
            printer.accept("Target version: " + res.targetVersion());
        }

        if (res.nodes() == null || res.nodes().isEmpty()) {
            printer.accept("No nodes information available");
            return;
        }

        res.nodes().stream()
            .collect(Collectors.groupingBy(
                RollingUpgradeStatusNode::version,
                TreeMap::new,
                Collectors.toList()
            ))
            .forEach((ver, nodes) -> {
                printer.accept("Version " + ver + ":");
                nodes.forEach(node -> printer.accept("  " + node.nodeId()));
            });
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, NoArg arg) {
        Collection<ClusterNode> coordinator = coordinatorOrNull(nodes);

        if (coordinator == null)
            throw new IgniteException("Could not find coordinator among nodes: " + nodes);

        return coordinator;
    }
}
