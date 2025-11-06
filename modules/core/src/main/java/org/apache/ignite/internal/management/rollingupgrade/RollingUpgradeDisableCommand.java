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
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** Command to disable rolling upgrade mode. */
@IgniteExperimental
public class RollingUpgradeDisableCommand implements ComputeCommand<NoArg, RollingUpgradeTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Disable rolling upgrade. All nodes in the cluster must be running the same version";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<RollingUpgradeDisableTask> taskClass() {
        return RollingUpgradeDisableTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(NoArg arg, RollingUpgradeTaskResult res, Consumer<String> printer) {
        if (res.errorMessage() != null) {
            printer.accept("Failed to disable rolling upgrade: " + res.errorMessage());
            return;
        }

        printer.accept("Rolling upgrade disabled");
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, NoArg arg) {
        Collection<ClusterNode> coordinator = coordinatorOrNull(nodes);

        if (coordinator == null)
            throw new IgniteException("Could not find coordinator among nodes: " + nodes);

        return coordinator;
    }
}
