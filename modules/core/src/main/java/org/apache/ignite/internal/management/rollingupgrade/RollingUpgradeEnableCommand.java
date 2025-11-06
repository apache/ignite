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
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** Command to enable rolling upgrade. */
@IgniteExperimental
public class RollingUpgradeEnableCommand implements ComputeCommand<RollingUpgradeEnableCommandArg, RollingUpgradeTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Enable rolling upgrade";
    }

    /** {@inheritDoc} */
    @Override public Class<RollingUpgradeEnableCommandArg> argClass() {
        return RollingUpgradeEnableCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<RollingUpgradeEnableTask> taskClass() {
        return RollingUpgradeEnableTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(RollingUpgradeEnableCommandArg arg, RollingUpgradeTaskResult res, Consumer<String> printer) {
        if (res.exception() != null) {
            printer.accept("Failed to enable rolling upgrade: " + res.exception().getMessage());
            return;
        }

        printer.accept("Rolling upgrade enabled "
                + "[currentVersion=" + res.rollUpVers().get1() + ", targetVersion=" + res.rollUpVers().get2() + ']');
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, RollingUpgradeEnableCommandArg arg) {
        Collection<ClusterNode> coordinator = coordinatorOrNull(nodes);

        if (coordinator == null)
            throw new IgniteException("Could not find coordinator among nodes: " + nodes);

        return coordinator;
    }
}
