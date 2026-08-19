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

package org.apache.ignite.internal.management.kill;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;

/**
 * Kill all command for mass cancellation of queries.
 */
public class KillAllCommand implements ComputeCommand<KillAllCommandArg, Map<ClusterNode, KillAllTaskResult>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Kill all SQL/scan/index/continuous queries matching specified criteria";
    }

    /** {@inheritDoc} */
    @Override public Class<KillAllCommandArg> argClass() {
        return KillAllCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<KillAllTask> taskClass() {
        return KillAllTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, KillAllCommandArg arg) {
        return CommandUtils.nodeOrAll(arg.nodeId(), nodes);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(KillAllCommandArg arg) {
        StringBuilder sb = new StringBuilder("Warning: the command will kill all ");

        sb.append(arg.target().toString().toLowerCase()).append(" queries");

        if (arg.minDuration() != null)
            sb.append(" with duration > ").append(arg.minDuration()).append(" seconds");

        if (arg.nodeId() != null)
            sb.append(" on node ").append(arg.nodeId());

        sb.append(".");

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        KillAllCommandArg arg,
        Map<ClusterNode, KillAllTaskResult> res,
        Consumer<String> printer
    ) {
        if (res.isEmpty()) {
            printer.accept("Nothing found.");
            return;
        }

        int totalKilled = 0;
        int totalFailed = 0;

        for (Map.Entry<ClusterNode, KillAllTaskResult> entry : res.entrySet()) {
            ClusterNode node = entry.getKey();
            KillAllTaskResult result = entry.getValue();

            totalKilled += result.killed();
            totalFailed += result.failed();

            if (result.killed() > 0 || result.failed() > 0)
                printer.accept("Node ID: " + node.id() + " Killed: " + result.killed() + " Failed: " + result.failed());
        }

        printer.accept("\nTotal killed: " + totalKilled + ", failed to kill: " + totalFailed + " "
            + arg.target().toString().toLowerCase() + " queries");
    }

}
