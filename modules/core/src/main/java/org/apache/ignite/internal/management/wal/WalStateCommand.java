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

package org.apache.ignite.internal.management.wal;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.wal.WalStateTask.GroupWalState;
import org.apache.ignite.internal.management.wal.WalStateTask.NodeWalState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.STRING;

/** */
public class WalStateCommand implements ComputeCommand<WalStateCommandArg, List<NodeWalState>> {
    /** {@inheritDoc} */
    @Override public Class<WalStateTask> taskClass() {
        return WalStateTask.class;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Print state of WAL:\n" +
            "    Global     - WAL enabled for group cluster-wide. May be disabled by `--wal disable` command.\n" +
            "    Local      - WAL enabled for group on specific node. May be disabled during rebalance or other system processes.\n" +
            "    Index      - WAL enabled for groups indexes on specific node. May be disabled during index rebuilt";
    }

    /** {@inheritDoc} */
    @Override public Class<WalStateCommandArg> argClass() {
        return WalStateCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<org.apache.ignite.cluster.ClusterNode> nodes(
        Collection<org.apache.ignite.cluster.ClusterNode> nodes,
        WalStateCommandArg arg
    ) {
        return CommandUtils.servers(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(WalStateCommandArg arg, List<NodeWalState> res, Consumer<String> printer) {
        for (NodeWalState r : res)
            printer.accept("Node [consistentId=" + r.consistentId + ", id=" + r.id + "] config WAL mode: " + r.cfgVal);

        printer.accept("");

        SystemViewCommand.printTable(
            List.of("Node", "Group", "Global", "Local", "Index"),
            List.of(STRING, STRING, STRING, STRING, STRING),
            res.stream().flatMap(r -> r.grpsState.entrySet().stream()
                .map(e -> {
                    GroupWalState state = e.getValue();
                    return List.of(r.consistentId, e.getKey(), state.globalWalEnabled(), state.localWalEnabled(), state.indexWalEnabled());
                })).collect(Collectors.toList()),
            printer
        );
    }
}
