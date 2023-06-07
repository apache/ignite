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

package org.apache.ignite.internal.management.diagnostic;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksResult;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTask;

/** */
public class DiagnosticPagelocksCommand implements ComputeCommand<DiagnosticPagelocksCommandArg, Map<ClusterNode, VisorPageLocksResult>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "View pages locks state information on the node or nodes";
    }

    /** {@inheritDoc} */
    @Override public Class<DiagnosticPagelocksCommandArg> argClass() {
        return DiagnosticPagelocksCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorPageLocksTask> taskClass() {
        return VisorPageLocksTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, GridClientNode> nodes, DiagnosticPagelocksCommandArg arg) {
        if (arg.all())
            return nodes.keySet();

        if (F.isEmpty(arg.nodes()))
            return null;

        Set<String> argNodes = new HashSet<>(Arrays.asList(arg.nodes()));

        return nodes.entrySet().stream()
            .filter(entry -> argNodes.contains(entry.getKey().toString())
                || argNodes.contains(String.valueOf(entry.getValue().consistentId())))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        DiagnosticPagelocksCommandArg arg,
        Map<ClusterNode, VisorPageLocksResult> res,
        Consumer<String> printer
    ) {
        res.forEach((n, res0) -> printer.accept(n.id() + " (" + n.consistentId() + ") " + res0.result()));
    }
}
