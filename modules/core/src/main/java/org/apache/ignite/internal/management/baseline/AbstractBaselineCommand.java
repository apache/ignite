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

package org.apache.ignite.internal.management.baseline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.baseline.BaselineCommand.VisorBaselineTaskArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.baseline.VisorBaselineAutoAdjustSettings;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskResult;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;

import static java.lang.Boolean.TRUE;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** */
public abstract class AbstractBaselineCommand implements ComputeCommand<VisorBaselineTaskArg, VisorBaselineTaskResult> {
    /** {@inheritDoc} */
    @Override public Class<VisorBaselineTask> taskClass() {
        return VisorBaselineTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, GridClientNode> nodes, VisorBaselineTaskArg arg) {
        return coordinatorOrNull(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        VisorBaselineTaskArg arg,
        VisorBaselineTaskResult res,
        Consumer<String> printer
    ) {
        printer.accept("Cluster state: " + (res.isActive() ? "active" : "inactive"));
        printer.accept("Current topology version: " + res.getTopologyVersion());
        VisorBaselineAutoAdjustSettings autoAdjustSettings = res.getAutoAdjustSettings();

        if (autoAdjustSettings != null) {
            printer.accept("Baseline auto adjustment " + (TRUE.equals(autoAdjustSettings.getEnabled()) ? "enabled" : "disabled")
                + ": softTimeout=" + autoAdjustSettings.getSoftTimeout()
            );
        }

        if (autoAdjustSettings.enabled) {
            if (res.isBaselineAdjustInProgress())
                printer.accept("Baseline auto-adjust is in progress");
            else if (res.getRemainingTimeToBaselineAdjust() < 0)
                printer.accept("Baseline auto-adjust are not scheduled");
            else
                printer.accept("Baseline auto-adjust will happen in '" + res.getRemainingTimeToBaselineAdjust() + "' ms");
        }

        printer.accept("");

        Map<String, VisorBaselineNode> baseline = res.getBaseline();

        Map<String, VisorBaselineNode> srvs = res.getServers();

        // if task runs on a node with VisorBaselineNode of old version (V1) we'll get order=null for all nodes.
        Function<VisorBaselineNode, String> extractFormattedAddrs = node -> {
            Stream<String> sortedByIpHosts =
                Optional.ofNullable(node)
                    .map(addrs -> node.getAddrs())
                    .orElse(Collections.emptyList())
                    .stream()
                    .sorted(Comparator
                        .comparing(resolvedAddr -> new VisorTaskUtils.SortableAddress(resolvedAddr.address())))
                    .map(resolvedAddr -> {
                        if (!resolvedAddr.hostname().equals(resolvedAddr.address()))
                            return resolvedAddr.hostname() + "/" + resolvedAddr.address();
                        else
                            return resolvedAddr.address();
                    });
            if (arg.verbose()) {
                String hosts = String.join(",", sortedByIpHosts.collect(Collectors.toList()));

                if (!hosts.isEmpty())
                    return ", Addresses=" + hosts;
                else
                    return "";
            }
            else
                return sortedByIpHosts.findFirst().map(ip -> ", Address=" + ip).orElse("");
        };

        String crdStr = srvs.values().stream()
            // check for not null
            .filter(node -> node.getOrder() != null)
            .min(Comparator.comparing(VisorBaselineNode::getOrder))
            // format
            .map(crd -> " (Coordinator: ConsistentId=" + crd.getConsistentId() + extractFormattedAddrs.apply(crd) +
                ", Order=" + crd.getOrder() + ")")
            .orElse("");

        printer.accept("Current topology version: " + res.getTopologyVersion() + crdStr);
        printer.accept("");

        if (F.isEmpty(baseline))
            printer.accept("Baseline nodes not found.");
        else {
            printer.accept("Baseline nodes:");

            for (VisorBaselineNode node : baseline.values()) {
                VisorBaselineNode srvNode = srvs.get(node.getConsistentId());

                String state = ", State=" + (srvNode != null ? "ONLINE" : "OFFLINE");

                String order = srvNode != null ? ", Order=" + srvNode.getOrder() : "";

                printer.accept(DOUBLE_INDENT + "ConsistentId=" + node.getConsistentId() +
                    extractFormattedAddrs.apply(srvNode) + state + order);
            }

            printer.accept(U.DELIM);
            printer.accept("Number of baseline nodes: " + baseline.size());

            printer.accept("");

            List<VisorBaselineNode> others = new ArrayList<>();

            for (VisorBaselineNode node : srvs.values()) {
                if (!baseline.containsKey(node.getConsistentId()))
                    others.add(node);
            }

            if (F.isEmpty(others))
                printer.accept("Other nodes not found.");
            else {
                printer.accept("Other nodes:");

                for (VisorBaselineNode node : others)
                    printer.accept(DOUBLE_INDENT + "ConsistentId=" + node.getConsistentId() + ", Order=" + node.getOrder());

                printer.accept("Number of other nodes: " + others.size());
            }
        }
    }
}
