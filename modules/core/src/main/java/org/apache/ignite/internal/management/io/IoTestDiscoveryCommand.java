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

package org.apache.ignite.internal.management.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.managers.discovery.IoTestDiscoveryResult;
import org.apache.ignite.internal.managers.discovery.IoTestDiscoveryResult.RingLatencySummary;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.STRING;
import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** */
public class IoTestDiscoveryCommand implements ComputeCommand<IoTestDiscoveryCommandArg, String> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Tests how custom messages traverse the TcpDiscoverySpi ring";
    }

    /** {@inheritDoc} */
    @Override public Class<IoTestDiscoveryCommandArg> argClass() {
        return IoTestDiscoveryCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<IoTestDiscoveryTask> taskClass() {
        return IoTestDiscoveryTask.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<ClusterNode> nodes(
        Collection<ClusterNode> nodes,
        IoTestDiscoveryCommandArg arg
    ) {
        return coordinatorOrNull(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        IoTestDiscoveryCommandArg arg,
        String res,
        Consumer<String> printer
    ) {
        printer.accept(res);
    }

    /** Formats a Discovery SPI test result for transport to the command client. */
    static String formatResult(IoTestDiscoveryCommandArg arg, IoTestDiscoveryResult res) {
        List<String> lines = new ArrayList<>();

        lines.add("TcpDiscoverySpi ring test");
        lines.add("Coordinator: " + nodeName(res.nodeConsistentIds(), res.coordinatorNodeId()) +
            " [id=" + res.coordinatorNodeId() + ']');
        lines.add("Parameters: samples=" + arg.samples() + " | interval=" + arg.interval() +
            " ms | payload=" + arg.payloadSize() + " bytes");

        RingLatencySummary ringLatency = res.ringLatency();

        lines.add("Full-ring latency:");

        SystemViewCommand.printTable(
            List.of("Samples", "Min, ms", "Avg, ms", "Max, ms"),
            List.of(NUMBER, NUMBER, NUMBER, NUMBER),
            List.of(List.of(
                ringLatency.samples(),
                formatMillis(ringLatency.minMillis()),
                formatMillis(ringLatency.averageMillis()),
                formatMillis(ringLatency.maxMillis())
            )),
            lines::add
        );

        lines.add("Estimated per-hop delivery (ring order)*:");

        List<List<?>> rows = res.hopLatencies().stream()
            .map(hop -> List.of(
                nodeName(res.nodeConsistentIds(), hop.fromNodeId()),
                nodeName(res.nodeConsistentIds(), hop.toNodeId()),
                hop.minMillis(),
                formatMillis(hop.averageMillis()),
                hop.maxMillis()
            ))
            .collect(Collectors.toList());

        SystemViewCommand.printTable(
            List.of("From", "To", "Min, ms", "Avg, ms", "Max, ms"),
            List.of(STRING, STRING, NUMBER, NUMBER, NUMBER),
            rows,
            lines::add
        );

        lines.add("* Per-hop delivery uses OS wall-clock time and requires synchronized node clocks.");
        lines.add("  Full-ring latency uses a monotonic clock.");

        return String.join(U.nl(), lines);
    }

    /** Returns a stable human-readable node name. */
    private static String nodeName(Map<UUID, String> consistentIds, UUID nodeId) {
        String consistentId = consistentIds.get(nodeId);

        return consistentId == null ? nodeId.toString() : consistentId;
    }

    /** Formats milliseconds uniformly. */
    private static String formatMillis(double millis) {
        return String.format(Locale.ROOT, "%.3f", millis);
    }
}
