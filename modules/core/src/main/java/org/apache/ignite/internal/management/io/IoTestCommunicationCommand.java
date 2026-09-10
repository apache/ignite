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
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.managers.communication.IoTestResult;
import org.apache.ignite.internal.managers.communication.IoTestResult.LatencySummary;
import org.apache.ignite.internal.managers.communication.IoTestResult.TargetResult;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.STRING;
import static org.apache.ignite.internal.management.api.CommandUtils.node;

/** */
public class IoTestCommunicationCommand implements ComputeCommand<IoTestCommunicationCommandArg, String> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Tests Communication SPI latency to all remote server nodes";
    }

    /** {@inheritDoc} */
    @Override public Class<IoTestCommunicationCommandArg> argClass() {
        return IoTestCommunicationCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<IoTestCommunicationTask> taskClass() {
        return IoTestCommunicationTask.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<ClusterNode> nodes(
        Collection<ClusterNode> nodes,
        IoTestCommunicationCommandArg arg
    ) {
        return node(arg.nodeId(), nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        IoTestCommunicationCommandArg arg,
        String res,
        Consumer<String> printer
    ) {
        printer.accept(res);
    }

    /** Formats a communication test result. */
    public static String formatResult(IoTestResult res) {
        List<String> lines = new ArrayList<>();

        printResult(res, lines::add);

        return String.join(U.nl(), lines);
    }

    /** Prints a communication test result. */
    private static void printResult(IoTestResult res, Consumer<String> printer) {
        List<TargetResult> targets = res.targets();

        printer.accept("Communication SPI test");
        printer.accept("Source: " + consistentId(res.sourceConsistentId()) + " [id=" + res.sourceNodeId() + ']');
        printer.accept("Parameters: warmup=" + res.warmupMillis() + " ms | duration=" + res.durationMillis() +
            " ms | threads=" + res.threads() + " | payload=" + res.payloadSize() + " bytes");
        printer.accept("Handling: " + (res.processInNioThread() ? "NIO thread" : "system pool"));

        List<List<?>> rttRows = new ArrayList<>(targets.size());
        List<List<?>> deliveryRows = new ArrayList<>(targets.size() * 2);

        for (TargetResult target : targets) {
            String targetId = consistentId(target.consistentId());

            rttRows.add(List.of(
                targetId,
                target.samples(),
                millis(target.minimumRttNanos() / 1_000_000.0),
                millis(target.averageRttNanos() / 1_000_000.0),
                millis(target.maximumRttNanos() / 1_000_000.0)
            ));

            addLatency(deliveryRows, targetId, "Request", target.requestDelivery());
            addLatency(deliveryRows, targetId, "Response", target.responseDelivery());
        }

        printer.accept("RTT:");

        SystemViewCommand.printTable(
            List.of("Target", "Samples", "Min, ms", "Avg, ms", "Max, ms"),
            List.of(STRING, NUMBER, NUMBER, NUMBER, NUMBER),
            rttRows,
            printer
        );

        printer.accept("Estimated one-way delivery*:");

        SystemViewCommand.printTable(
            List.of("Target", "Direction", "Min, ms", "Avg, ms", "Max, ms"),
            List.of(STRING, STRING, NUMBER, NUMBER, NUMBER),
            deliveryRows,
            printer
        );

        printer.accept("* One-way delivery uses OS wall-clock time and requires synchronized node clocks.");
        printer.accept("  RTT uses a monotonic clock." +
            (res.processInNioThread() ? "" : " System-pool handling includes executor dispatch time."));
    }

    /** Adds one-way delivery statistics to the table. */
    private static void addLatency(List<List<?>> rows, String target, String direction, LatencySummary latency) {
        rows.add(List.of(
            target,
            direction,
            latency.minimumMillis(),
            millis(latency.averageMillis()),
            latency.maximumMillis()
        ));
    }

    /** Formats milliseconds. */
    private static String millis(double millis) {
        return String.format(Locale.ROOT, "%.3f", millis);
    }

    /** Returns a stable display value for a consistent ID. */
    private static String consistentId(String consistentId) {
        return consistentId == null ? "n/a" : consistentId;
    }
}
