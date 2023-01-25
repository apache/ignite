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

package org.apache.ignite.internal.commandline.metric;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.metric.VisorMetricTask;
import org.apache.ignite.internal.visor.metric.VisorMetricTaskArg;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.CommandList.METRIC;
import static org.apache.ignite.internal.commandline.CommandLogger.grouped;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.metric.MetricCommandArg.CONFIGURE_HISTOGRAM;
import static org.apache.ignite.internal.commandline.metric.MetricCommandArg.CONFIGURE_HITRATE;
import static org.apache.ignite.internal.commandline.metric.MetricCommandArg.NODE_ID;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommand.printTable;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/** Represents command for metric values printing. */
public class MetricCommand extends AbstractCommand<VisorMetricTaskArg> {
    /**
     * Argument for the metric values obtainig task.
     * @see VisorMetricTask
     */
    private VisorMetricTaskArg taskArg;

    /** ID of the node to get metric values from. */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try {
            Map<String, ?> res;

            try (GridClient client = Command.startClient(clientCfg)) {
                res = executeTaskByNameOnNode(
                    client,
                    VisorMetricTask.class.getName(),
                    taskArg,
                    nodeId,
                    clientCfg
                );
            }

            if (res != null) {
                List<List<?>> data = res.entrySet().stream()
                    .map(entry -> Arrays.asList(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());

                printTable(asList("metric", "value"), asList(STRING, STRING), data, log);
            }
            else if (arg().bounds() == null && arg().rateTimeInterval() < 0)
                log.info("No metric with specified name was found [name=" + taskArg.name() + "]");

            return res;
        }
        catch (Throwable e) {
            log.error("Failed to perform operation.");
            log.error(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        nodeId = null;

        String metricName = null;
        Object val = null;
        boolean configureHistogram = false;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("Failed to read command argument.");

            MetricCommandArg cmdArg = CommandArgUtils.of(arg, MetricCommandArg.class);

            if (cmdArg == NODE_ID) {
                String nodeIdArg = argIter.nextArg(
                    "ID of the node from which metric values should be obtained is expected.");

                try {
                    nodeId = UUID.fromString(nodeIdArg);
                }
                catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Failed to parse " + NODE_ID + " command argument." +
                        " String representation of \"java.util.UUID\" is exepected. For example:" +
                        " 123e4567-e89b-42d3-a456-556642440000", e);
                }
            }
            else if (cmdArg == CONFIGURE_HISTOGRAM || cmdArg == CONFIGURE_HITRATE) {
                if (metricName != null) {
                    throw new IllegalArgumentException(
                        "One of " + CONFIGURE_HISTOGRAM + ", " + CONFIGURE_HITRATE + " must be specified"
                    );
                }

                configureHistogram = cmdArg == CONFIGURE_HISTOGRAM;
                metricName = argIter.nextArg("Name of metric to configure expected");

                if (configureHistogram) {
                    val = Arrays.stream(argIter.nextArg("Comma-separated histogram bounds expected").split(","))
                        .mapToLong(Long::parseLong)
                        .toArray();

                    if (!F.isSorted((long[])val))
                        throw new IllegalArgumentException("Bounds must be sorted");
                }
                else {
                    val = argIter.nextNonNegativeLongArg("Hitrate time interval");

                    if ((long)val == 0)
                        throw new IllegalArgumentException("Positive value expected");
                }
            }
            else {
                if (metricName != null)
                    throw new IllegalArgumentException("Multiple metric(metric registry) names are not supported.");

                metricName = arg;
            }
        }

        if (metricName == null)
            throw new IllegalArgumentException("The name of a metric(metric registry) is expected.");

        taskArg = new VisorMetricTaskArg(
            metricName,
            val != null && configureHistogram ? (long[])val : null,
            val != null && !configureHistogram ? (Long)val : -1
        );
    }

    /** {@inheritDoc} */
    @Override public VisorMetricTaskArg arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        Map<String, String> params = new TreeMap<>();

        params.put("node_id", "ID of the node to get the metric values from. If not set, random node will be chosen.");
        params.put("name", "Name of the metric which value should be printed." +
            " If name of the metric registry is specified, value of all its metrics will be printed.");

        usage(log, "Print metric value:", METRIC, params, optional(NODE_ID, "node_id"),
            "name");

        params.remove("node_id");
        params.put("name", "Name of the metric which value should be configured.");
        params.put("newBounds", "Comma-separated list of longs to configure histogram.");
        params.put("newRateTimeInterval", "Rate time interval of hitrate.");

        usage(
            log,
            "Configure metric:", METRIC,
            params,
            or(grouped(CONFIGURE_HISTOGRAM, "name", "newBounds"),
                grouped(CONFIGURE_HITRATE, "name", "newRateTimeInterval"))
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return METRIC.toCommandName();
    }
}
