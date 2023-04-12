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

package org.apache.ignite.internal.commandline.systemview;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTask;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTaskResult;
import org.apache.ignite.spi.systemview.view.SystemView;

import static java.util.Collections.nCopies;
import static java.util.Collections.singleton;
import static org.apache.ignite.internal.commandline.CommandList.SYSTEM_VIEW;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.TaskExecutor.getBalancedNode;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.ALL_NODES;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_ID;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_IDS;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.DATE;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/** Represents command for {@link SystemView} content printing. */
public class SystemViewCommand extends AbstractCommand<org.apache.ignite.internal.management.SystemViewCommandArg> {
    /** Column separator. */
    public static final String COLUMN_SEPARATOR = "    ";

    /**
     * Argument for the system view content obtainig task.
     * @see VisorSystemViewTask
     */
    private org.apache.ignite.internal.management.SystemViewCommandArg taskArg;

    /** ID of the nodes to get the system view content from. */
    private Collection<UUID> nodeIds;

    /** Flag to get the system view from all nodes. */
    private boolean allNodes;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try {
            VisorSystemViewTaskResult res;

            try (GridClient client = Command.startClient(clientCfg)) {
                GridClientCompute compute = client.compute();

                Map<UUID, GridClientNode> clusterNodes = compute.nodes().stream()
                    .collect(Collectors.toMap(GridClientNode::nodeId, n -> n));

                if (allNodes)
                    nodeIds = clusterNodes.keySet();
                else if (F.isEmpty(nodeIds))
                    nodeIds = singleton(getBalancedNode(compute).nodeId());
                else {
                    for (UUID id : nodeIds) {
                        if (!clusterNodes.containsKey(id))
                            throw new IllegalArgumentException("Node with id=" + id + " not found.");
                    }
                }

                Collection<GridClientNode> connectable = F.viewReadOnly(nodeIds, clusterNodes::get,
                    id -> clusterNodes.get(id).connectable());

                if (!F.isEmpty(connectable))
                    compute = compute.projection(connectable);

                res = compute.execute(VisorSystemViewTask.class.getName(),
                    new VisorTaskArgument<>(nodeIds, taskArg, false));
            }

            printResults(log, taskArg, res);

            return res;
        }
        catch (Throwable e) {
            log.error("Failed to perform operation.");
            log.error(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** */
    public static void printResults(
        IgniteLogger log,
        org.apache.ignite.internal.management.SystemViewCommandArg taskArg,
        VisorSystemViewTaskResult res
    ) {
        if (res != null) {
            res.rows().forEach((nodeId, rows) -> {
                log.info("Results from node with ID: " + nodeId);
                log.info("---");

                printTable(res.attributes(), res.types(), rows, log);

                log.info("---" + U.nl());
            });
        }
        else
            log.info("No system view with specified name was found [name=" + taskArg.getSystemViewName() + "]");
    }

    /**
     * Prints specified data rows as table.
     *
     * @param titles Titles of the table columns.
     * @param types  Types of the table columns.
     * @param data Table data rows.
     * @param log Logger.
     */
    public static void printTable(List<String> titles, List<SimpleType> types, List<List<?>> data, IgniteLogger log) {
        List<Integer> colSzs;

        if (titles != null)
            colSzs = titles.stream().map(String::length).collect(Collectors.toList());
        else
            colSzs = types.stream().map(x -> 0).collect(Collectors.toList());

        List<List<String>> rows = new ArrayList<>(data.size());

        data.forEach(row -> {
            ListIterator<Integer> colSzIter = colSzs.listIterator();

            rows.add(row.stream().map(val -> {
                String res = String.valueOf(val);

                colSzIter.set(Math.max(colSzIter.next(), res.length()));

                return res;
            }).collect(Collectors.toList()));
        });

        if (titles != null)
            printRow(titles, nCopies(titles.size(), STRING), colSzs, log);

        rows.forEach(row -> printRow(row, types, colSzs, log));
    }

    /**
     * Prints row content with respect to type and size of each column.
     *
     * @param row Row which content should be printed.
     * @param types Column types in sequential order for decent row formatting.
     * @param colSzs Column sizes in sequential order for decent row formatting.
     * @param log Logger.
     */
    private static void printRow(
        Collection<String> row,
        Collection<SimpleType> types,
        Collection<Integer> colSzs,
        IgniteLogger log
    ) {
        Iterator<SimpleType> typeIter = types.iterator();
        Iterator<Integer> colSzsIter = colSzs.iterator();

        log.info(row.stream().map(colVal -> {
            SimpleType colType = typeIter.next();

            int colSz = colSzsIter.next();

            String format = colType == DATE || colType == NUMBER ?
                "%" + colSz + "s" :
                "%-" + colSz + "s";

            return String.format(format, colVal);
        }).collect(Collectors.joining(COLUMN_SEPARATOR)));
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        nodeIds = null;
        allNodes = false;

        String sysViewName = null;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("Failed to read command argument.");

            SystemViewCommandArg cmdArg = CommandArgUtils.of(arg, SystemViewCommandArg.class);

            if (cmdArg == NODE_ID || cmdArg == NODE_IDS) {
                if (nodeIds != null)
                    throw new IllegalArgumentException("Only one of " + NODE_ID + ", " + NODE_IDS + " commands is expected.");

                String idsArg = argIter.nextArg(
                    cmdArg == NODE_ID ? "ID of the node from which system view content should be obtained is expected." :
                        "Comma-separated list of node IDs from which system view content should be obtained is expected.");

                nodeIds = F.viewReadOnly(argIter.parseStringSet(idsArg), name -> {
                    try {
                        return UUID.fromString(name);
                    }
                    catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Failed to parse " + (cmdArg == NODE_ID ? NODE_ID : NODE_IDS) +
                            " command argument. String representation of \"java.util.UUID\" is exepected. For example:" +
                            " 123e4567-e89b-42d3-a456-556642440000", e);
                    }
                });
            }
            else if (cmdArg == ALL_NODES)
                allNodes = true;
            else {
                if (sysViewName != null)
                    throw new IllegalArgumentException("Multiple system view names are not supported.");

                sysViewName = arg;
            }
        }

        if (allNodes && !F.isEmpty(nodeIds))
            throw new IllegalArgumentException("The " + ALL_NODES + " parameter cannot be used with specified node IDs.");

        if (sysViewName == null) {
            throw new IllegalArgumentException(
                "The name of the system view for which its content should be printed is expected.");
        }

        taskArg = new org.apache.ignite.internal.management.SystemViewCommandArg();

        taskArg.setSystemViewName(sysViewName);
    }

    /** {@inheritDoc} */
    @Override public org.apache.ignite.internal.management.SystemViewCommandArg arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put("system_view_name", "Name of the system view which content should be printed." +
            " Both \"SQL\" and \"Java\" styles of system view name are supported" +
            " (e.g. SQL_TABLES and sql.tables will be handled similarly).");
        params.put(NODE_ID + " node_id", "ID of the node to get the system view from (deprecated. Use " + NODE_IDS + " instead). " +
            "If not set, random node will be chosen.");
        params.put(NODE_IDS + " nodeId1" + optional(",nodeId2,....,nodeIdN"),
            "Comma-separated list of nodes IDs to get the system view from. If not set, random node will be chosen.");
        params.put(ALL_NODES.argName(),
            "Get the system view from all nodes. If not set, random node will be chosen.");

        usage(log, "Print system view content:", SYSTEM_VIEW, params, "system_view_name",
            or(optional(NODE_ID, "node_id"),
                optional(NODE_IDS, "nodeId1" + optional(",nodeId2,....,nodeIdN")),
                optional(ALL_NODES)));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SYSTEM_VIEW.toCommandName();
    }
}
