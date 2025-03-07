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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.util.IgniteUtils.EMPTY_UUIDS;

/** */
public class DiagnosticConnectivityCommand
    implements ComputeCommand<DiagnosticConnectivityCommandArg, Map<ClusterNode, ConnectivityResult>> {
    /**
     * Header of output table.
     */
    private static final List<String> TABLE_HEADER = Arrays.asList(
        "SOURCE-NODE-ID",
        "SOURCE-CONSISTENT-ID",
        "SOURCE-NODE-TYPE",
        "DESTINATION-NODE-ID",
        "DESTINATION_CONSISTENT_ID",
        "DESTINATION-NODE-TYPE"
    );

    /**
     * Client node type string.
     */
    private static final String NODE_TYPE_CLIENT = "CLIENT";

    /**
     * Server node type string.
     */
    private static final String NODE_TYPE_SERVER = "SERVER";


    /** {@inheritDoc} */
    @Override public String description() {
        return "View connectvity state of all nodes in cluster";
    }

    /** {@inheritDoc} */
    @Override public Class<DiagnosticConnectivityCommandArg> argClass() {
        return DiagnosticConnectivityCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<ConnectivityTask> taskClass() {
        return ConnectivityTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, DiagnosticConnectivityCommandArg arg) {
        // Task runs on default node but maps to all nodes in cluster.
        arg.nodes(F.nodeIds(nodes).toArray(EMPTY_UUIDS));

        return null;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        DiagnosticConnectivityCommandArg arg,
        Map<ClusterNode, ConnectivityResult> res,
        Consumer<String> printer
    ) {
        final boolean[] hasFailed = {false};

        final List<List<String>> table = new ArrayList<>();

        table.add(TABLE_HEADER);

        for (Map.Entry<ClusterNode, ConnectivityResult> entry : res.entrySet()) {
            ClusterNode key = entry.getKey();

            String id = key.id().toString();
            String consId = key.consistentId().toString();
            String isClient = key.isClient() ? NODE_TYPE_CLIENT : NODE_TYPE_SERVER;

            ConnectivityResult val = entry.getValue();

            Map<ClusterNode, Boolean> statuses = val.getNodeIds();

            List<List<String>> row = statuses.entrySet().stream().map(nodeStat -> {
                ClusterNode remoteNode = nodeStat.getKey();

                String remoteId = remoteNode.id().toString();
                String remoteConsId = remoteNode.consistentId().toString();
                String nodeType = remoteNode.isClient() ? NODE_TYPE_CLIENT : NODE_TYPE_SERVER;

                Boolean status = nodeStat.getValue();

                if (!status) {
                    hasFailed[0] = true;
                    return Arrays.asList(id, consId, isClient, remoteId, remoteConsId, nodeType);
                }

                return null;
            })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

            table.addAll(row);
        }

        if (hasFailed[0])
            printer.accept("There is no connectivity between the following nodes:\n" + formatAsTable(table));
        else
            printer.accept("There are no connectivity problems.");
    }

    /**
     * Format output as a table
     * @param rows table rows.
     * @return formatted string.
     */
    public static String formatAsTable(List<List<String>> rows) {
        int[] maxLengths = new int[rows.get(0).size()];

        for (List<String> row : rows) {
            for (int i = 0; i < row.size(); i++)
                maxLengths[i] = Math.max(maxLengths[i], row.get(i).length());
        }

        StringBuilder formatBuilder = new StringBuilder();

        for (int maxLength : maxLengths)
            formatBuilder.append("%-").append(maxLength + 2).append("s");

        String format = formatBuilder.toString();

        StringBuilder result = new StringBuilder();

        for (List<String> row : rows)
            result.append(String.format(format, row.toArray())).append("\n");

        return result.toString();
    }
}
