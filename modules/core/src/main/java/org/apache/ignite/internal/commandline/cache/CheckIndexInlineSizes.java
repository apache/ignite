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

package org.apache.ignite.internal.commandline.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.cache.check_indexes_inline_size.CheckIndexInlineSizesResult;
import org.apache.ignite.internal.commandline.cache.check_indexes_inline_size.CheckIndexInlineSizesTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.VisorTaskArgument;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;

/**
 * Command for check secondary indexes inline size on the different nodes.
 */
public class CheckIndexInlineSizes implements Command<Void> {
    /** Success message. */
    public static final String INDEXES_INLINE_SIZE_ARE_THE_SAME =
        "All secondary indexes have the same effective inline size on all cluster nodes.";

    /** Predicate to filter server nodes. */
    private static final Predicate<GridClientNode> SRV_NODES = node -> !node.isClient() && !node.isDaemon();

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Set<GridClientNode> serverNodes = client.compute().nodes().stream()
                .filter(SRV_NODES)
                .collect(toSet());

            Collection<UUID> serverNodeIds = F.transform(serverNodes, GridClientNode::nodeId);

            CheckIndexInlineSizesResult res = client.compute().projection(serverNodes).execute(
                CheckIndexInlineSizesTask.class.getName(),
                new VisorTaskArgument<>(serverNodeIds, false)
            );

            analyzeResults(log, res);
        }

        return null;
    }

    /**
     * Compares inline sizes from nodes and print to log information about "problem" indexes.
     *
     * @param log Logger
     * @param res Indexes inline size.
     */
    private void analyzeResults(
        Logger log,
        CheckIndexInlineSizesResult res
    ) {
        Map<String, Map<Integer, Set<UUID>>> indexToSizeNode = new HashMap<>();

        for (Map.Entry<UUID, Map<String, Integer>> nodeRes : res.inlineSizes().entrySet()) {
            for (Map.Entry<String, Integer> index : nodeRes.getValue().entrySet()) {
                Map<Integer, Set<UUID>> sizeToNodes = indexToSizeNode.computeIfAbsent(index.getKey(), x -> new HashMap<>());

                sizeToNodes.computeIfAbsent(index.getValue(), x -> new HashSet<>()).add(nodeRes.getKey());
            }
        }

        log.info("Found " + indexToSizeNode.size() + " secondary indexes.");

        Map<String, Map<Integer, Set<UUID>>> problems = indexToSizeNode.entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (F.isEmpty(problems))
            log.info(INDEXES_INLINE_SIZE_ARE_THE_SAME);
        else
            printProblemsAndShowRecommendations(problems, log);
    }

    /** */
    private void printProblemsAndShowRecommendations(Map<String, Map<Integer, Set<UUID>>> problems, Logger log) {
        log.info(problems.size() +
            " index(es) have different effective inline size on nodes. It can lead to performance degradation in SQL queries.");
        log.info("Index(es):");

        for (Map.Entry<String, Map<Integer, Set<UUID>>> entry : problems.entrySet()) {
            SB sb = new SB();

            sb.a("Full index name: ").a(entry.getKey());

            for (Integer size : entry.getValue().keySet())
                sb.a(" nodes: ").a(entry.getValue().get(size)).a(" inline size: ").a(size).a(",");

            sb.setLength(sb.length() - 1);

            log.info(INDENT + sb);
        }

        log.info("");

        log.info("Recommendations:");
        log.info(INDENT + "Check that value of property " + IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE + " are the same on all nodes.");
        log.info(INDENT + "Recreate indexes (execute DROP INDEX, CREATE INDEX commands) with different inline size.");
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        usageCache(
            logger,
            CacheSubcommands.CHECK_INDEX_INLINE_SIZES,
            "Checks that secondary indexes inline size are same on the cluster nodes.",
            null
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CacheSubcommands.CHECK_INDEX_INLINE_SIZES.text().toUpperCase();
    }
}
