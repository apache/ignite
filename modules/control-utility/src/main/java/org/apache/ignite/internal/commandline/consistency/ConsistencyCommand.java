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

package org.apache.ignite.internal.commandline.consistency;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTaskArg;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyTaskResult;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.commandline.CommandList.CONSISTENCY;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.FINALIZE_COUNTERS;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.REPAIR;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.STATUS;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.of;

/**
 *
 */
public class ConsistencyCommand extends AbstractCommand<Object> {
    /** Cache. */
    public static final String CACHE = "--cache";

    /** Partitions. */
    public static final String PARTITIONS = "--partitions";

    /** Strategy. */
    public static final String STRATEGY = "--strategy";

    /** Parallel check. */
    public static final String PARALLEL = "--parallel";

    /** Command argument. */
    private VisorConsistencyRepairTaskArg cmdArg;

    /** Consistency sub-command to execute. */
    private ConsistencySubCommand cmd;

    /** Parallel check.*/
    private boolean parallel;

    /** Predicate to filter server nodes. */
    private static final Predicate<GridClientNode> SRV_NODES = node -> !node.isClient() && !node.isDaemon();

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        String output;

        try (GridClient client = Command.startClient(clientCfg)) {
            if (cmd == FINALIZE_COUNTERS)
                output = executeTaskByNameOnNode(client, cmd.taskName(), arg(), null, clientCfg);
            else {
                StringBuilder sb = new StringBuilder();
                boolean failed = false;

                Set<UUID> nodeIds = parallel ?
                    Collections.singleton(BROADCAST_UUID) :
                    client.compute().nodes().stream()
                        .filter(SRV_NODES)
                        .map(GridClientNode::nodeId)
                        .collect(toSet());

                for (UUID nodeId : nodeIds) {
                    VisorConsistencyTaskResult res = executeTaskByNameOnNode(
                        client,
                        cmd.taskName(),
                        arg(),
                        nodeId,
                        clientCfg
                    );

                    if (res.cancelled()) {
                        sb.append("Operation execution cancelled.\n\n");

                        failed = true;
                    }

                    if (res.failed()) {
                        sb.append("Operation execution failed.\n\n");

                        failed = true;
                    }

                    if (failed)
                        sb.append("[EXECUTION FAILED OR CANCELLED, RESULTS MAY BE INCOMPLETE OR INCONSISTENT]\n\n");

                    if (res.message() != null)
                        sb.append(res.message());
                    else
                        assert !parallel;

                    if (failed)
                        break;
                }

                output = sb.toString();

                if (failed)
                    throw new IgniteCheckedException(output);
            }
        }
        catch (Throwable e) {
            log.error("Failed to perform operation.");
            log.error(CommandLogger.errorMessage(e));

            throw e;
        }

        log.info(output);

        return output;
    }

    /** {@inheritDoc} */
    @Override public VisorConsistencyRepairTaskArg arg() {
        return cmdArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put("cache-name", "Cache to be checked/repaired.");
        params.put("partition", "Cache's partition to be checked/repaired.");

        usage(
            log,
            "Check/Repair cache consistency using Read Repair approach:",
            CONSISTENCY,
            params,
            REPAIR.toString(),
            "cache-name",
            "partition");

        usage(
            log,
            "Cache consistency check/repair operations status:",
            CONSISTENCY,
            Collections.emptyMap(),
            STATUS.toString());

        usage(
            log,
            "Finalize partitions update counters:",
            CONSISTENCY,
            Collections.emptyMap(),
            FINALIZE_COUNTERS.toString());
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmd = of(argIter.nextArg("Expected consistency action."));

        parallel = cmd != REPAIR; // REPAIR is sequential by default.

        if (cmd == REPAIR) {
            String cacheOrGrpName = null;
            Collection<Integer> parts = null;
            ReadRepairStrategy strategy = null;

            while (argIter.hasNextArg()) {
                String arg = argIter.peekNextArg();

                if (CACHE.equals(arg) || PARTITIONS.equals(arg) || STRATEGY.equals(arg) || PARALLEL.equals(arg)) {
                    arg = argIter.nextArg("Expected parameter key.");

                    switch (arg) {
                        case CACHE:
                            cacheOrGrpName = argIter.nextArg("Expected cache(group) name.");

                            break;

                        case PARTITIONS:
                            parts = argIter.nextStringSet("Expected comma separated list of partitions.").stream()
                                .map(Integer::parseInt)
                                .collect(Collectors.toSet());

                            break;

                        case STRATEGY:
                            strategy = ReadRepairStrategy.fromString(argIter.nextArg("Expected strategy."));

                            break;

                        case PARALLEL:
                            parallel = true;

                            break;

                        default:
                            throw new IllegalArgumentException("Illegal argument: " + arg);
                    }
                }
                else
                    break;
            }

            if (cacheOrGrpName == null)
                throw new IllegalArgumentException("Cache (or cache group) name argument missed.");

            if (F.isEmpty(parts))
                throw new IllegalArgumentException("Partitions argument missed.");

            if (strategy == null)
                throw new IllegalArgumentException("Strategy argument missed.");

            // see https://issues.apache.org/jira/browse/IGNITE-15316
            if (parallel && strategy != ReadRepairStrategy.CHECK_ONLY) {
                throw new UnsupportedOperationException(
                    "Parallel mode currently allowed only when CHECK_ONLY strategy is chosen.");
            }

            cmdArg = new VisorConsistencyRepairTaskArg(cacheOrGrpName, parts, strategy);
        }
        else if (cmd == STATUS || cmd == FINALIZE_COUNTERS)
            cmdArg = null;
        else
            throw new IllegalArgumentException("Unsupported operation.");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CONSISTENCY.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public boolean experimental() {
        return true;
    }
}
