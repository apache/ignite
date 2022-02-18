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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTaskArg;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.CONSISTENCY;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.REPAIR;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.STATUS;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.of;

/**
 *
 */
public class ConsistencyCommand extends AbstractCommand<Object> {
    /** Cache. */
    public static final String CACHE = "--cache";

    /** Partition. */
    public static final String PARTITION = "--partition";

    /** Strategy. */
    public static final String STRATEGY = "--strategy";

    /** Command argument. */
    private VisorConsistencyRepairTaskArg cmdArg;

    /** Consistency sub-command to execute. */
    private ConsistencySubCommand cmd;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        boolean failed = false;

        StringBuilder sb = new StringBuilder();

        try (GridClient client = Command.startClient(clientCfg)) {
            VisorConsistencyTaskResult res = executeTaskByNameOnNode(
                client,
                cmd.taskName(),
                arg(),
                BROADCAST_UUID,
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

            sb.append(res.message());
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }

        String output = sb.toString();

        if (failed)
            throw new IgniteCheckedException(output);
        else
            log.info(output);

        return output;
    }

    /** {@inheritDoc} */
    @Override public VisorConsistencyRepairTaskArg arg() {
        return cmdArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put("cache-name", "Cache to be checked/repaired.");
        params.put("partition", "Cache's partition to be checked/repaired.");

        usage(
            log,
            "Checks/Repairs cache consistency using Read Repair approach:",
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
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmd = of(argIter.nextArg("Expected consistency action."));

        if (cmd == REPAIR) {
            String cacheName = null;
            int part = -1;
            ReadRepairStrategy strategy = null;

            while (argIter.hasNextArg()) {
                String arg = argIter.peekNextArg();

                if (CACHE.equals(arg) || PARTITION.equals(arg) || STRATEGY.equals(arg)) {
                    arg = argIter.nextArg("Expected parameter key.");

                    switch (arg) {
                        case CACHE:
                            cacheName = argIter.nextArg("Expected cache name.");

                            break;

                        case PARTITION:
                            part = argIter.nextNonNegativeIntArg("Expected partition.");

                            break;

                        case STRATEGY:
                            strategy = ReadRepairStrategy.fromString(argIter.nextArg("Expected strategy."));

                            break;

                        default:
                            throw new IllegalArgumentException("Illegal argument: " + arg);
                    }
                }
                else
                    break;
            }

            if (cacheName == null)
                throw new IllegalArgumentException("Cache name argument missed.");

            if (part == -1)
                throw new IllegalArgumentException("Partition argument missed.");

            if (strategy == null)
                throw new IllegalArgumentException("Strategy argument missed.");

            cmdArg = new VisorConsistencyRepairTaskArg(cacheName, part, strategy);
        }
        else if (cmd == STATUS)
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
