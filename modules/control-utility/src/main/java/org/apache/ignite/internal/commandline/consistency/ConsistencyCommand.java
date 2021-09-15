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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.CONSISTENCY;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.REPAIR;
import static org.apache.ignite.internal.commandline.consistency.ConsistencySubCommand.of;

/**
 *
 */
public class ConsistencyCommand extends AbstractCommand<VisorConsistencyRepairTaskArg> {
    /** Command argument. */
    private VisorConsistencyRepairTaskArg cmdArg;

    /** Consistency sub-command to execute. */
    private ConsistencySubCommand cmd;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Object res = executeTaskByNameOnNode(
                client,
                cmd.taskName(),
                arg(),
                BROADCAST_UUID,
                clientCfg
            );

            log.info(String.valueOf(res));

            return res;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
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
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmd = of(argIter.nextArg("Expected consistency action."));

        if (cmd == REPAIR) {
            String cacheName = argIter.nextArg("Expected cache name.");
            int part = argIter.nextNonNegativeIntArg("Expected partition.");

            cmdArg = new VisorConsistencyRepairTaskArg(cacheName, part);
        }
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
