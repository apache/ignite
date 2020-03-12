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

package org.apache.ignite.internal.commandline.query;

import java.util.Collections;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionsTask;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionsTaskArg;
import org.apache.ignite.internal.visor.query.VisorContinuousQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorContinuousQueryCancelTaskArg;
import org.apache.ignite.internal.visor.query.VisorQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorQueryCancelTaskArg;
import org.apache.ignite.internal.visor.query.VisorScanQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorScanQueryCancelTaskArg;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTask;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.commandline.CommandList.KILL_QUERY;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.CONTINUOUS_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.SCAN_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.SQL_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.of;
import static org.apache.ignite.internal.visor.tx.VisorTxOperation.KILL;

public class KillCommand implements Command<Object> {
    /** Command argument. */
    private Object taskArgs;

    /** Task name. */
    private String taskName;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            return executeTaskByNameOnNode(
                client,
                taskName,
                taskArgs,
                null,
                clientCfg
            );
        }
        catch (Throwable e) {
            logger.severe("Failed to perform operation.");
            logger.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return taskArgs;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        KillQuerySubcommand cmd = of(argIter.nextArg("Expected type of resource to kill."));

        if (cmd == null)
            throw new IllegalArgumentException("Expected type of resource to kill.");

        switch (cmd) {
            case SCAN_QUERY:
                UUID originNodeId = UUID.fromString(argIter.nextArg("Expected query originating node id."));

                String cacheName = argIter.nextArg("Expected cache name");

                long qryId = Long.parseLong(argIter.nextArg("Expected query identifier"));

                taskArgs = new VisorScanQueryCancelTaskArg(originNodeId, cacheName, qryId);

                taskName = VisorScanQueryCancelTask.class.getName();

                break;

            case CONTINUOUS_QUERY:
                taskArgs = new VisorContinuousQueryCancelTaskArg(
                    UUID.fromString(argIter.nextArg("Expected continuous query id.")));

                taskName = VisorContinuousQueryCancelTask.class.getName();

                break;

            case SQL_QUERY:
                taskArgs = new VisorQueryCancelTaskArg(Long.parseLong(argIter.nextArg("Expected SQL query id.")));

                taskName = VisorQueryCancelTask.class.getName();

                break;

            case COMPUTE:
                taskArgs = new VisorComputeCancelSessionsTaskArg(Collections.singleton(
                    IgniteUuid.fromString(argIter.nextArg("Expected compute task id."))));

                taskName = VisorComputeCancelSessionsTask.class.getName();

                break;

            case TRANSACTION:
                String xid = argIter.nextArg("Expected transaction id.");

                taskArgs = new VisorTxTaskArg(KILL, null, null, null, null, null, null, xid, null, null, null);

                taskName = VisorTxTask.class.getName();

                break;

            case SERVICE:
                taskArgs = new VisorCancelServiceTaskArg(argIter.nextArg("Expected service name."));

                taskName = VisorCancelServiceTask.class.getName();

                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Kill scan query by id:", KILL_QUERY, SCAN_QUERY.toString(), "id");
        Command.usage(logger, "Kill continuous query by id:", KILL_QUERY, CONTINUOUS_QUERY.toString(), "id");
        Command.usage(logger, "Kill sql query by id:", KILL_QUERY, SQL_QUERY.toString(), "id");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return KILL_QUERY.toCommandName();
    }
}
