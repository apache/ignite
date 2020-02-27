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

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionsTask;
import org.apache.ignite.internal.visor.query.VisorQueryCancelTask;
import org.apache.ignite.internal.visor.tx.VisorTxTask;

import static org.apache.ignite.internal.commandline.CommandList.KILL_QUERY;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.CONTINUOUS_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.SCAN_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.SQL_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.of;

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
                taskArgs = Long.valueOf(argIter.nextArg("Expected scan query id."));

                taskName = VisorQueryCancelTask.class.getName();

                break;
            case CONTINUOUS_QUERY:
                taskArgs = argIter.nextArg("Expected continuous query id.");

                taskName = VisorQueryCancelTask.class.getName();

                break;
            case SQL_QUERY:
                taskArgs = argIter.nextArg("Expected SQL query id.");

                taskName = VisorQueryCancelTask.class.getName();

                break;
            case COMPUTE:
                taskArgs = argIter.nextArg("Expected compute task id.");

                taskName = VisorComputeCancelSessionsTask.class.getName();

            case TRANSACTION:
                taskArgs = argIter.nextArg("Expected transaction id.");

                taskName = VisorTxTask.class.getName();

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
