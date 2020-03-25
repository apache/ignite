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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTask;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTaskArg;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionTask;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;

import static org.apache.ignite.internal.commandline.CommandList.KILL;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.SERVICE;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.COMPUTE;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.TRANSACTION;

/**
 * control.sh kill command.
 *
 * @see KillSubcommand
 * @see ServiceMXBean
 * @see ComputeMXBean
 * @see TransactionsMXBean
 */
public class KillCommand implements Command<Object> {
    /** Command argument. */
    private Object taskArgs;

    /** Task name. */
    private String taskName;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
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
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return taskArgs;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        KillSubcommand cmd;

        try {
            cmd = KillSubcommand.valueOf(argIter.nextArg("Expected type of resource to kill.").toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Expected type of resource to kill.");
        }

        switch (cmd) {
            case COMPUTE:
                taskArgs = new VisorComputeCancelSessionTaskArg(
                    IgniteUuid.fromString(argIter.nextArg("Expected compute task id.")));

                taskName = VisorComputeCancelSessionTask.class.getName();

                break;

            case SERVICE:
                taskArgs = new VisorCancelServiceTaskArg(argIter.nextArg("Expected service name."));

                taskName = VisorCancelServiceTask.class.getName();

                break;

            case TRANSACTION:
                String xid = argIter.nextArg("Expected transaction id.");

                taskArgs = new VisorTxTaskArg(VisorTxOperation.KILL, null, null, null, null, null, null, xid, null,
                    null, null);

                taskName = VisorTxTask.class.getName();

                break;

            default:
                throw new IllegalArgumentException("Unknown kill subcommand: " + cmd);
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new HashMap<>();

        params.put("session_id", "Session identifier.");

        Command.usage(log, "Kill compute task by session id:", KILL, params, COMPUTE.toString(),
            "session_id");

        params.clear();
        params.put("name", "Service name.");

        Command.usage(log, "Kill service by name:", KILL, params, SERVICE.toString(), "name");

        params.clear();
        params.put("xid", "Transaction identifier.");

        Command.usage(log, "Kill transaction by xid:", KILL, params, TRANSACTION.toString(), "xid");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return KILL.toCommandName();
    }
}
