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
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.T2;
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
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;

import static org.apache.ignite.internal.QueryMXBeanImpl.EXPECTED_GLOBAL_QRY_ID_FORMAT;
import static org.apache.ignite.internal.commandline.CommandList.KILL;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.COMPUTE;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.CONTINUOUS_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.SCAN_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.SERVICE;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.SQL_QUERY;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.TRANSACTION;
import static org.apache.ignite.internal.commandline.query.KillQuerySubcommand.of;
import static org.apache.ignite.internal.sql.command.SqlKillQueryCommand.parseGlobalQueryId;

/**
 * control.sh kill command.
 *
 * @see KillQuerySubcommand
 * @see QueryMXBean
 * @see ComputeMXBean
 * @see TransactionsMXBean
 * @see ServiceMXBean
 */
public class KillCommand implements Command<Object> {
    /** Command argument. */
    private Object taskArgs;

    /** Task name. */
    private String taskName;

    /** Subcommand. */
    private KillQuerySubcommand cmd;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Object res = executeTaskByNameOnNode(
                client,
                taskName,
                taskArgs,
                null,
                clientCfg
            );

            switch (cmd) {
                case SCAN_QUERY:
                case SQL_QUERY:
                case CONTINUOUS_QUERY:
                    if (!(boolean)res)
                        throw new RuntimeException("Query not found.");

                    break;

                case COMPUTE:
                    if (!(boolean)res)
                        throw new RuntimeException("Compute task not found.");

                    break;

                case TRANSACTION:
                    String xid = ((VisorTxTaskArg)taskArgs).getXid();

                    if (!TransactionsMXBeanImpl.isXidFound(xid, (Map<ClusterNode, VisorTxTaskResult>)res))
                        throw new RuntimeException("Transaction not found.");

                    break;

                case SERVICE:
                    if (!(boolean)res)
                        throw new RuntimeException("Service not found or can't be canceled.");

                    break;
            }

            return res;
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
        cmd = of(argIter.nextArg("Expected type of resource to kill."));

        if (cmd == null)
            throw new IllegalArgumentException("Expected type of resource to kill.");

        switch (cmd) {
            case SCAN_QUERY:
                String originNodeIsStr = argIter.nextArg("Expected query originating node id.");

                UUID originNodeId = UUID.fromString(originNodeIsStr);

                String cacheName = argIter.nextArg("Expected cache name.");

                long qryId = Long.parseLong(argIter.nextArg("Expected query identifier."));

                taskArgs = new VisorScanQueryCancelTaskArg(originNodeId, cacheName, qryId);

                taskName = VisorScanQueryCancelTask.class.getName();

                break;

            case CONTINUOUS_QUERY:
                taskArgs = new VisorContinuousQueryCancelTaskArg(
                    UUID.fromString(argIter.nextArg("Expected continuous query id.")));

                taskName = VisorContinuousQueryCancelTask.class.getName();

                break;

            case SQL_QUERY:
                T2<UUID, Long> ids = parseGlobalQueryId(argIter.nextArg("Expected SQL query id."));

                if (ids == null)
                    throw new IllegalArgumentException("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT);

                taskArgs = new VisorQueryCancelTaskArg(ids.get1(), ids.get2());

                taskName = VisorQueryCancelTask.class.getName();

                break;

            case COMPUTE:
                taskArgs = new VisorComputeCancelSessionsTaskArg(Collections.singleton(
                    IgniteUuid.fromString(argIter.nextArg("Expected compute task id."))));

                taskName = VisorComputeCancelSessionsTask.class.getName();

                break;

            case TRANSACTION:
                String xid = argIter.nextArg("Expected transaction id.");

                taskArgs = new VisorTxTaskArg(VisorTxOperation.KILL, null, null, null, null, null, null, xid, null,
                    null, null);

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
        Command.usage(logger, "Kill scan query by node id, cache name and query id:", KILL, SCAN_QUERY.toString(),
            "origin_node_id", "cache_name", "query_id");
        Command.usage(logger, "Kill continuous query by routine id:", KILL, CONTINUOUS_QUERY.toString(),
            "routine_id");
        Command.usage(logger, "Kill sql query by query id:", KILL, SQL_QUERY.toString(),
            "query_id");
        Command.usage(logger, "Kill compute task by session id:", KILL, COMPUTE.toString(),
            "session_id");
        Command.usage(logger, "Kill transaction by xid:", KILL, TRANSACTION.toString(), "xid");
        Command.usage(logger, "Kill service by name:", KILL, SERVICE.toString(), "name");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return KILL.toCommandName();
    }
}
