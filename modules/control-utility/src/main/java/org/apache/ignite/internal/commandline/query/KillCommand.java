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
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionTask;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionTaskArg;
import org.apache.ignite.internal.visor.query.VisorContinuousQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorContinuousQueryCancelTaskArg;
import org.apache.ignite.internal.visor.query.VisorQueryCancelOnInitiatorTask;
import org.apache.ignite.internal.visor.query.VisorQueryCancelOnInitiatorTaskArg;
import org.apache.ignite.internal.visor.query.VisorScanQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorScanQueryCancelTaskArg;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTask;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTaskArg;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCancelTask;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.QueryMXBeanImpl.EXPECTED_GLOBAL_QRY_ID_FORMAT;
import static org.apache.ignite.internal.commandline.CommandList.KILL;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.COMPUTE;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.CONTINUOUS;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.SCAN;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.SERVICE;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.SNAPSHOT;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.SQL;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.TRANSACTION;
import static org.apache.ignite.internal.sql.command.SqlKillQueryCommand.parseGlobalQueryId;

/**
 * control.sh kill command.
 *
 * @see KillSubcommand
 * @see QueryMXBean
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

            case SQL:
                T2<UUID, Long> ids = parseGlobalQueryId(argIter.nextArg("Expected SQL query id."));

                if (ids == null)
                    throw new IllegalArgumentException("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT);

                taskArgs = new VisorQueryCancelOnInitiatorTaskArg(ids.get1(), ids.get2());

                taskName = VisorQueryCancelOnInitiatorTask.class.getName();

                break;

            case SCAN:
                String originNodeIsStr = argIter.nextArg("Expected query originating node id.");

                UUID originNodeId = UUID.fromString(originNodeIsStr);

                String cacheName = argIter.nextArg("Expected cache name.");

                long qryId = Long.parseLong(argIter.nextArg("Expected query identifier."));

                taskArgs = new VisorScanQueryCancelTaskArg(originNodeId, cacheName, qryId);

                taskName = VisorScanQueryCancelTask.class.getName();

                break;

            case CONTINUOUS:
                taskArgs = new VisorContinuousQueryCancelTaskArg(
                    UUID.fromString(argIter.nextArg("Expected query originating node id.")),
                    UUID.fromString(argIter.nextArg("Expected continuous query id.")));

                taskName = VisorContinuousQueryCancelTask.class.getName();

                break;

            case SNAPSHOT:
                taskArgs = argIter.nextArg("Expected snapshot name.");

                taskName = VisorSnapshotCancelTask.class.getName();

                break;

            default:
                throw new IllegalArgumentException("Unknown kill subcommand: " + cmd);
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Kill compute task by session id:", KILL, singletonMap("session_id", "Session identifier."),
            COMPUTE.toString(), "session_id");

        Command.usage(log, "Kill service by name:", KILL, singletonMap("name", "Service name."),
            SERVICE.toString(), "name");

        Command.usage(log, "Kill transaction by xid:", KILL, singletonMap("xid", "Transaction identifier."),
            TRANSACTION.toString(), "xid");

        Command.usage(log, "Kill sql query by query id:", KILL, singletonMap("query_id", "Query identifier."),
            SQL.toString(), "query_id");

        Map<String, String> params = new HashMap<>();

        params.put("origin_node_id", "Originating node id.");
        params.put("cache_name", "Cache name.");
        params.put("query_id", "Query identifier.");

        Command.usage(log, "Kill scan query by node id, cache name and query id:", KILL,
            params, SCAN.toString(),"origin_node_id", "cache_name", "query_id");

        params.clear();
        params.put("origin_node_id", "Originating node id.");
        params.put("routine_id", "Routine identifier.");

        Command.usage(log, "Kill continuous query by routine id:", KILL, params, CONTINUOUS.toString(),
            "origin_node_id", "routine_id");

        Command.usage(log, "Kill running snapshot by snapshot name:", KILL, singletonMap("snapshot_name", "Snapshot name."),
            SNAPSHOT.toString(), "snapshot_name");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return KILL.toCommandName();
    }
}
