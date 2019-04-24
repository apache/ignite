/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.misc.VisorClusterNode;
import org.apache.ignite.internal.visor.misc.VisorWalTask;
import org.apache.ignite.internal.visor.misc.VisorWalTaskArg;
import org.apache.ignite.internal.visor.misc.VisorWalTaskOperation;
import org.apache.ignite.internal.visor.misc.VisorWalTaskResult;

import static org.apache.ignite.internal.commandline.CommandArgIterator.isCommandOrOption;
import static org.apache.ignite.internal.commandline.Commands.WAL;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;

/**
 *
 */
public class WalCommands extends Command<T2<String, String>> {
    /** */
    static final String WAL_PRINT = "print";

    /** */
    static final String WAL_DELETE = "delete";


    private CommandLogger logger;

    private String walAct;
    private String walArgs;

    /**
     * Execute WAL command.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to execute wal action.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        this.logger = logger;

        try (GridClient client = startClient(clientCfg)) {
            switch (walAct) {
                case WAL_DELETE:
                    deleteUnusedWalSegments(client, walArgs, clientCfg);

                    break;

                case WAL_PRINT:
                default:
                    printUnusedWalSegments(client, walArgs, clientCfg);

                    break;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (WAL_DELETE.equals(walAct))
            return "Warning: the command will delete unused WAL segments.";

        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String str = argIter.nextArg("Expected arguments for " + WAL.text());

        String walAct = str.toLowerCase();

        if (WAL_PRINT.equals(walAct) || WAL_DELETE.equals(walAct)) {
            String walArgs = (str = argIter.peekNextArg()) != null && !isCommandOrOption(str)
                ? argIter.nextArg("Unexpected argument for " + WAL.text() + ": " + walAct)
                : "";

            this.walAct = walAct;
            this.walArgs = walArgs;
        }
        else
            throw new IllegalArgumentException("Unexpected action " + walAct + " for " + WAL.text());
    }

    @Override public T2<String, String> arg() {
        return new T2<>(walAct, walArgs);
    }

    /**
     * Execute delete unused WAL segments task.
     *  @param client Client.
     * @param walArgs WAL args.
     * @param clientCfg Client configuration.
     */
    private void deleteUnusedWalSegments(
        GridClient client,
        String walArgs,
        GridClientConfiguration clientCfg
    ) throws Exception {
        VisorWalTaskResult res = executeTask(client, VisorWalTask.class,
            walArg(VisorWalTaskOperation.DELETE_UNUSED_WAL_SEGMENTS, walArgs), clientCfg);
        printDeleteWalSegments0(res);
    }

    /**
     * Execute print unused WAL segments task.
     *  @param client Client.
     * @param walArgs Wal args.
     * @param clientCfg Client configuration.
     */
    private void printUnusedWalSegments(
        GridClient client,
        String walArgs,
        GridClientConfiguration clientCfg
    ) throws Exception {
        VisorWalTaskResult res = executeTask(client, VisorWalTask.class,
            walArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS, walArgs), clientCfg);
        printUnusedWalSegments0(res);
    }

    /**
     * Prepare WAL task argument.
     *
     * @param op Operation.
     * @param s Argument from command line.
     * @return Task argument.
     */
    private VisorWalTaskArg walArg(VisorWalTaskOperation op, String s) {
        List<String> consistentIds = null;

        if (!F.isEmpty(s)) {
            consistentIds = new ArrayList<>();

            for (String consistentId : s.split(","))
                consistentIds.add(consistentId.trim());
        }

        switch (op) {
            case DELETE_UNUSED_WAL_SEGMENTS:
            case PRINT_UNUSED_WAL_SEGMENTS:
                return new VisorWalTaskArg(op, consistentIds);

            default:
                return new VisorWalTaskArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS, consistentIds);
        }

    }

    /**
     * Print list of unused wal segments.
     *
     * @param taskRes Task result with baseline topology.
     */
    private void printUnusedWalSegments0(VisorWalTaskResult taskRes) {
        logger.log("Unused wal segments per node:");
        logger.nl();

        Map<String, Collection<String>> res = taskRes.results();
        Map<String, Exception> failRes = taskRes.exceptions();
        Map<String, VisorClusterNode> nodesInfo = taskRes.getNodesInfo();

        for (Map.Entry<String, Collection<String>> entry : res.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            logger.log("Node=" + node.getConsistentId());
            logger.logWithIndent("addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames()), 2);

            for (String fileName : entry.getValue())
                logger.logWithIndent(fileName);

            logger.nl();
        }

        for (Map.Entry<String, Exception> entry : failRes.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            logger.log("Node=" + node.getConsistentId());
            logger.logWithIndent("addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames()), 2);
            logger.logWithIndent("failed with error: " + entry.getValue().getMessage());
            logger.nl();
        }
    }

    /**
     * Print list of unused wal segments.
     *
     * @param taskRes Task result with baseline topology.
     */
    private void printDeleteWalSegments0(VisorWalTaskResult taskRes) {
        logger.log("WAL segments deleted for nodes:");
        logger.nl();

        Map<String, Collection<String>> res = taskRes.results();
        Map<String, Exception> errors = taskRes.exceptions();
        Map<String, VisorClusterNode> nodesInfo = taskRes.getNodesInfo();

        for (Map.Entry<String, Collection<String>> entry : res.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            logger.log("Node=" + node.getConsistentId());
            logger.logWithIndent("addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames()), 2);
            logger.nl();
        }

        for (Map.Entry<String, Exception> entry : errors.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            logger.log("Node=" + node.getConsistentId());
            logger.logWithIndent("addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames()), 2);
            logger.logWithIndent("failed with error: " + entry.getValue().getMessage());
            logger.nl();
        }
    }
}
