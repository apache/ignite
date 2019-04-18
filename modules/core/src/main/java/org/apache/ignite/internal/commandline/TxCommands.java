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

package org.apache.ignite.internal.commandline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;

import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;

public class TxCommands extends Command<VisorTxTaskArg> {
    /** Arguments */
    private VisorTxTaskArg args;

    @Override public VisorTxTaskArg arg() {
        return args;
    }

    /**
     * Dump transactions information.
     *
     * @param clientCfg Client configuration.
     */
    @Override
    public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        try (GridClient client = startClient(clientCfg)) {
            Map<ClusterNode, VisorTxTaskResult> res = executeTask(client, VisorTxTask.class, args, clientCfg);

            if (res.isEmpty())
                logger.log("Nothing found.");
            else if (args.getOperation() == VisorTxOperation.KILL)
                logger.log("Killed transactions:");
            else
                logger.log("Matching transactions:");

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
                if (entry.getValue().getInfos().isEmpty())
                    continue;

                ClusterNode key = entry.getKey();

                logger.log(key.getClass().getSimpleName() + " [id=" + key.id() +
                    ", addrs=" + key.addresses() +
                    ", order=" + key.order() +
                    ", ver=" + key.version() +
                    ", isClient=" + key.isClient() +
                    ", consistentId=" + key.consistentId() +
                    "]");

                for (VisorTxInfo info : entry.getValue().getInfos())
                    logger.log(info.toUserString());
            }

            return res;
        }
        catch (Throwable e) {
            logger.error("Failed to perform operation.", e);

            throw e;
        }
    }

    @Override public String confirmationPrompt0() {
        if (args.getOperation() == VisorTxOperation.KILL)
            return "Warning: the command will kill some transactions.";

        return null;
    }

    /**
     * @param argIter Argument iterator.
     */
    @Override public void parseArguments(CommandArgIterator argIter) {
        VisorTxProjection proj = null;

        Integer limit = null;

        VisorTxSortOrder sortOrder = null;

        Long duration = null;

        Integer size = null;

        String lbRegex = null;

        List<String> consistentIds = null;

        VisorTxOperation op = VisorTxOperation.LIST;

        String xid = null;

        while (true) {
            String str = argIter.peekNextArg();

            if (str == null)
                break;

            TxCommandArg arg = CommandArgUtils.of(str, TxCommandArg.class);

            if (arg == null)
                break;

            switch (arg) {
                case TX_LIMIT:
                    argIter.nextArg("");

                    limit = (int)argIter.nextLongArg(TxCommandArg.TX_LIMIT.toString());

                    break;

                case TX_ORDER:
                    argIter.nextArg("");

                    sortOrder = VisorTxSortOrder.valueOf(argIter.nextArg(TxCommandArg.TX_ORDER.toString()).toUpperCase());

                    break;

                case TX_SERVERS:
                    argIter.nextArg("");

                    proj = VisorTxProjection.SERVER;
                    break;

                case TX_CLIENTS:
                    argIter.nextArg("");

                    proj = VisorTxProjection.CLIENT;
                    break;

                case TX_NODES:
                    argIter.nextArg("");

                    Set<String> ids = argIter.nextStringSet(TxCommandArg.TX_NODES.toString());

                    if (ids.isEmpty()) {
                        throw new IllegalArgumentException("Consistent id list is empty.");
                    }

                    consistentIds = new ArrayList<>(ids);
                    break;

                case TX_DURATION:
                    argIter.nextArg("");

                    duration = argIter.nextLongArg(TxCommandArg.TX_DURATION.toString()) * 1000L;
                    break;

                case TX_SIZE:
                    argIter.nextArg("");

                    size = (int)argIter.nextLongArg(TxCommandArg.TX_SIZE.toString());
                    break;

                case TX_LABEL:
                    argIter.nextArg("");

                    lbRegex = argIter.nextArg(TxCommandArg.TX_LABEL.toString());

                    try {
                        Pattern.compile(lbRegex);
                    }
                    catch (PatternSyntaxException ignored) {
                        throw new IllegalArgumentException("Illegal regex syntax");
                    }

                    break;

                case TX_XID:
                    argIter.nextArg("");

                    xid = argIter.nextArg(TxCommandArg.TX_XID.toString());
                    break;

                case TX_KILL:
                    argIter.nextArg("");

                    op = VisorTxOperation.KILL;
                    break;

                default:
                    throw new AssertionError();
            }
        }

        if (proj != null && consistentIds != null)
            throw new IllegalArgumentException("Projection can't be used together with list of consistent ids.");

        this.args = new VisorTxTaskArg(op, limit, duration, size, null, proj, consistentIds, xid, lbRegex, sortOrder);
    }
}
