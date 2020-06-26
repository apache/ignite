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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.tx.FetchNearXidVersionTask;
import org.apache.ignite.internal.visor.tx.TxKeyLockType;
import org.apache.ignite.internal.visor.tx.TxMappingType;
import org.apache.ignite.internal.visor.tx.TxVerboseId;
import org.apache.ignite.internal.visor.tx.TxVerboseInfo;
import org.apache.ignite.internal.visor.tx.TxVerboseKey;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.internal.client.util.GridClientUtils.checkFeatureSupportedByCluster;
import static org.apache.ignite.internal.commandline.CommandList.TX;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.TxCommandArg.TX_INFO;

/**
 * Transaction commands.
 */
public class TxCommands implements Command<VisorTxTaskArg> {
    /** Arguments */
    private VisorTxTaskArg args;

    /** Logger. */
    private Logger logger;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "List or kill transactions:", TX, getTxOptions());
        Command.usage(logger, "Print detailed information (topology and key lock ownership) about specific transaction:",
            TX, TX_INFO.argName(), or("<TX identifier as GridCacheVersion [topVer=..., order=..., nodeOrder=...] " +
                "(can be found in logs)>", "<TX identifier as UUID (can be retrieved via --tx command)>"));

    }

    /**
     * @return Transaction command options.
     */
    private String[] getTxOptions() {
        List<String> list = new ArrayList<>();

        list.add(optional(TxCommandArg.TX_XID, "XID"));
        list.add(optional(TxCommandArg.TX_DURATION, "SECONDS"));
        list.add(optional(TxCommandArg.TX_SIZE, "SIZE"));
        list.add(optional(TxCommandArg.TX_LABEL, "PATTERN_REGEX"));
        list.add(optional(or(TxCommandArg.TX_SERVERS, TxCommandArg.TX_CLIENTS)));
        list.add(optional(TxCommandArg.TX_NODES, "consistentId1[,consistentId2,....,consistentIdN]"));
        list.add(optional(TxCommandArg.TX_LIMIT, "NUMBER"));
        list.add(optional(TxCommandArg.TX_ORDER, or(VisorTxSortOrder.values())));
        list.add(optional(TxCommandArg.TX_KILL));
        list.add(optional(TX_INFO));
        list.add(optional(CMD_AUTO_CONFIRMATION));

        return list.toArray(new String[list.size()]);
    }

    /** {@inheritDoc} */
    @Override public VisorTxTaskArg arg() {
        return args;
    }

    /**
     * Dump transactions information.
     *
     * @param clientCfg Client configuration.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        this.logger = logger;

        try (GridClient client = Command.startClient(clientCfg)) {
            if (args.getOperation() == VisorTxOperation.INFO)
                return transactionInfo(client, clientCfg);

            Map<ClusterNode, VisorTxTaskResult> res = executeTask(client, VisorTxTask.class, args, clientCfg);

            if (res.isEmpty())
                logger.info("Nothing found.");
            else if (args.getOperation() == VisorTxOperation.KILL)
                logger.info("Killed transactions:");
            else
                logger.info("Matching transactions:");

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
                if (entry.getValue().getInfos().isEmpty())
                    continue;

                ClusterNode key = entry.getKey();

                logger.info(key.getClass().getSimpleName() + " [id=" + key.id() +
                    ", addrs=" + key.addresses() +
                    ", order=" + key.order() +
                    ", ver=" + key.version() +
                    ", isClient=" + key.isClient() +
                    ", consistentId=" + key.consistentId() +
                    "]");

                for (VisorTxInfo info : entry.getValue().getInfos())
                    logger.info(info.toUserString());
            }

            return res;
        }
        catch (Throwable e) {
            logger.severe("Failed to perform operation.");
            logger.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /**
     * Dump transactions information.
     *
     * @param client Client.
     */
    private void transactions(GridClient client, GridClientConfiguration conf) throws GridClientException {
        try {
            if (args.getOperation() == VisorTxOperation.INFO) {
                transactionInfo(client, conf);

                return;
            }

            Map<ClusterNode, VisorTxTaskResult> res = executeTask(client, VisorTxTask.class, args, conf);

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
                if (entry.getValue().getInfos().isEmpty())
                    continue;

                ClusterNode key = entry.getKey();

                logger.info(nodeDescription(key));

                for (VisorTxInfo info : entry.getValue().getInfos())
                    logger.info(info.toUserString());
            }
        }
        catch (Throwable e) {
            logger.severe("Failed to perform operation.");

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (args != null && args.getOperation() == VisorTxOperation.KILL)
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

        TxVerboseId txVerboseId = null;

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

                    limit = (int)argIter.nextNonNegativeLongArg(TxCommandArg.TX_LIMIT.toString());

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

                    duration = argIter.nextNonNegativeLongArg(TxCommandArg.TX_DURATION.toString()) * 1000L;
                    break;

                case TX_SIZE:
                    argIter.nextArg("");

                    size = (int)argIter.nextNonNegativeLongArg(TxCommandArg.TX_SIZE.toString());
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

                case TX_INFO:
                    argIter.nextArg("");

                    op = VisorTxOperation.INFO;

                    txVerboseId = TxVerboseId.fromString(argIter.nextArg(TX_INFO.argName()));

                    break;

                default:
                    throw new AssertionError();
            }
        }

        if (proj != null && consistentIds != null)
            throw new IllegalArgumentException("Projection can't be used together with list of consistent ids.");

        this.args = new VisorTxTaskArg(op, limit, duration, size, null, proj,
            consistentIds, xid, lbRegex, sortOrder, txVerboseId);
    }

    /**
     * Provides text descrition of a cluster node.
     *
     * @param node Node.
     */
    private static String nodeDescription(ClusterNode node) {
        return node.getClass().getSimpleName() + " [id=" + node.id() +
            ", addrs=" + node.addresses() +
            ", order=" + node.order() +
            ", ver=" + node.version() +
            ", isClient=" + node.isClient() +
            ", consistentId=" + node.consistentId() +
            "]";
    }

    /**
     * Executes --tx --info command.
     *
     * @param client Client.
     */
    private Object transactionInfo(GridClient client, GridClientConfiguration conf) throws GridClientException {
        checkFeatureSupportedByCluster(client, IgniteFeatures.TX_INFO_COMMAND, true, true);

        GridCacheVersion nearXidVer = executeTask(client, FetchNearXidVersionTask.class, args.txInfoArgument(), conf);

        boolean histMode = false;

        if (nearXidVer != null) {
            logger.info("Resolved transaction near XID version: " + nearXidVer);

            args.txInfoArgument(new TxVerboseId(null, nearXidVer));
        }
        else {
            logger.info("Active transactions not found.");

            if (args.txInfoArgument().gridCacheVersion() != null) {
                logger.info("Will try to peek history to find out whether transaction was committed / rolled back.");

                histMode = true;
            }
            else {
                logger.info("You can specify transaction in GridCacheVersion format in order to peek history " +
                    "to find out whether transaction was committed / rolled back.");

                return null;
            }
        }

        Map<ClusterNode, VisorTxTaskResult> res = executeTask(client, VisorTxTask.class, args, conf);

        if (histMode)
            printTxInfoHistoricalResult(res);
        else
            printTxInfoResult(res);

        return res;
    }

    /**
     * Prints result of --tx --info command to output.
     *
     * @param res Response.
     */
    private void printTxInfoResult(Map<ClusterNode, VisorTxTaskResult> res) {
        String lb = null;

        Map<Integer, String> usedCaches = new HashMap<>();
        Map<Integer, String> usedCacheGroups = new HashMap<>();
        VisorTxInfo firstInfo = null;
        TxVerboseInfo firstVerboseInfo = null;
        Set<TransactionState> states = new HashSet<>();

        for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
            for (VisorTxInfo info : entry.getValue().getInfos()) {
                assert info.getTxVerboseInfo() != null;

                if (lb == null)
                    lb = info.getLabel();

                if (firstInfo == null) {
                    firstInfo = info;
                    firstVerboseInfo = info.getTxVerboseInfo();
                }

                usedCaches.putAll(info.getTxVerboseInfo().usedCaches());
                usedCacheGroups.putAll(info.getTxVerboseInfo().usedCacheGroups());
                states.add(info.getState());
            }
        }

        String indent = "";

        logger.info("");
        logger.info(indent + "Transaction detailed info:");

        printTransactionDetailedInfo(
            res, usedCaches, usedCacheGroups, firstInfo, firstVerboseInfo, states, indent + DOUBLE_INDENT);
    }

    /**
     * Prints detailed info about transaction to output.
     *
     * @param res Response.
     * @param usedCaches Used caches.
     * @param usedCacheGroups Used cache groups.
     * @param firstInfo First info.
     * @param firstVerboseInfo First verbose info.
     * @param states States.
     * @param indent Indent.
     */
    private void printTransactionDetailedInfo(Map<ClusterNode, VisorTxTaskResult> res, Map<Integer, String> usedCaches,
        Map<Integer, String> usedCacheGroups, VisorTxInfo firstInfo, TxVerboseInfo firstVerboseInfo,
        Set<TransactionState> states, String indent) {
        logger.info(indent + "Near XID version: " + firstVerboseInfo.nearXidVersion());
        logger.info(indent + "Near XID version (UUID): " + firstInfo.getNearXid());
        logger.info(indent + "Isolation: " + firstInfo.getIsolation());
        logger.info(indent + "Concurrency: " + firstInfo.getConcurrency());
        logger.info(indent + "Timeout: " + firstInfo.getTimeout());
        logger.info(indent + "Initiator node: " + firstVerboseInfo.nearNodeId());
        logger.info(indent + "Initiator node (consistent ID): " + firstVerboseInfo.nearNodeConsistentId());
        logger.info(indent + "Label: " + firstInfo.getLabel());
        logger.info(indent + "Topology version: " + firstInfo.getTopologyVersion());
        logger.info(indent + "Used caches (ID to name): " + usedCaches);
        logger.info(indent + "Used cache groups (ID to name): " + usedCacheGroups);
        logger.info(indent + "States across the cluster: " + states);
        logger.info(indent + "Transaction topology: ");

        printTransactionTopology(res, indent + DOUBLE_INDENT);
    }

    /**
     * Prints transaction topology to output.
     *
     * @param res Response.
     * @param indent Indent.
     */
    private void printTransactionTopology(Map<ClusterNode, VisorTxTaskResult> res, String indent) {
        for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
            logger.info(indent + nodeDescription(entry.getKey()) + ':');

            printTransactionMappings(indent + DOUBLE_INDENT, entry);
        }
    }

    /**
     * Prints transaction mappings for specific cluster node to output.
     *
     * @param indent Indent.
     * @param entry Entry.
     */
    private void printTransactionMappings(String indent, Map.Entry<ClusterNode, VisorTxTaskResult> entry) {
        for (VisorTxInfo info : entry.getValue().getInfos()) {
            TxVerboseInfo verboseInfo = info.getTxVerboseInfo();

            if (verboseInfo != null) {
                logger.info(indent + "Mapping [type=" + verboseInfo.txMappingType() + "]:");

                printTransactionMapping(indent + DOUBLE_INDENT, info, verboseInfo);
            }
            else {
                logger.info(indent + "Mapping [type=HISTORICAL]:");

                logger.info(indent + DOUBLE_INDENT + "State: " + info.getState());
            }
        }
    }

    /**
     * Prints specific transaction mapping to output.
     *
     * @param indent Indent.
     * @param info Info.
     * @param verboseInfo Verbose info.
     */
    private void printTransactionMapping(String indent, VisorTxInfo info, TxVerboseInfo verboseInfo) {
        logger.info(indent + "XID version (UUID): " + info.getXid());
        logger.info(indent + "State: " + info.getState());

        if (verboseInfo.txMappingType() == TxMappingType.REMOTE) {
            logger.info(indent + "Primary node: " + verboseInfo.dhtNodeId());
            logger.info(indent + "Primary node (consistent ID): " + verboseInfo.dhtNodeConsistentId());
        }

        if (!F.isEmpty(verboseInfo.localTxKeys())) {
            logger.info(indent + "Mapped keys:");

            printTransactionKeys(indent + DOUBLE_INDENT, verboseInfo);
        }
    }

    /**
     * Prints keys of specific transaction mapping to output.
     *
     * @param indent Indent.
     * @param verboseInfo Verbose info.
     */
    private void printTransactionKeys(String indent, TxVerboseInfo verboseInfo) {
        for (TxVerboseKey txVerboseKey : verboseInfo.localTxKeys()) {
            logger.info(indent + (txVerboseKey.read() ? "Read" : "Write") +
                " [lock=" + txVerboseKey.lockType() + "]: " + txVerboseKey.txKey());

            if (txVerboseKey.lockType() == TxKeyLockType.AWAITS_LOCK)
                logger.info(indent + DOUBLE_INDENT + "Lock owner XID: " + txVerboseKey.ownerVersion());
        }
    }

    /**
     * Prints results of --tx --info to output in case requested transaction is not active.
     *
     * @param res Response.
     */
    private void printTxInfoHistoricalResult(Map<ClusterNode, VisorTxTaskResult> res) {
        if (F.isEmpty(res))
            logger.info("Transaction was not found in history across the cluster.");
        else {
            logger.info("Transaction was found in completed versions history of the following nodes:");

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
                logger.info(DOUBLE_INDENT + nodeDescription(entry.getKey()) + ':');
                logger.info(DOUBLE_INDENT + DOUBLE_INDENT + "State: " + entry.getValue().getInfos().get(0).getState());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return TX.toCommandName();
    }
}
