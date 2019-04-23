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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
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

import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.TxCommandArg.TX_INFO;

public class TxCommands extends Command<VisorTxTaskArg> {
    /** Double indent. */
    private static final String DOUBLE_INDENT = INDENT + INDENT;

    /** Arguments */
    private VisorTxTaskArg args;

    /** Logger. */
    private CommandLogger logger;

    @Override public VisorTxTaskArg arg() {
        return args;
    }

    /**
     * Dump transactions information.
     *
     * @param clientCfg Client configuration.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        this.logger = logger;

        try (GridClient client = startClient(clientCfg)) {
            if (args.getOperation() == VisorTxOperation.INFO)
                return transactionInfo(client, clientCfg);

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

                logger.log(nodeDescription(key));

                for (VisorTxInfo info : entry.getValue().getInfos())
                    logger.log(info.toUserString());
            }
        }
        catch (Throwable e) {
            logger.log("Failed to perform operation.");

            throw e;
        }
    }

    /** {@inheritDoc} */
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
        checkFeatureSupportedByCluster(client, IgniteFeatures.TX_INFO_COMMAND, true);

        GridCacheVersion nearXidVer = executeTask(client, FetchNearXidVersionTask.class, args.txInfoArgument(), conf);

        boolean histMode = false;

        if (nearXidVer != null) {
            logger.log("Resolved transaction near XID version: " + nearXidVer);

            args.txInfoArgument(new TxVerboseId(null, nearXidVer));
        }
        else {
            logger.log("Active transactions not found.");

            if (args.txInfoArgument().gridCacheVersion() != null) {
                logger.log("Will try to peek history to find out whether transaction was committed / rolled back.");

                histMode = true;
            }
            else {
                logger.log("You can specify transaction in GridCacheVersion format in order to peek history " +
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

        logger.nl();
        logger.log(indent + "Transaction detailed info:");

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
        logger.log(indent + "Near XID version: " + firstVerboseInfo.nearXidVersion());
        logger.log(indent + "Near XID version (UUID): " + firstInfo.getNearXid());
        logger.log(indent + "Isolation: " + firstInfo.getIsolation());
        logger.log(indent + "Concurrency: " + firstInfo.getConcurrency());
        logger.log(indent + "Timeout: " + firstInfo.getTimeout());
        logger.log(indent + "Initiator node: " + firstVerboseInfo.nearNodeId());
        logger.log(indent + "Initiator node (consistent ID): " + firstVerboseInfo.nearNodeConsistentId());
        logger.log(indent + "Label: " + firstInfo.getLabel());
        logger.log(indent + "Topology version: " + firstInfo.getTopologyVersion());
        logger.log(indent + "Used caches (ID to name): " + usedCaches);
        logger.log(indent + "Used cache groups (ID to name): " + usedCacheGroups);
        logger.log(indent + "States across the cluster: " + states);
        logger.log(indent + "Transaction topology: ");

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
            logger.log(indent + nodeDescription(entry.getKey()) + ':');

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
                logger.log(indent + "Mapping [type=" + verboseInfo.txMappingType() + "]:");

                printTransactionMapping(indent + DOUBLE_INDENT, info, verboseInfo);
            }
            else {
                logger.log(indent + "Mapping [type=HISTORICAL]:");

                logger.log(indent + DOUBLE_INDENT + "State: " + info.getState());
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
        logger.log(indent + "XID version (UUID): " + info.getXid());
        logger.log(indent + "State: " + info.getState());

        if (verboseInfo.txMappingType() == TxMappingType.REMOTE) {
            logger.log(indent + "Primary node: " + verboseInfo.dhtNodeId());
            logger.log(indent + "Primary node (consistent ID): " + verboseInfo.dhtNodeConsistentId());
        }

        if (!F.isEmpty(verboseInfo.localTxKeys())) {
            logger.log(indent + "Mapped keys:");

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
            logger.log(indent + (txVerboseKey.read() ? "Read" : "Write") +
                " [lock=" + txVerboseKey.lockType() + "]: " + txVerboseKey.txKey());

            if (txVerboseKey.lockType() == TxKeyLockType.AWAITS_LOCK)
                logger.log(indent + DOUBLE_INDENT + "Lock owner XID: " + txVerboseKey.ownerVersion());
        }
    }

    /**
     * Prints results of --tx --info to output in case requested transaction is not active.
     *
     * @param res Response.
     */
    private void printTxInfoHistoricalResult(Map<ClusterNode, VisorTxTaskResult> res) {
        if (F.isEmpty(res))
            logger.log("Transaction was not found in history across the cluster.");
        else {
            logger.log("Transaction was found in completed versions history of the following nodes:");

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
                logger.log(DOUBLE_INDENT + nodeDescription(entry.getKey()) + ':');
                logger.log(DOUBLE_INDENT + DOUBLE_INDENT + "State: " + entry.getValue().getInfos().get(0).getState());
            }
        }
    }

    /**
     * Checks that all cluster nodes support specified feature.
     *
     * @param client Client.
     * @param feature Feature.
     * @param validateClientNodes Whether client nodes should be checked as well.
     */
    private static void checkFeatureSupportedByCluster(
        GridClient client,
        IgniteFeatures feature,
        boolean validateClientNodes
    ) throws GridClientException {
        Collection<GridClientNode> nodes = validateClientNodes ?
            client.compute().nodes() :
            client.compute().nodes(GridClientNode::connectable);

        for (GridClientNode node : nodes) {
            byte[] featuresAttrBytes = node.attribute(IgniteNodeAttributes.ATTR_IGNITE_FEATURES);

            if (!IgniteFeatures.nodeSupports(featuresAttrBytes, feature)) {
                throw new IllegalStateException("Failed to execute command: cluster contains node that " +
                    "doesn't support feature [nodeId=" + node.nodeId() + ", feature=" + feature + ']');
            }
        }
    }
}
