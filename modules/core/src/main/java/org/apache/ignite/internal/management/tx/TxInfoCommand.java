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

package org.apache.ignite.internal.management.tx;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.management.tx.TxCommand.AbstractTxCommandArg;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.tx.TxCommand.nodeDescription;

/** */
public class TxInfoCommand implements LocalCommand<AbstractTxCommandArg, Map<ClusterNode, TxTaskResult>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Print detailed information (topology and key lock ownership) about specific transaction";
    }

    /** {@inheritDoc} */
    @Override public Class<TxInfoCommandArg> argClass() {
        return TxInfoCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Map<ClusterNode, TxTaskResult> execute(
        @Nullable GridClient cli,
        @Nullable Ignite ignite,
        AbstractTxCommandArg arg0,
        Consumer<String> printer
    ) throws Exception {
        TxInfoCommandArg arg = (TxInfoCommandArg)arg0;

        Optional<GridClientNode> node = CommandUtils.nodes(cli, ignite).stream()
            .filter(n -> !n.isClient())
            .filter(GridClientNode::connectable)
            .findFirst();

        if (!node.isPresent())
            throw new IllegalStateException("No nodes to connect");

        GridCacheVersion nearXidVer = CommandUtils.execute(
            cli,
            ignite,
            FetchNearXidVersionTask.class,
            arg,
            Collections.singleton(node.get())
        );

        boolean histMode = false;

        if (nearXidVer != null) {
            printer.accept("Resolved transaction near XID version: " + nearXidVer);

            arg = new TxInfoCommandArg();

            arg.gridCacheVersion(nearXidVer);
        }
        else {
            printer.accept("Active transactions not found.");

            if (arg.gridCacheVersion() != null) {
                printer.accept("Will try to peek history to find out whether transaction was committed / rolled back.");

                histMode = true;
            }
            else {
                printer.accept("You can specify transaction in GridCacheVersion format in order to peek history " +
                    "to find out whether transaction was committed / rolled back.");

                return null;
            }
        }

        Map<ClusterNode, TxTaskResult> res = CommandUtils.execute(
            cli,
            ignite,
            TxTask.class,
            arg,
            Collections.singleton(node.get())
        );

        if (histMode)
            printTxInfoHistoricalResult(res, printer);
        else
            printTxInfoResult(res, printer);

        return res;
    }

    /**
     * Prints results of --tx --info to output in case requested transaction is not active.
     *
     * @param res Response.
     */
    private void printTxInfoHistoricalResult(Map<ClusterNode, TxTaskResult> res, Consumer<String> printer) {
        if (F.isEmpty(res))
            printer.accept("Transaction was not found in history across the cluster.");
        else {
            printer.accept("Transaction was found in completed versions history of the following nodes:");

            for (Map.Entry<ClusterNode, TxTaskResult> entry : res.entrySet()) {
                printer.accept(DOUBLE_INDENT + nodeDescription(entry.getKey()) + ':');
                printer.accept(DOUBLE_INDENT + DOUBLE_INDENT + "State: " + entry.getValue().getInfos().get(0).getState());
            }
        }
    }

    /**
     * Prints result of --tx --info command to output.
     *
     * @param res Response.
     */
    private void printTxInfoResult(Map<ClusterNode, TxTaskResult> res, Consumer<String> printer) {
        String lb = null;

        Map<Integer, String> usedCaches = new HashMap<>();
        Map<Integer, String> usedCacheGrps = new HashMap<>();
        TxInfo firstInfo = null;
        TxVerboseInfo firstVerboseInfo = null;
        Set<TransactionState> states = new HashSet<>();

        for (Map.Entry<ClusterNode, TxTaskResult> entry : res.entrySet()) {
            for (TxInfo info : entry.getValue().getInfos()) {
                assert info.getTxVerboseInfo() != null;

                if (lb == null)
                    lb = info.getLabel();

                if (firstInfo == null) {
                    firstInfo = info;
                    firstVerboseInfo = info.getTxVerboseInfo();
                }

                usedCaches.putAll(info.getTxVerboseInfo().usedCaches());
                usedCacheGrps.putAll(info.getTxVerboseInfo().usedCacheGroups());
                states.add(info.getState());
            }
        }

        String indent = "";

        printer.accept("");
        printer.accept(indent + "Transaction detailed info:");

        printTransactionDetailedInfo(
            res, usedCaches, usedCacheGrps, firstInfo, firstVerboseInfo, states, indent + DOUBLE_INDENT, printer);
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
    private void printTransactionDetailedInfo(Map<ClusterNode, TxTaskResult> res, Map<Integer, String> usedCaches,
                                              Map<Integer, String> usedCacheGroups, TxInfo firstInfo, TxVerboseInfo firstVerboseInfo,
                                              Set<TransactionState> states, String indent, Consumer<String> printer) {
        printer.accept(indent + "Near XID version: " +
            (firstVerboseInfo == null ? null : firstVerboseInfo.nearXidVersion()));
        printer.accept(indent + "Near XID version (UUID): " +
            (firstInfo == null ? null : firstInfo.getNearXid()));
        printer.accept(indent + "Isolation: " +
            (firstInfo == null ? null : firstInfo.getIsolation()));
        printer.accept(indent + "Concurrency: " +
            (firstInfo == null ? null : firstInfo.getConcurrency()));
        printer.accept(indent + "Timeout: " +
            (firstInfo == null ? null : firstInfo.getTimeout()));
        printer.accept(indent + "Initiator node: " +
            (firstVerboseInfo == null ? null : firstVerboseInfo.nearNodeId()));
        printer.accept(indent + "Initiator node (consistent ID): " +
            (firstVerboseInfo == null ? null : firstVerboseInfo.nearNodeConsistentId()));
        printer.accept(indent + "Label: " +
            (firstInfo == null ? null : firstInfo.getLabel()));
        printer.accept(indent + "Topology version: " +
            (firstInfo == null ? null : firstInfo.getTopologyVersion()));
        printer.accept(indent + "Used caches (ID to name): " + usedCaches);
        printer.accept(indent + "Used cache groups (ID to name): " + usedCacheGroups);
        printer.accept(indent + "States across the cluster: " + states);
        printer.accept(indent + "Transaction topology: ");

        printTransactionTopology(res, indent + DOUBLE_INDENT, printer);
    }

    /**
     * Prints transaction topology to output.
     *
     * @param res Response.
     * @param indent Indent.
     */
    private void printTransactionTopology(Map<ClusterNode, TxTaskResult> res, String indent, Consumer<String> printer) {
        for (Map.Entry<ClusterNode, TxTaskResult> entry : res.entrySet()) {
            printer.accept(indent + nodeDescription(entry.getKey()) + ':');

            printTransactionMappings(indent + DOUBLE_INDENT, entry, printer);
        }
    }

    /**
     * Prints transaction mappings for specific cluster node to output.
     *
     * @param indent Indent.
     * @param entry Entry.
     */
    private void printTransactionMappings(String indent, Map.Entry<ClusterNode, TxTaskResult> entry, Consumer<String> printer) {
        for (TxInfo info : entry.getValue().getInfos()) {
            TxVerboseInfo verboseInfo = info.getTxVerboseInfo();

            if (verboseInfo != null) {
                printer.accept(indent + "Mapping [type=" + verboseInfo.txMappingType() + "]:");

                printTransactionMapping(indent + DOUBLE_INDENT, info, verboseInfo, printer);
            }
            else {
                printer.accept(indent + "Mapping [type=HISTORICAL]:");

                printer.accept(indent + DOUBLE_INDENT + "State: " + info.getState());
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
    private void printTransactionMapping(String indent, TxInfo info, TxVerboseInfo verboseInfo, Consumer<String> printer) {
        printer.accept(indent + "XID version (UUID): " + info.getXid());
        printer.accept(indent + "State: " + info.getState());

        if (verboseInfo.txMappingType() == TxMappingType.REMOTE) {
            printer.accept(indent + "Primary node: " + verboseInfo.dhtNodeId());
            printer.accept(indent + "Primary node (consistent ID): " + verboseInfo.dhtNodeConsistentId());
        }

        if (!F.isEmpty(verboseInfo.localTxKeys())) {
            printer.accept(indent + "Mapped keys:");

            printTransactionKeys(indent + DOUBLE_INDENT, verboseInfo, printer);
        }
    }

    /**
     * Prints keys of specific transaction mapping to output.
     *
     * @param indent Indent.
     * @param verboseInfo Verbose info.
     */
    private void printTransactionKeys(String indent, TxVerboseInfo verboseInfo, Consumer<String> printer) {
        for (TxVerboseKey txVerboseKey : verboseInfo.localTxKeys()) {
            printer.accept(indent + (txVerboseKey.read() ? "Read" : "Write") +
                " [lock=" + txVerboseKey.lockType() + "]: " + txVerboseKey.txKey());

            if (txVerboseKey.lockType() == TxKeyLockType.AWAITS_LOCK)
                printer.accept(indent + DOUBLE_INDENT + "Lock owner XID: " + txVerboseKey.ownerVersion());
        }
    }
}
