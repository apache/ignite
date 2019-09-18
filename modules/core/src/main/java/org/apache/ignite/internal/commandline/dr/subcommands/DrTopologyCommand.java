/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.dr.DrSubCommandsList;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.dr.VisorDrTopologyTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrTopologyTaskResult;

import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.visor.dr.VisorDrTopologyTaskArgs.DATA_NODES_FLAG;
import static org.apache.ignite.internal.visor.dr.VisorDrTopologyTaskArgs.OTHER_NODES_FLAG;
import static org.apache.ignite.internal.visor.dr.VisorDrTopologyTaskArgs.RECEIVER_HUBS_FLAG;
import static org.apache.ignite.internal.visor.dr.VisorDrTopologyTaskArgs.SENDER_HUBS_FLAG;

/** */
public class DrTopologyCommand extends
    DrAbstractRemoteSubCommand<VisorDrTopologyTaskArgs, VisorDrTopologyTaskResult, DrTopologyCommand.DrTopologyArguments>
{
    /** Sender hubs parameter. */
    public static final String SENDER_HUBS_PARAM = "--sender-hubs";
    /** Receiver hubs parameter. */
    public static final String RECEIVER_HUBS_PARAM = "--receiver-hubs";
    /** Data nodes parameter. */
    public static final String DATA_NODES_PARAM = "--data-nodes";
    /** Other nodes parameter. */
    public static final String OTHER_NODES_PARAM = "--other-nodes";

    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrTopologyTask";
    }

    /** {@inheritDoc} */
    @Override protected DrTopologyArguments parseArguments0(CommandArgIterator argIter) {
        boolean senderHubs = false;
        boolean receiverHubs = false;
        boolean dataNodes = false;
        boolean otherNodes = false;

        String nextArg;

        //noinspection LabeledStatement
        args_loop: while ((nextArg = argIter.peekNextArg()) != null) {
            switch (nextArg.toLowerCase(Locale.ENGLISH)) {
                case SENDER_HUBS_PARAM:
                    senderHubs = true;
                    break;

                case RECEIVER_HUBS_PARAM:
                    receiverHubs = true;
                    break;

                case DATA_NODES_PARAM:
                    dataNodes = true;
                    break;

                case OTHER_NODES_PARAM:
                    otherNodes = true;
                    break;

                default:
                    //noinspection BreakStatementWithLabel
                    break args_loop;
            }

            argIter.nextArg(null); // Skip peeked argument.
        }

        if (!senderHubs && !receiverHubs && !dataNodes && !otherNodes)
            senderHubs = receiverHubs = dataNodes = otherNodes = true;

        return new DrTopologyArguments(senderHubs, receiverHubs, dataNodes, otherNodes);
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrTopologyTaskResult res, Logger log) {
        log.info("Data Center ID: " + res.getDataCenterId());

        log.info(String.format(
            "Topology: %d server(s), %d client(s)",
            res.getServerNodesCount(),
            res.getClientNodesCount()
        ));

        if (res.getDataCenterId() == 0) {
            log.info("Data Replication state: is not configured.");

            return;
        }

        if (arg().dataNodes) {
            List<T2<UUID, String>> dataNodes = res.getDataNodes();

            if (dataNodes.isEmpty())
                log.info("Data nodes: not found");
            else
                log.info("Data nodes: " + dataNodes.size());

            for (T2<UUID, String> dataNode : dataNodes)
                log.info(String.format(INDENT + "nodeId=%s, Address=%s", dataNode.toArray()));

            log.info(DELIM);
        }

        if (arg().senderHubs) {
            List<T3<UUID, String, String>> senderHubs = res.getSenderHubs();

            if (senderHubs.isEmpty())
                log.info("Sender hubs: not found");
            else
                log.info("Sender hubs: " + senderHubs.size());

            for (T3<UUID, String, String> senderHub : senderHubs)
                log.info(String.format(INDENT + "nodeId=%s, Address=%s, Mode=%s", senderHub.toArray()));

            log.info(DELIM);
        }

        if (arg().receiverHubs) {
            List<T3<UUID, String, String>> receiverHubs = res.getReceiverHubs();

            if (receiverHubs.isEmpty())
                log.info("Receiver hubs: not found");
            else
                log.info("Receiver hubs: " + receiverHubs.size());

            for (T3<UUID, String, String> receiverHub : receiverHubs)
                log.info(String.format(INDENT + "nodeId=%s, Address=%s, Mode=%s", receiverHub.toArray()));

            log.info(DELIM);
        }

        if (arg().otherNodes) {
            List<T3<UUID, String, String>> otherNodes = res.getOtherNodes();

            if (otherNodes.isEmpty())
                log.info("Other nodes: not found");
            else
                log.info("Other nodes: " + otherNodes.size());

            for (T3<UUID, String, String> otherNode : otherNodes)
                log.info(String.format(INDENT + "nodeId=%s, Address=%s, Mode=%s", otherNode.toArray()));

            log.info(DELIM);
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DrSubCommandsList.TOPOLOGY.text();
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class DrTopologyArguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrTopologyTaskArgs> {
        /** */
        private final boolean senderHubs;
        /** */
        private final boolean receiverHubs;
        /** */
        private final boolean dataNodes;
        /** */
        private final boolean otherNodes;

        /** */
        public DrTopologyArguments(boolean senderHubs, boolean receiverHubs, boolean dataNodes, boolean otherNodes) {
            this.senderHubs = senderHubs;
            this.receiverHubs = receiverHubs;
            this.dataNodes = dataNodes;
            this.otherNodes = otherNodes;
        }

        /** {@inheritDoc} */
        @Override public VisorDrTopologyTaskArgs toVisorArgs() {
            int flags = 0;

            if (senderHubs)
                flags |= SENDER_HUBS_FLAG;

            if (receiverHubs)
                flags |= RECEIVER_HUBS_FLAG;

            if (dataNodes)
                flags |= DATA_NODES_FLAG;

            if (otherNodes)
                flags |= OTHER_NODES_FLAG;

            return new VisorDrTopologyTaskArgs(flags);
        }
    }
}
