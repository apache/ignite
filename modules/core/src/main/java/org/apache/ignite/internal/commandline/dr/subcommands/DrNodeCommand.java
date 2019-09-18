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
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.dr.DrSubCommandsList;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.dr.VisorDrNodeTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrNodeTaskResult;

import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

/** */
public class DrNodeCommand
    extends DrAbstractRemoteSubCommand<VisorDrNodeTaskArgs, VisorDrNodeTaskResult, DrNodeCommand.DrNodeArguments>
{
    /** Config parameter. */
    public static final String CONFIG_PARAM = "--config";
    /** Metrics parameter. */
    public static final String METRICS_PARAM = "--metrics";
    /** Clear store parameter. */
    public static final String CLEAR_STORE_PARAM = "--clear-store";
    /** Node Id. */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrNodeTask";
    }

    /** {@inheritDoc} */
    @Override public DrNodeArguments parseArguments0(CommandArgIterator argIter) {
        String nodeIdStr = argIter.nextArg("nodeId value expected.");

        try {
            nodeId = UUID.fromString(nodeIdStr);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("nodeId must be UUID.", e);
        }

        boolean config = false;
        boolean metrics = false;
        boolean clearStore = false;

        String nextArg;

        //noinspection LabeledStatement
        args_loop: while ((nextArg = argIter.peekNextArg()) != null) {
            switch (nextArg.toLowerCase(Locale.ENGLISH)) {
                case CONFIG_PARAM:
                    config = true;

                    break;

                case METRICS_PARAM:
                    metrics = true;

                    break;

                case CLEAR_STORE_PARAM:
                    clearStore = true;

                    break;

                default:
                    //noinspection BreakStatementWithLabel
                    break args_loop;
            }

            // Skip peeked argument.
            argIter.nextArg(null);
        }

        return new DrNodeArguments(config, metrics, clearStore);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (arg().clearStore)
            return "Warning: this command will clear DR store.";

        return null;
    }

    /** {@inheritDoc} */
    @Override protected VisorDrNodeTaskResult execute0(
        GridClientConfiguration clientCfg,
        GridClient client
    ) throws Exception {
        return executeTaskByNameOnNode(
            client,
            visorTaskName(),
            arg().toVisorArgs(),
            nodeId,
            clientCfg
        );
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrNodeTaskResult res, Logger log) {
        log.info("Data Center ID: " + res.getDataCenterId());

        log.info("Node addresses: " + res.getAddresses());

        log.info("Mode=" + res.getMode() + (res.getDataNode() ? ", Baseline node" : ""));

        log.info(DELIM);

        if (res.getDataCenterId() == 0) {
            log.info("Data Replication state: is not configured.");

            return;
        }

        List<T2<Byte, List<String>>> sndDataCenters = res.getSenderDataCenters();
        if (sndDataCenters != null && !sndDataCenters.isEmpty()) {
            log.info("Node is configured to send data to:");

            for (T2<Byte, List<String>> dataCenter : sndDataCenters)
                log.info(String.format(INDENT + "DataCenterId=%d, Addresses=%s", dataCenter.toArray()));
        }

        String receiverAddr = res.getReceiverAddress();
        if (receiverAddr != null) {
            log.info("Node is configured to receive data:");

            log.info(INDENT + "Address=" + receiverAddr);
        }

        if (!res.getResponseMsgs().isEmpty()) {
            log.info(DELIM);

            for (String responseMsg : res.getResponseMsgs())
                log.info(responseMsg);
        }

        printList(log, res.getCommonConfig(), "Common configuration:");
        printList(log, res.getSenderConfig(), "Sender configuration:");
        printList(log, res.getReceiverConfig(), "Receiver configuration:");

        printList(log, res.getSenderMetrics(), "Sender metrics:");
        printList(log, res.getReceiverMetrics(), "Receiver metrics:");
    }

    /** */
    private static void printList(Logger log, List<T2<String, Object>> cfg, String s) {
        if (cfg != null && !cfg.isEmpty()) {
            log.info(s);

            for (T2<String, Object> t2 : cfg)
                log.info(String.format(INDENT + "%s=%s", t2.toArray()));
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DrSubCommandsList.NODE.text();
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class DrNodeArguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrNodeTaskArgs> {
        /** Config. */
        private final boolean config;
        /** Metrics. */
        private final boolean metrics;
        /** Clear store. */
        private final boolean clearStore;

        /**
         * @param config Config.
         * @param metrics Metrics.
         * @param clearStore Clear store.
         */
        public DrNodeArguments(boolean config, boolean metrics, boolean clearStore) {
            this.config = config;
            this.metrics = metrics;
            this.clearStore = clearStore;
        }

        /** {@inheritDoc} */
        @Override public VisorDrNodeTaskArgs toVisorArgs() {
            return new VisorDrNodeTaskArgs(config, metrics, clearStore);
        }
    }
}
