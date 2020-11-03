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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.baseline.AutoAdjustCommandArg;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.commandline.baseline.BaselineSubcommands;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.baseline.VisorBaselineAutoAdjustSettings;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskArg;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskResult;

import static java.lang.Boolean.TRUE;
import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;
import static org.apache.ignite.internal.commandline.CommandList.BASELINE;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.baseline.BaselineSubcommands.of;

/**
 * Commands associated with baseline functionality.
 */
public class BaselineCommand implements Command<BaselineArguments> {
    /** Arguments. */
    private BaselineArguments baselineArgs;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        final String constistIds = "consistentId1[,consistentId2,....,consistentIdN]";

        Command.usage(logger, "Print cluster baseline topology:", BASELINE);
        Command.usage(logger, "Add nodes into baseline topology:", BASELINE, BaselineSubcommands.ADD.text(),
            constistIds, optional(CMD_AUTO_CONFIRMATION));
        Command.usage(logger, "Remove nodes from baseline topology:", BASELINE, BaselineSubcommands.REMOVE.text(),
            constistIds, optional(CMD_AUTO_CONFIRMATION));
        Command.usage(logger, "Set baseline topology:", BASELINE, BaselineSubcommands.SET.text(), constistIds,
            optional(CMD_AUTO_CONFIRMATION));
        Command.usage(logger, "Set baseline topology based on version:", BASELINE,
            BaselineSubcommands.VERSION.text() + " topologyVersion", optional(CMD_AUTO_CONFIRMATION));
        Command.usage(logger, "Set baseline autoadjustment settings:", BASELINE,
            BaselineSubcommands.AUTO_ADJUST.text(), "[disable|enable] [timeout <timeoutMillis>]", optional(CMD_AUTO_CONFIRMATION));
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (baselineArgs != null && BaselineSubcommands.COLLECT != baselineArgs.getCmd())
            return "Warning: the command will perform changes in baseline.";

        return null;
    }

    /**
     * Change baseline.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to execute baseline action.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            UUID coordinatorId = client.compute()
                //Only non client node can be coordinator.
                .nodes(node -> !node.isClient())
                .stream()
                .min(Comparator.comparingLong(GridClientNode::order))
                .map(GridClientNode::nodeId)
                .orElse(null);

            VisorBaselineTaskResult res = executeTaskByNameOnNode(
                client,
                VisorBaselineTask.class.getName(),
                toVisorArguments(baselineArgs),
                coordinatorId,
                clientCfg
            );

            baselinePrint0(res, logger);
        }
        catch (Throwable e) {
            logger.severe("Failed to execute baseline command='" + baselineArgs.getCmd().text() + "'");
            logger.severe(CommandLogger.errorMessage(e));

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public BaselineArguments arg() {
        return baselineArgs;
    }

    /**
     * Prepare task argument.
     *
     * @param args Argument from command line.
     * @return Task argument.
     */
    private VisorBaselineTaskArg toVisorArguments(BaselineArguments args) {
        VisorBaselineAutoAdjustSettings settings = args.getCmd() == BaselineSubcommands.AUTO_ADJUST
            ? new VisorBaselineAutoAdjustSettings(args.getEnableAutoAdjust(), args.getSoftBaselineTimeout())
            : null;

        return new VisorBaselineTaskArg(args.getCmd().visorBaselineOperation(), args.getTopVer(), args.getConsistentIds(), settings);
    }

    /**
     * Print baseline topology.
     *
     * @param res Task result with baseline topology.
     */
    private void baselinePrint0(VisorBaselineTaskResult res, Logger logger) {
        logger.info("Cluster state: " + (res.isActive() ? "active" : "inactive"));
        logger.info("Current topology version: " + res.getTopologyVersion());
        VisorBaselineAutoAdjustSettings autoAdjustSettings = res.getAutoAdjustSettings();

        if (autoAdjustSettings != null) {
            logger.info("Baseline auto adjustment " + (TRUE.equals(autoAdjustSettings.getEnabled()) ? "enabled" : "disabled")
                + ": softTimeout=" + autoAdjustSettings.getSoftTimeout()
            );
        }

        if (autoAdjustSettings.enabled) {
            if (res.isBaselineAdjustInProgress())
                logger.info("Baseline auto-adjust is in progress");
            else if (res.getRemainingTimeToBaselineAdjust() < 0)
                logger.info("Baseline auto-adjust are not scheduled");
            else
                logger.info("Baseline auto-adjust will happen in '" + res.getRemainingTimeToBaselineAdjust() + "' ms");
        }

        logger.info("");

        Map<String, VisorBaselineNode> baseline = res.getBaseline();

        Map<String, VisorBaselineNode> srvs = res.getServers();

        // if task runs on a node with VisorBaselineNode of old version (V1) we'll get order=null for all nodes.

        String crdStr = srvs.values().stream()
            // check for not null
            .filter(node -> node.getOrder() != null)
            .min(Comparator.comparing(VisorBaselineNode::getOrder))
            // format
            .map(crd -> " (Coordinator: ConsistentId=" + crd.getConsistentId() + ", Order=" + crd.getOrder() + ")")
            .orElse("");

        logger.info("Current topology version: " + res.getTopologyVersion() + crdStr);
        logger.info("");

        if (F.isEmpty(baseline))
            logger.info("Baseline nodes not found.");
        else {
            logger.info("Baseline nodes:");

            for (VisorBaselineNode node : baseline.values()) {
                VisorBaselineNode srvNode = srvs.get(node.getConsistentId());

                String state = ", State=" + (srvNode != null ? "ONLINE" : "OFFLINE");

                String order = srvNode != null ? ", Order=" + srvNode.getOrder() : "";

                logger.info(DOUBLE_INDENT + "ConsistentId=" + node.getConsistentId() + state + order);
            }

            logger.info(DELIM);
            logger.info("Number of baseline nodes: " + baseline.size());

            logger.info("");

            List<VisorBaselineNode> others = new ArrayList<>();

            for (VisorBaselineNode node : srvs.values()) {
                if (!baseline.containsKey(node.getConsistentId()))
                    others.add(node);
            }

            if (F.isEmpty(others))
                logger.info("Other nodes not found.");
            else {
                logger.info("Other nodes:");

                for (VisorBaselineNode node : others)
                    logger.info(DOUBLE_INDENT + "ConsistentId=" + node.getConsistentId() + ", Order=" + node.getOrder());

                logger.info("Number of other nodes: " + others.size());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            this.baselineArgs = new BaselineArguments.Builder(BaselineSubcommands.COLLECT).build();

            return;
        }

        BaselineSubcommands cmd = of(argIter.nextArg("Expected baseline action"));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct baseline action");

        BaselineArguments.Builder baselineArgs = new BaselineArguments.Builder(cmd);

        switch (cmd) {
            case ADD:
            case REMOVE:
            case SET:
                Set<String> ids = argIter.nextStringSet("list of consistent ids");

                if (F.isEmpty(ids))
                    throw new IllegalArgumentException("Empty list of consistent IDs");

                baselineArgs.withConsistentIds(new ArrayList<>(ids));

                break;

            case VERSION:
                baselineArgs.withTopVer(argIter.nextNonNegativeLongArg("topology version"));

                break;

            case AUTO_ADJUST:
                do {
                    AutoAdjustCommandArg autoAdjustArg = CommandArgUtils.of(
                        argIter.nextArg("Expected one of auto-adjust arguments"), AutoAdjustCommandArg.class
                    );

                    if (autoAdjustArg == null)
                        throw new IllegalArgumentException("Expected one of auto-adjust arguments");

                    if (autoAdjustArg == AutoAdjustCommandArg.ENABLE || autoAdjustArg == AutoAdjustCommandArg.DISABLE)
                        baselineArgs.withEnable(autoAdjustArg == AutoAdjustCommandArg.ENABLE);

                    if (autoAdjustArg == AutoAdjustCommandArg.TIMEOUT)
                        baselineArgs.withSoftBaselineTimeout(argIter.nextNonNegativeLongArg("soft timeout"));
                }
                while (argIter.hasNextSubArg());

                break;
        }

        this.baselineArgs = baselineArgs.build();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return BASELINE.toCommandName();
    }
}
