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
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.baseline.VisorBaselineAutoAdjustSettings;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskArg;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskResult;

import static java.lang.Boolean.TRUE;
import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;

public class BaselineCommand implements Command {

    @Override public String confirmationPrompt(Arguments args) {
        if (org.apache.ignite.internal.commandline.baseline.BaselineCommand.COLLECT != args.baselineArguments().getCmd())
            return "Warning: the command will perform changes in baseline.";

        return null;
    }


    /**
     * Change baseline.
     *
     * @param args Arguments.
     * @param clientCfg Client configuration.
     * @throws Exception If failed to execute baseline action.
     */
    @Override public Object execute(Arguments args, GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        BaselineArguments baselineArgs = args.baselineArguments();

        try (GridClient client = GridClientFactory.start(clientCfg)){
            VisorBaselineTaskResult res = executeTask(client, VisorBaselineTask.class, toVisorArguments(baselineArgs), clientCfg);

            baselinePrint0(res, logger);
        }
        catch (Throwable e) {
            logger.error("Failed to execute baseline command='" + baselineArgs.getCmd().text() + "'", e);

            throw e;
        }

        return null;
    }

    /**
     * Prepare task argument.
     *
     * @param args Argument from command line.
     * @return Task argument.
     */
    private VisorBaselineTaskArg toVisorArguments(BaselineArguments args) {
        VisorBaselineAutoAdjustSettings settings = args.getCmd() == org.apache.ignite.internal.commandline.baseline.BaselineCommand.AUTO_ADJUST
            ? new VisorBaselineAutoAdjustSettings(args.getEnableAutoAdjust(), args.getSoftBaselineTimeout())
            : null;

        return new VisorBaselineTaskArg(args.getCmd().visorBaselineOperation(), args.getTopVer(), args.getConsistentIds(), settings);
    }

    /**
     * Print baseline topology.
     *
     * @param res Task result with baseline topology.
     */
    private void baselinePrint0(VisorBaselineTaskResult res, CommandLogger logger) {
        logger.log("Cluster state: " + (res.isActive() ? "active" : "inactive"));
        logger.log("Current topology version: " + res.getTopologyVersion());
        VisorBaselineAutoAdjustSettings autoAdjustSettings = res.getAutoAdjustSettings();

        if (autoAdjustSettings != null) {
            logger.log("Baseline auto adjustment " + (TRUE.equals(autoAdjustSettings.getEnabled()) ? "enabled" : "disabled")
                + ": softTimeout=" + autoAdjustSettings.getSoftTimeout()
            );
        }

        if (autoAdjustSettings.enabled) {
            if (res.isBaselineAdjustInProgress())
                logger.log("Baseline auto-adjust is in progress");
            else if (res.getRemainingTimeToBaselineAdjust() < 0)
                logger.log("Baseline auto-adjust are not scheduled");
            else
                logger.log("Baseline auto-adjust will happen in '" + res.getRemainingTimeToBaselineAdjust() + "' ms");
        }

        logger.nl();

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

        logger.log("Current topology version: " + res.getTopologyVersion() + crdStr);
        logger.nl();

        if (F.isEmpty(baseline))
            logger.log("Baseline nodes not found.");
        else {
            logger.log("Baseline nodes:");

            for (VisorBaselineNode node : baseline.values()) {
                VisorBaselineNode srvNode = srvs.get(node.getConsistentId());

                String state = ", State=" + (srvNode != null ? "ONLINE" : "OFFLINE");

                String order = srvNode != null ? ", Order=" + srvNode.getOrder() : "";

                logger.logWithIndent("ConsistentId=" + node.getConsistentId() + state + order, 2);
            }

            logger.log(DELIM);
            logger.log("Number of baseline nodes: " + baseline.size());

            logger.nl();

            List<VisorBaselineNode> others = new ArrayList<>();

            for (VisorBaselineNode node : srvs.values()) {
                if (!baseline.containsKey(node.getConsistentId()))
                    others.add(node);
            }

            if (F.isEmpty(others))
                logger.log("Other nodes not found.");
            else {
                logger.log("Other nodes:");

                for (VisorBaselineNode node : others)
                    logger.logWithIndent("ConsistentId=" + node.getConsistentId() + ", Order=" + node.getOrder(), 2);

                logger.log("Number of other nodes: " + others.size());
            }
        }
    }

}
