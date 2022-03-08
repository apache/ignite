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

package org.apache.ignite.internal.commandline.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.VisorCacheMetricsStatusTask;
import org.apache.ignite.internal.visor.cache.VisorCacheMetricsStatusTaskArg;
import org.apache.ignite.internal.visor.cache.VisorCacheMetricsToggleTask;
import org.apache.ignite.internal.visor.cache.VisorCacheMetricsToggleTaskArg;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.METRICS;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg.ALL_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg.DISABLE;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg.ENABLE;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg.STATUS;

/**
 * Cache sub-command for a cache metrics collection management: enabling/disabling or showing status.
 */
public class CacheMetrics extends AbstractCommand<CacheMetrics.Arguments> {
    /** Parsed arguments. */
    private Arguments arguments;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Object taskResult = TaskExecutor.executeTaskByNameOnNode(client, arguments.taskClsName, arguments.taskArg,
                null, clientCfg);

            return processTaskResult(log, taskResult);
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /**
     * @param log Logger.
     * @param taskResult Task result.
     */
    private String processTaskResult(Logger log, Object taskResult) {
        String resultMsg = "";

        String emptyResultMsg = "Empty result: none of the specified caches were found.";
        String notFoundCachesMsg = "Not found caches:" + U.nl();

        switch (arguments.subCmdArg) {
            case ENABLE:
            case DISABLE:
                Set<String> toggleTaskResult = (Set<String>)taskResult;

                if (toggleTaskResult.isEmpty()) {
                    resultMsg = emptyResultMsg;

                    log.warning(resultMsg);
                }
                else {
                    String successMsg = "Command performed successfully for caches:";

                    resultMsg = successMsg + U.nl() + toggleTaskResult;

                    Collection<String> notFoundCaches = F.view(arguments.cacheNames,
                        name -> !toggleTaskResult.contains(name));

                    if (!notFoundCaches.isEmpty())
                        resultMsg += U.nl() + notFoundCachesMsg + notFoundCaches;

                    log.info(resultMsg);
                }

                break;
            case STATUS:
                Map<String, Boolean> statusTaskResult = (Map<String, Boolean>)taskResult;

                if (statusTaskResult.isEmpty()) {
                    resultMsg = emptyResultMsg;

                    log.warning(resultMsg);
                }
                else {
                    resultMsg = "[Cache Name -> Status]:" + U.nl();

                    Collection<String> rowsCollection = F.transform(statusTaskResult.entrySet(),
                        e -> e.getKey() + " -> " + (e.getValue() ? "ENABLED" : "DISABLED"));

                    String rowsStr = String.join(U.nl(), rowsCollection);

                    resultMsg += rowsStr;

                    Collection<String> notFoundCaches = F.view(arguments.cacheNames,
                        name -> !statusTaskResult.containsKey(name));

                    if (!notFoundCaches.isEmpty())
                        resultMsg += U.nl() + notFoundCachesMsg + notFoundCaches;

                    log.info(resultMsg);
                }
        }

        return resultMsg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        String desc = "Manages user cache metrics collection: enables, disables it or shows status.";

        usageCache(log, METRICS, desc, null, or(ENABLE, DISABLE, STATUS),
            or("cache1" + optional(",...,cacheN"), ALL_CACHES));
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return arguments;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String incorrectSubCmdMsg = "Expected correct sub-command.";

        String readSubCmd = argIter.nextArg(incorrectSubCmdMsg);

        CacheMetricsCommandArg subCmdArg = CommandArgUtils.of(readSubCmd, CacheMetricsCommandArg.class);

        if (subCmdArg == null)
            throw new IllegalArgumentException(incorrectSubCmdMsg);

        String cacheArgErrorMsg = "cache names list or '" + ALL_CACHES + "' argument.";

        Set<String> caches = argIter.nextStringSet(cacheArgErrorMsg);

        String allCachesStr;
        boolean applyToAllCaches = false;

        if (caches.isEmpty()) {
            allCachesStr = argIter.nextArg("Expected " + cacheArgErrorMsg);

            applyToAllCaches = ALL_CACHES.argName().equals(allCachesStr);

            if (!applyToAllCaches)
                throw new IllegalArgumentException("Expected " + cacheArgErrorMsg);
        }

        arguments = new Arguments(subCmdArg, applyToAllCaches, caches);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return METRICS.text().toUpperCase();
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Metrics sub-command argument. */
        private final CacheMetricsCommandArg subCmdArg;

        /** Apply to all caches flag. */
        private final boolean applyToAllCaches;

        /** Affected cache names. */
        private final Set<String> cacheNames;

        /** Task class name, obtained from command parameters. */
        private String taskClsName;

        /** Task argument, obtained from command parameters. */
        private IgniteDataTransferObject taskArg;

        /** */
        Arguments(CacheMetricsCommandArg subCmdArg, boolean applyToAllCaches, Set<String> cacheNames) {
            this.subCmdArg = subCmdArg;
            this.applyToAllCaches = applyToAllCaches;
            this.cacheNames = cacheNames;

            processArgs();
        }

        /** */
        private void processArgs() {
            switch (subCmdArg) {
                case ENABLE:
                case DISABLE:
                    taskArg = applyToAllCaches ?
                        new VisorCacheMetricsToggleTaskArg(subCmdArg == ENABLE, applyToAllCaches) :
                        new VisorCacheMetricsToggleTaskArg(subCmdArg == ENABLE, cacheNames);

                    taskClsName = VisorCacheMetricsToggleTask.class.getName();

                    break;
                case STATUS:
                    taskArg = applyToAllCaches ?
                        new VisorCacheMetricsStatusTaskArg(applyToAllCaches) :
                        new VisorCacheMetricsStatusTaskArg(cacheNames);

                    taskClsName = VisorCacheMetricsStatusTask.class.getName();
            }
        }
    }
}
