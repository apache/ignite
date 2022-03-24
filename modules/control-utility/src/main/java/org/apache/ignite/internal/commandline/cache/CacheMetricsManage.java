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
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTask;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTaskArg;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.METRICS;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg.ALL_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg.DISABLE;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg.ENABLE;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg.STATUS;

/**
 * Cache sub-command for a cache metrics collection management. It provides to enable, disable or show status.
 */
public class CacheMetricsManage extends AbstractCommand<VisorCacheMetricsManageTaskArg> {
    /** Task argument. */
    private VisorCacheMetricsManageTaskArg arg;

    /** Cache metrics sub-command argument. */
    private CacheMetricsCommandArg subCmdArg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorCacheMetricsManageTaskResult taskResult = TaskExecutor.executeTaskByNameOnNode(client,
                VisorCacheMetricsManageTask.class.getName(), arg, null, clientCfg);

            return processTaskResult(log, taskResult.result());
        }
    }

    /**
     * @param log Logger.
     * @param result Task result.
     */
    private String processTaskResult(Logger log, Object result) {
        Objects.requireNonNull(result);

        String resultMsg;

        String noCachesAffectedMsg = "No caches affected. Are there any caches in cluster?";

        switch (subCmdArg) {
            case ENABLE:
            case DISABLE:
                resultMsg = ((Integer)result) > 0 ? "Command performed successfully." : noCachesAffectedMsg;

                break;
            case STATUS:
                Map<String, Boolean> statusTaskResult = (Map<String, Boolean>)result;

                if (statusTaskResult.isEmpty())
                    resultMsg = noCachesAffectedMsg;
                else {
                    resultMsg = "[Cache Name -> Metrics status]:" + U.nl();

                    Collection<String> rowsCollection = F.transform(statusTaskResult.entrySet(),
                        e -> e.getKey() + " -> " + (e.getValue() ? "enabled" : "disabled"));

                    String rowsStr = String.join(U.nl(), rowsCollection);

                    resultMsg += rowsStr;
                }

                break;
            default:
                throw new IllegalStateException("Unexpected value: " + subCmdArg);
        }

        log.info(resultMsg);

        return resultMsg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        String desc = "Manages user cache metrics collection: enables, disables it or shows status.";

        usageCache(log, METRICS, desc, null, or(ENABLE, DISABLE, STATUS),
            or("cache1" + optional(",...,cacheN"), ALL_CACHES));
    }

    /** {@inheritDoc} */
    @Override public VisorCacheMetricsManageTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String incorrectSubCmdMsg = "Expected correct sub-command.";

        String readSubCmd = argIter.nextArg(incorrectSubCmdMsg);

        subCmdArg = CommandArgUtils.of(readSubCmd, CacheMetricsCommandArg.class);

        if (subCmdArg == null)
            throw new IllegalArgumentException(incorrectSubCmdMsg);

        String cacheArgErrorMsg = "cache names list or '" + ALL_CACHES + "' argument.";

        Set<String> caches = argIter.nextStringSet(cacheArgErrorMsg);

        String allCachesStr;
        boolean applyToAllCaches = false;

        // An empty string set if a command or option was passed.
        if (caches.isEmpty()) {
            allCachesStr = argIter.nextArg("Expected " + cacheArgErrorMsg);

            applyToAllCaches = ALL_CACHES.argName().equals(allCachesStr);

            if (!applyToAllCaches)
                throw new IllegalArgumentException("Expected " + cacheArgErrorMsg);
        }

        arg = applyToAllCaches ? new VisorCacheMetricsManageTaskArg(subCmdArg.taskArgumentSubCommand()) :
            new VisorCacheMetricsManageTaskArg(subCmdArg.taskArgumentSubCommand(), caches);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return METRICS.text().toUpperCase();
    }
}
