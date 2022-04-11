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
import org.apache.ignite.internal.commandline.cache.argument.CacheMetricsManageCommandArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTask;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTaskArg;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.METRICS;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsManageCommandArg.ALL_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsManageCommandArg.CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsManageCommandArg.DISABLE;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsManageCommandArg.ENABLE;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsManageCommandArg.STATUS;

/**
 * Cache sub-command for a cache metrics collection management. It provides to enable, disable or show status.
 */
public class CacheMetricsManage extends AbstractCommand<VisorCacheMetricsManageTaskArg> {
    /** Incorrect sub command message. */
    public static final String INCORRECT_SUB_COMMAND_MESSAGE = "Expected correct sub-command.";

    /** Incorrect cache argument message. */
    public static final String INCORRECT_CACHE_ARGUMENT_MESSAGE =
        String.format("'%s' or '%s' arguments should be passed.", CACHES, ALL_CACHES);

    /** Invalid caches list message. */
    public static final String INVALID_CACHES_LIST_MESSAGE = "comma-separated list of cache names";

    /** No caches affected message. */
    public static final String NO_CACHES_AFFECTED_MESSAGE = "No caches affected. Are there any caches in cluster?";

    /** Success message. */
    public static final String SUCCESS_MESSAGE = "Command performed successfully.";

    /** Task argument. */
    private VisorCacheMetricsManageTaskArg arg;

    /** Cache metrics sub-command argument. */
    private CacheMetricsManageCommandArg subCmdArg;

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

        switch (subCmdArg) {
            case ENABLE:
            case DISABLE:
                resultMsg = ((Integer)result) > 0 ? SUCCESS_MESSAGE : NO_CACHES_AFFECTED_MESSAGE;

                break;
            case STATUS:
                Map<String, Boolean> statusTaskResult = (Map<String, Boolean>)result;

                if (statusTaskResult.isEmpty())
                    resultMsg = NO_CACHES_AFFECTED_MESSAGE;
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
        String desc = "Manages user cache metrics collection: enables, disables them or shows status.";

        usageCache(log, METRICS, desc, null, or(ENABLE, DISABLE, STATUS),
            or(CACHES + "cache1" + optional(",...,cacheN"), ALL_CACHES));
    }

    /** {@inheritDoc} */
    @Override public VisorCacheMetricsManageTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        subCmdArg = ensureArg(argIter, INCORRECT_SUB_COMMAND_MESSAGE);

        CacheMetricsManageCommandArg cachesArg = ensureArg(argIter, INCORRECT_CACHE_ARGUMENT_MESSAGE);

        switch (cachesArg) {
            case CACHES:
                Set<String> caches = argIter.nextStringSet(INVALID_CACHES_LIST_MESSAGE);

                if (caches.isEmpty())
                    throw new IllegalArgumentException("Expected " + INVALID_CACHES_LIST_MESSAGE);

                arg = new VisorCacheMetricsManageTaskArg(subCmdArg.taskArgumentSubCommand(), caches);

                break;
            case ALL_CACHES:
                arg = new VisorCacheMetricsManageTaskArg(subCmdArg.taskArgumentSubCommand());

                break;
            default:
                throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);
        }
    }

    /**
     * @param argIter Argument iterator.
     * @param errorMsg Error message.
     */
    private CacheMetricsManageCommandArg ensureArg(CommandArgIterator argIter, String errorMsg) {
        CacheMetricsManageCommandArg arg = CommandArgUtils.of(argIter.nextArg(errorMsg),
            CacheMetricsManageCommandArg.class);

        if (arg == null)
            throw new IllegalArgumentException(errorMsg);

        return arg;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return METRICS.text().toUpperCase();
    }
}
