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
import java.util.List;
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
import org.apache.ignite.internal.commandline.systemview.SystemViewCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.cache.metrics.CacheMetricsManageSubCommand;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTask;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTaskArg;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsManageTaskResult;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.METRICS_MANAGE;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsManageCommandArg.ALL_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricsManageCommandArg.CACHES;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricsManageSubCommand.DISABLE;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricsManageSubCommand.ENABLE;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricsManageSubCommand.STATUS;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/**
 * Cache sub-command for a cache metrics collection management. It provides an ability to enable, disable or show status.
 */
public class CacheMetricsManage extends AbstractCommand<VisorCacheMetricsManageTaskArg> {
    /** Incorrect sub command message. */
    public static final String INCORRECT_SUB_COMMAND_MESSAGE = "Expected correct sub-command.";

    /** Incorrect cache argument message. */
    public static final String INCORRECT_CACHE_ARGUMENT_MESSAGE =
        String.format("Expected one of these arguments: '%s' or '%s'. Multiple arguments are not allowed.", CACHES, ALL_CACHES);

    /** Invalid caches list message. */
    public static final String INVALID_CACHES_LIST_MESSAGE = "comma-separated list of cache names.";

    /** No caches processed message. */
    public static final String NONE_CACHES_PROCESSED_MESSAGE = "None of caches have been processed. " +
        "Are there any caches in cluster?";

    /** Duplicated '--caches' option message. */
    public static final String DUPLICATED_CACHES_OPTION_MESSAGE = "Duplicated '" + CACHES + "' option found.";

    /** Duplicated '--all-caches' option message. */
    public static final String DUPLICATED_ALL_CACHES_OPTION_MESSAGE = "Duplicated '" + ALL_CACHES + "' option found.";

    /** Success message. */
    public static final String SUCCESS_MESSAGE = "Command performed successfully.";

    /** Task argument. */
    private VisorCacheMetricsManageTaskArg arg;

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

        switch (arg.subCommand()) {
            case ENABLE:
            case DISABLE:
                String resultMsg = ((Integer)result) > 0 ? SUCCESS_MESSAGE : NONE_CACHES_PROCESSED_MESSAGE;

                log.info(resultMsg);

                break;
            case STATUS:
                Map<String, Boolean> statusTaskResult = (Map<String, Boolean>)result;

                if (statusTaskResult.isEmpty())
                    log.info(NONE_CACHES_PROCESSED_MESSAGE);
                else {
                    Collection<List<?>> values = F.viewReadOnly(statusTaskResult.entrySet(),
                        e -> asList(e.getKey(), e.getValue() ? "enabled" : "disabled"));

                    SystemViewCommand.printTable(asList("Cache Name", "Metrics Enable Status"), asList(STRING, STRING), values, log);
                }

                break;
            default:
                throw new IllegalStateException("Unexpected value: " + arg.subCommand());
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        String desc = "Manages user cache metrics collection: enables, disables them or shows status.";

        usageCache(log, METRICS_MANAGE, desc, null, or(ENABLE, DISABLE, STATUS),
            or(CACHES + " cache1" + optional(",...,cacheN"), ALL_CACHES));
    }

    /** {@inheritDoc} */
    @Override public VisorCacheMetricsManageTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        CacheMetricsManageSubCommand subCmd = CacheMetricsManageSubCommand.of(argIter.nextArg(INCORRECT_SUB_COMMAND_MESSAGE));

        if (subCmd == null)
            throw new IllegalArgumentException(INCORRECT_SUB_COMMAND_MESSAGE);

        Set<String> cacheNames = null;

        boolean isAllCaches = false;

        while (argIter.hasNextSubArg()) {
            CacheMetricsManageCommandArg cmdArg = CommandArgUtils.of(
                argIter.nextArg(INCORRECT_CACHE_ARGUMENT_MESSAGE),
                CacheMetricsManageCommandArg.class
            );

            if (cmdArg == null)
                throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);

            switch (cmdArg) {
                case CACHES:
                    if (!F.isEmpty(cacheNames))
                        throw new IllegalArgumentException(DUPLICATED_CACHES_OPTION_MESSAGE);

                    cacheNames = argIter.nextStringSet(INVALID_CACHES_LIST_MESSAGE);

                    if (cacheNames.isEmpty())
                        throw new IllegalArgumentException("Expected " + INVALID_CACHES_LIST_MESSAGE);

                    break;
                case ALL_CACHES:
                    if (isAllCaches)
                        throw new IllegalArgumentException(DUPLICATED_ALL_CACHES_OPTION_MESSAGE);

                    isAllCaches = true;

                    break;
                default:
                    throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);
            }
        }

        // Fail if no arguments passed after subcommand
        if (F.isEmpty(cacheNames) && !isAllCaches)
            throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);

        // Fail if both '--all-caches' and '--caches' options are present (they are mutual exclusive options)
        if (!F.isEmpty(cacheNames) && isAllCaches)
            throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);

        arg = new VisorCacheMetricsManageTaskArg(subCmd, cacheNames);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return METRICS_MANAGE.text().toUpperCase();
    }
}
