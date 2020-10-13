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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.persistence.CleanSubcommandArg;
import org.apache.ignite.internal.commandline.persistence.PersistenceArguments;
import org.apache.ignite.internal.commandline.persistence.PersistenceSubcommands;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.persistence.PersistenceCleanSettings;
import org.apache.ignite.internal.visor.persistence.PersistenceCleanType;
import org.apache.ignite.internal.visor.persistence.PersistenceTask;
import org.apache.ignite.internal.visor.persistence.PersistenceTaskArg;
import org.apache.ignite.internal.visor.persistence.PersistenceTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.PERSISTENCE;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.persistence.CleanSubcommandArg.ALL;
import static org.apache.ignite.internal.commandline.persistence.CleanSubcommandArg.CACHES;
import static org.apache.ignite.internal.commandline.persistence.CleanSubcommandArg.CORRUPTED;
import static org.apache.ignite.internal.commandline.persistence.PersistenceSubcommands.CLEAN;
import static org.apache.ignite.internal.commandline.persistence.PersistenceSubcommands.of;

/** */
public class PersistenceCommand implements Command<PersistenceArguments> {
    /** */
    private PersistenceArguments cleaningArgs;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Optional<GridClientNode> firstNodeOpt = client.compute().nodes().stream().findFirst();

            if (firstNodeOpt.isPresent()) {
                UUID uuid = firstNodeOpt.get().nodeId();

                PersistenceTaskResult res = executeTaskByNameOnNode(client,
                    PersistenceTask.class.getName(),
                    convertArguments(cleaningArgs),
                    uuid,
                    clientCfg
                );

                printResult(res, logger);

            }
            else
                logger.warning("No nodes found in topology, command won't be executed.");
        }
        catch (Throwable t) {
            logger.severe("Failed to execute persistence command='" + cleaningArgs.subcommand().text() + "'");
            logger.severe(CommandLogger.errorMessage(t));

            throw t;
        }

        return null;
    }

    private void printResult(PersistenceTaskResult res, Logger logger) {
        if (!res.inMaintenanceMode()) {
            logger.warning("Persistence command can be sent only to node in Maintenance Mode.");

            return;
        }
        else {
            logger.info("Maintenance task is " + (!res.maintenanceTaskCompleted() ? "not " : "") + "fixed.");

            List<String> cleanedCaches = res.cleanedCaches();

            if (cleanedCaches != null && !cleanedCaches.isEmpty()) {
                String cacheNames = cleanedCaches.stream().collect(Collectors.joining(", "));

                logger.info("Cleaned caches: [" + cacheNames + ']');
            }
        }
    }

    /** {@inheritDoc} */
    @Override public PersistenceArguments arg() {
        return cleaningArgs;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {

    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            cleaningArgs = new PersistenceArguments.Builder(PersistenceSubcommands.INFO).build();

            return;
        }

        PersistenceSubcommands cmd = of(argIter.nextArg("Expected persistence maintenance action"));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct persistence maintenance action");

        PersistenceArguments.Builder bldr = new PersistenceArguments.Builder(cmd);

        switch (cmd) {
            case CLEAN:
                CleanSubcommandArg cleanSubcommandArg = CommandArgUtils.of(
                    argIter.nextArg("Expected one of clean subcommand arguments"), CleanSubcommandArg.class
                );

                if (cleanSubcommandArg == null)
                    throw new IllegalArgumentException("Expected one of clean subcommand arguments");

                bldr.withCleanSubcommandArg(cleanSubcommandArg);

                if (cleanSubcommandArg == ALL || cleanSubcommandArg == CORRUPTED)
                    break;

                if (cleanSubcommandArg == CACHES) {
                    Set<String> caches = argIter.nextStringSet("list of cache names");

                    if (F.isEmpty(caches))
                        throw new IllegalArgumentException("Empty list of cache names");

                    bldr.withCacheNames(new ArrayList<>(caches));
                }

                break;
        }

        cleaningArgs = bldr.build();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PERSISTENCE.toCommandName();
    }

    /** */
    private PersistenceTaskArg convertArguments(PersistenceArguments args) {
        PersistenceCleanSettings cleanSettings = convertCleanSettings(args);

        PersistenceTaskArg taskArgs = new PersistenceTaskArg(args.subcommand().operation(), cleanSettings);

        return taskArgs;
    }

    /** */
    private PersistenceCleanSettings convertCleanSettings(PersistenceArguments args) {
        if (args.subcommand() != CLEAN)
            return null;

        PersistenceCleanType type;

        switch (args.cleanArg()) {
            case ALL:
                type = PersistenceCleanType.ALL;

                break;
            case CORRUPTED:
                type = PersistenceCleanType.CORRUPTED;

                break;

            default:
                type = PersistenceCleanType.CACHES;
        }

        PersistenceCleanSettings settings = new PersistenceCleanSettings(type, args.cachesList());

        return settings;
    }
}
