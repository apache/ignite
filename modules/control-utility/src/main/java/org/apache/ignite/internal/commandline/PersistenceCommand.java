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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.persistence.CleanAndBackupSubcommandArg;
import org.apache.ignite.internal.commandline.persistence.PersistenceArguments;
import org.apache.ignite.internal.commandline.persistence.PersistenceSubcommands;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.persistence.PersistenceCleanAndBackupSettings;
import org.apache.ignite.internal.visor.persistence.PersistenceCleanAndBackupType;
import org.apache.ignite.internal.visor.persistence.PersistenceTask;
import org.apache.ignite.internal.visor.persistence.PersistenceTaskArg;
import org.apache.ignite.internal.visor.persistence.PersistenceTaskResult;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.commandline.CommandList.PERSISTENCE;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.persistence.CleanAndBackupSubcommandArg.ALL;
import static org.apache.ignite.internal.commandline.persistence.CleanAndBackupSubcommandArg.CACHES;
import static org.apache.ignite.internal.commandline.persistence.CleanAndBackupSubcommandArg.CORRUPTED;
import static org.apache.ignite.internal.commandline.persistence.PersistenceSubcommands.BACKUP;
import static org.apache.ignite.internal.commandline.persistence.PersistenceSubcommands.CLEAN;
import static org.apache.ignite.internal.commandline.persistence.PersistenceSubcommands.INFO;
import static org.apache.ignite.internal.commandline.persistence.PersistenceSubcommands.of;

/** */
public class PersistenceCommand implements Command<PersistenceArguments> {
    /** */
    private PersistenceArguments cleaningArgs;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
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
            logger.error("Failed to execute persistence command='" + cleaningArgs.subcommand().text() + "'");
            logger.error(CommandLogger.errorMessage(t));

            throw t;
        }

        return null;
    }

    /**
     * Prints result of command execution: information about caches or result of clean/backup command.
     *
     * @param res {@link PersistenceTaskResult} object with results of command execution.
     * @param logger {@link IgniteLogger} to print output to.
     */
    private void printResult(PersistenceTaskResult res, IgniteLogger logger) {
        if (!res.inMaintenanceMode()) {
            logger.warning("Persistence command can be sent only to node in Maintenance Mode.");

            return;
        }
        //info command
        else if (res.cachesInfo() != null) {
            logger.info("Persistent caches found on node:");

            //sort results so corrupted caches occur in the list at the top
            res.cachesInfo().entrySet().stream().sorted((ci0, ci1) -> {
                IgniteBiTuple<Boolean, Boolean> t0 = ci0.getValue();
                IgniteBiTuple<Boolean, Boolean> t1 = ci1.getValue();

                boolean corrupted0 = t0.get1() || t0.get2();
                boolean corrupted1 = t1.get1() || t1.get2();

                if (corrupted0 && corrupted1)
                    return 0;
                else if (!corrupted0 && !corrupted1)
                    return 0;
                else if (corrupted0 && !corrupted1)
                    return -1;
                else
                    return 1;
            }).forEach(
                e -> {
                    IgniteBiTuple<Boolean, Boolean> t = e.getValue();

                    String status;

                    if (!t.get1())
                        status = "corrupted - WAL disabled globally.";
                    else if (!t.get1())
                        status = "corrupted - WAL disabled locally.";
                    else
                        status = "no corruption.";

                    logger.info(INDENT + "cache name: " + e.getKey() + ". Status: " + status);
                }
            );
        }
        //clean command
        else if (cleaningArgs != null && cleaningArgs.subcommand() == CLEAN) {
            logger.info("Maintenance task is " + (!res.maintenanceTaskCompleted() ? "not " : "") + "fixed.");

            List<String> cleanedCaches = res.handledCaches();

            if (cleanedCaches != null && !cleanedCaches.isEmpty()) {
                String cacheDirNames = String.join(", ", cleanedCaches);

                logger.info("Cache directories were cleaned: [" + cacheDirNames + ']');
            }

            List<String> failedToHandleCaches = res.failedCaches();

            if (failedToHandleCaches != null && !failedToHandleCaches.isEmpty()) {
                String failedToHandleCachesStr = String.join(", ", failedToHandleCaches);

                logger.info("Failed to clean following directories: [" + failedToHandleCachesStr + ']');
            }
        }
        // backup command
        else {
            List<String> backupCompletedCaches = res.handledCaches();

            if (backupCompletedCaches != null && !backupCompletedCaches.isEmpty()) {
                String cacheDirNames = String.join(", ", backupCompletedCaches);

                logger.info("Cache data files was backed up to the following directories in node's work directory: [" +
                    cacheDirNames + ']');
            }

            List<String> backupFailedCaches = res.failedCaches();

            if (backupFailedCaches != null && !backupFailedCaches.isEmpty()) {
                String backupFailedCachesStr = String.join(", ", backupFailedCaches);

                logger.info("Failed to backup the following directories in node's work directory: [" +
                    backupFailedCachesStr + ']');
            }
        }
    }

    /** {@inheritDoc} */
    @Override public PersistenceArguments arg() {
        return cleaningArgs;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        final String cacheNames = "cache1,cache2,cache3";

        usage(logger, "Print information about potentially corrupted caches on local node:",
            PERSISTENCE);
        usage(logger, "The same information is printed when info subcommand is passed:", PERSISTENCE,
            INFO.text());

        usage(logger, "Clean directories of caches with corrupted data files:", PERSISTENCE, CLEAN.text(),
            CORRUPTED.argName());
        usage(logger, "Clean directories of all caches:", PERSISTENCE, CLEAN.text(),
            ALL.argName());
        usage(logger, "Clean directories of only given caches:", PERSISTENCE, CLEAN.text(),
            CACHES.argName(), cacheNames);

        usage(logger, "Backup data files of corrupted caches only:", PERSISTENCE, BACKUP.text(),
            CORRUPTED.argName());
        usage(logger, "Backup data files of all caches:", PERSISTENCE, BACKUP.text(), ALL.argName());
        usage(logger, "Backup data files of only given caches:", PERSISTENCE, BACKUP.text(),
            CACHES.argName(), cacheNames);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            cleaningArgs = new PersistenceArguments.Builder(INFO).build();

            return;
        }

        PersistenceSubcommands cmd = of(argIter.nextArg("Expected persistence maintenance action"));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct persistence maintenance action");

        PersistenceArguments.Builder bldr = new PersistenceArguments.Builder(cmd);

        switch (cmd) {
            case BACKUP:
            case CLEAN:
                CleanAndBackupSubcommandArg cleanAndBackupSubcommandArg = CommandArgUtils.of(
                    argIter.nextArg("Expected one of subcommand arguments"), CleanAndBackupSubcommandArg.class
                );

                if (cleanAndBackupSubcommandArg == null)
                    throw new IllegalArgumentException("Expected one of subcommand arguments");

                bldr.withCleanAndBackupSubcommandArg(cleanAndBackupSubcommandArg);

                if (cleanAndBackupSubcommandArg == ALL || cleanAndBackupSubcommandArg == CORRUPTED)
                    break;

                if (cleanAndBackupSubcommandArg == CACHES) {
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
        PersistenceCleanAndBackupSettings cleanSettings = convertCleanAndBackupSettings(args);

        PersistenceTaskArg taskArgs = new PersistenceTaskArg(args.subcommand().operation(), cleanSettings);

        return taskArgs;
    }

    /** */
    private PersistenceCleanAndBackupSettings convertCleanAndBackupSettings(PersistenceArguments args) {
        if (args.subcommand() == INFO)
            return null;

        PersistenceCleanAndBackupType type;

        switch (args.cleanArg()) {
            case ALL:
                type = PersistenceCleanAndBackupType.ALL;

                break;
            case CORRUPTED:
                type = PersistenceCleanAndBackupType.CORRUPTED;

                break;

            default:
                type = PersistenceCleanAndBackupType.CACHES;
        }

        return new PersistenceCleanAndBackupSettings(type, args.cachesList());
    }
}
