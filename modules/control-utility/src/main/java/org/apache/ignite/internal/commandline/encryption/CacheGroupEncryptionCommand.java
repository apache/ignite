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

package org.apache.ignite.internal.commandline.encryption;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupEncryptionTaskArg;
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupEncryptionTaskResult;
import org.apache.ignite.internal.visor.encryption.VisorEncryptionKeyIdsTask;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionResumeTask;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionStatusTask;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionSuspendTask;

import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.CACHE_GROUP_KEY_IDS;

/**
 * Base cache group encryption multinode subcommand.
 *
 * @param <T> Command result type.
 */
public abstract class CacheGroupEncryptionCommand<T> extends AbstractCommand<VisorCacheGroupEncryptionTaskArg> {
    /** Cache group reencryption task argument. */
    private VisorCacheGroupEncryptionTaskArg taskArg;

    /** {@inheritDoc} */
    @Override public VisorCacheGroupEncryptionTaskArg arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String grpName = argIter.nextArg("Сache group name is expected.");

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected command argument: " + argIter.peekNextArg());

        taskArg = new VisorCacheGroupEncryptionTaskArg(grpName);
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorCacheGroupEncryptionTaskResult<T> res = executeTaskByNameOnNode(
                client,
                visorTaskName(),
                taskArg,
                BROADCAST_UUID,
                clientCfg
            );

            printResults(res, taskArg.groupName(), log);

            return res;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /**
     * @param res Response.
     * @param grpName Cache group name.
     * @param log Logger.
     */
    protected void printResults(VisorCacheGroupEncryptionTaskResult<T> res, String grpName, Logger log) {
        Map<UUID, IgniteException> exceptions = res.exceptions();

        for (Map.Entry<UUID, IgniteException> entry : exceptions.entrySet()) {
            log.info(INDENT + "Node " + entry.getKey() + ":");

            log.info(String.format("%sfailed to execute command for the cache group \"%s\": %s.",
                DOUBLE_INDENT, grpName, entry.getValue().getMessage()));
        }

        Map<UUID, T> results = res.results();

        for (Map.Entry<UUID, T> entry : results.entrySet()) {
            log.info(INDENT + "Node " + entry.getKey() + ":");

            printNodeResult(entry.getValue(), grpName, log);
        }
    }

    /**
     * @param res Response.
     * @param grpName Cache group name.
     * @param log Logger.
     */
    protected abstract void printNodeResult(T res, String grpName, Logger log);

    /**
     * @return Visor task name.
     */
    protected abstract String visorTaskName();

    /** Subcommand to Display re-encryption status of the cache group. */
    protected static class ReencryptionStatus extends CacheGroupEncryptionCommand<Long> {
        /** {@inheritDoc} */
        @Override protected void printNodeResult(Long bytesLeft, String grpName, Logger log) {
            if (bytesLeft == -1)
                log.info(DOUBLE_INDENT + "re-encryption completed or not required");
            else if (bytesLeft == 0)
                log.info(DOUBLE_INDENT + "re-encryption will be completed after the next checkpoint");
            else
                log.info(String.format("%s%d KB of data left for re-encryption", DOUBLE_INDENT, bytesLeft / 1024));
        }

        /** {@inheritDoc} */
        @Override protected String visorTaskName() {
            return VisorReencryptionStatusTask.class.getName();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return EncryptionSubcommands.REENCRYPTION_STATUS.text().toUpperCase();
        }

        /** {@inheritDoc} */
        @Override public void printUsage(Logger log) {
            Command.usage(log, "Display re-encryption status of the cache group:", CommandList.ENCRYPTION,
                EncryptionSubcommands.REENCRYPTION_STATUS.toString(), "cacheGroupName");
        }
    }

    /** Subcommand to view current encryption key IDs of the cache group. */
    protected static class CacheKeyIds extends CacheGroupEncryptionCommand<List<Integer>> {
        /** {@inheritDoc} */
        @Override protected void printResults(
            VisorCacheGroupEncryptionTaskResult<List<Integer>> res,
            String grpName,
            Logger log
        ) {
            log.info("Encryption key identifiers for cache: " + grpName);

            super.printResults(res, grpName, log);
        }

        /** {@inheritDoc} */
        @Override protected void printNodeResult(List<Integer> keyIds, String grpName, Logger log) {
            if (F.isEmpty(keyIds)) {
                log.info(DOUBLE_INDENT + "---");

                return;
            }

            for (int i = 0; i < keyIds.size(); i++)
                log.info(DOUBLE_INDENT + keyIds.get(i) + (i == 0 ? " (active)" : ""));
        }

        /** {@inheritDoc} */
        @Override protected String visorTaskName() {
            return VisorEncryptionKeyIdsTask.class.getName();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return CACHE_GROUP_KEY_IDS.text().toUpperCase();
        }

        /** {@inheritDoc} */
        @Override public void printUsage(Logger log) {
            Command.usage(log, "View encryption key identifiers of the cache group:", ENCRYPTION,
                CACHE_GROUP_KEY_IDS.toString(), "cacheGroupName");
        }
    }

    /** Subcommand to suspend re-encryption of the cache group. */
    protected static class SuspendReencryption extends CacheGroupEncryptionCommand<Boolean> {
        /** {@inheritDoc} */
        @Override protected String visorTaskName() {
            return VisorReencryptionSuspendTask.class.getName();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return EncryptionSubcommands.REENCRYPTION_SUSPEND.text().toUpperCase();
        }

        /** {@inheritDoc} */
        @Override public void printUsage(Logger log) {
            Command.usage(log, "Suspend re-encryption of the cache group:", CommandList.ENCRYPTION,
                EncryptionSubcommands.REENCRYPTION_SUSPEND.toString(), "cacheGroupName");
        }

        /** {@inheritDoc} */
        @Override protected void printNodeResult(Boolean success, String grpName, Logger log) {
            log.info(String.format("%sre-encryption of the cache group \"%s\" has %sbeen suspended.",
                DOUBLE_INDENT, grpName, (success ? "" : "already ")));
        }

        /** {@inheritDoc} */
        @Override protected void printResults(
            VisorCacheGroupEncryptionTaskResult<Boolean> res,
            String grpName,
            Logger log
        ) {
            super.printResults(res, grpName, log);

            log.info("");
            log.info("Note: the re-encryption suspend status is not persisted, re-encryption will be started " +
                "automatically after the node is restarted.");
            log.info("");
        }
    }

    /** Subcommand to resume re-encryption of the cache group. */
    protected static class ResumeReencryption extends CacheGroupEncryptionCommand<Boolean> {
        /** {@inheritDoc} */
        @Override protected String visorTaskName() {
            return VisorReencryptionResumeTask.class.getName();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return EncryptionSubcommands.REENCRYPTION_RESUME.text().toUpperCase();
        }

        /** {@inheritDoc} */
        @Override public void printUsage(Logger log) {
            Command.usage(log, "Resume re-encryption of the cache group:", CommandList.ENCRYPTION,
                EncryptionSubcommands.REENCRYPTION_RESUME.toString(), "cacheGroupName");
        }

        /** {@inheritDoc} */
        @Override protected void printNodeResult(Boolean success, String grpName, Logger log) {
            log.info(String.format("%sre-encryption of the cache group \"%s\" has %sbeen resumed.",
                DOUBLE_INDENT, grpName, (success ? "" : "already ")));
        }
    }
}
