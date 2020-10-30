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

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.encryption.VisorGroupReencryptionActionType;
import org.apache.ignite.internal.visor.encryption.VisorGroupReencryptionTask;
import org.apache.ignite.internal.visor.encryption.VisorGroupReencryptionTaskArg;

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.visor.encryption.VisorGroupReencryptionActionType.SUSPEND;

/**
 * Subcommand to control the process of re-encryption of the cache group.
 */
public class GroupReencryptionCommand implements Command<VisorGroupReencryptionTaskArg> {
    /** Cache group reencryption task argument. */
    private VisorGroupReencryptionTaskArg taskArg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Map<UUID, Object> res = executeTaskByNameOnNode(
                client,
                VisorGroupReencryptionTask.class.getName(),
                taskArg,
                BROADCAST_UUID,
                clientCfg
            );

            switch (taskArg.type()) {
                case STATUS:
                    printStatusResult(taskArg.groupName(), res, log);

                    break;
                case SUSPEND:
                case RESUME:
                    printSuspendResumeResult(taskArg.groupName(), taskArg.type() == SUSPEND, res, log);

                    break;
                default:
                    assert false : "Unknown type: " + taskArg.type();
            }

            return res;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /**
     * @param grpName Cache group name.
     * @param suspend Suspend flag.
     * @param res Response.
     * @param log Logger.
     */
    private void printSuspendResumeResult(String grpName, boolean suspend, Map<UUID, Object> res, Logger log) {
        for (Map.Entry<UUID, Object> entry : res.entrySet()) {
            String msg;

            if (entry.getValue() instanceof Throwable) {
                msg = String.format("failed to %s re-encryption of the cache group \"%s\": %s.",
                    (suspend ? "suspend" : "resume"), grpName, ((Throwable)entry.getValue()).getMessage());
            }
            else {
                msg = String.format("re-encryption of the cache group \"%s\" has %sbeen %s.",
                    grpName, (((boolean)entry.getValue()) ? "" : "already "), suspend ? "suspended" : "resumed");
            }

            log.info(INDENT + "Node " + entry.getKey() + ": " + msg);
        }
    }

    /**
     * @param grpName Cache group name.
     * @param nodeStates Node ID(s) with number of bytes left for reencryption.
     * @param log Logger.
     */
    private void printStatusResult(String grpName, Map<UUID, Object> nodeStates, Logger log) {
        log.info("Re-encryption status for the cache group: " + grpName);

        for (Map.Entry<UUID, Object> entry : nodeStates.entrySet()) {
            log.info(INDENT + "Node: " + entry.getKey());

            if (entry.getValue() instanceof Throwable) {
                log.info(String.format("%sfailed to get re-encryption status of the cache group \"%s\": %s.",
                    DOUBLE_INDENT, grpName, ((Throwable)entry.getValue()).getMessage()));

                continue;
            }

            long bytesLeft = (Long)entry.getValue();

            if (bytesLeft == 0) {
                log.info(DOUBLE_INDENT + "re-encryption completed or not required");

                continue;
            }

            log.info(String.format("%s%d KB of data left for re-encryption", DOUBLE_INDENT, bytesLeft / 1024));
        }
    }

    /** {@inheritDoc} */
    @Override public VisorGroupReencryptionTaskArg arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        ReencryptionCommandArg cmdArg = ReencryptionCommandArg.STATUS;
        String grpName = argIter.nextArg("Сache group name is expected.");

        if (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("Failed to read command argument.");

            cmdArg = CommandArgUtils.of(arg, ReencryptionCommandArg.class);

            if (cmdArg == null)
                throw new IllegalArgumentException("Unexpected command argument: " + arg);

            if (argIter.hasNextSubArg()) {
                throw new IllegalArgumentException("Only one of the following options is expected: " +
                    GridFunc.concat(Arrays.asList(ReencryptionCommandArg.values()), ", "));
            }
        }

        taskArg = new VisorGroupReencryptionTaskArg(grpName, VisorGroupReencryptionActionType.valueOf(cmdArg.name()));
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Control the process of re-encryption of the cache group:", CommandList.ENCRYPTION,
            U.map(ReencryptionCommandArg.STATUS.argName(), "Display re-encryption status (default action).",
                ReencryptionCommandArg.SUSPEND.argName(), "Suspend re-encryption.",
                ReencryptionCommandArg.RESUME.argName(), "Resume re-encryption."),
            EncryptionSubcommands.GROUP_REENCRYPTION.toString(), "cacheGroupName",
            optional(ReencryptionCommandArg.STATUS, ReencryptionCommandArg.SUSPEND, ReencryptionCommandArg.RESUME));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return EncryptionSubcommands.GROUP_REENCRYPTION.text().toUpperCase();
    }

    /**
     * Reencryption management command arguments name.
     */
    private enum ReencryptionCommandArg implements CommandArg {
        /** Suspend reencryption argument. */
        SUSPEND("--suspend"),

        /** Resume reencryption argument. */
        RESUME("--resume"),

        /** Reencryption status argument. */
        STATUS("--status");

        /** Option name. */
        private final String name;

        /**
         * @param name Argument name.
         */
        ReencryptionCommandArg(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String argName() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return name;
        }
    }
}
