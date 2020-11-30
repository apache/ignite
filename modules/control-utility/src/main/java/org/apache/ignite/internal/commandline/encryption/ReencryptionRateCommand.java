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

import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupEncryptionTaskResult;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionRateTask;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionRateTaskArg;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.REENCRYPTION_RATE;

/**
 * View/change cache group re-encryption rate limit subcommand.
 */
public class ReencryptionRateCommand extends AbstractCommand<VisorReencryptionRateTaskArg> {
    /** Re-encryption rate task argument. */
    private VisorReencryptionRateTaskArg taskArg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorCacheGroupEncryptionTaskResult<Double> res = executeTaskByNameOnNode(
                client,
                VisorReencryptionRateTask.class.getName(),
                taskArg,
                BROADCAST_UUID,
                clientCfg
            );

            Map<UUID, IgniteException> exceptions = res.exceptions();

            for (Map.Entry<UUID, IgniteException> entry : exceptions.entrySet()) {
                log.info(INDENT + "Node " + entry.getKey() + ":");
                log.info(DOUBLE_INDENT +
                    "failed to get/set re-encryption rate limit: " + entry.getValue().getMessage());
            }

            Map<UUID, Double> results = res.results();
            boolean read = taskArg.rate() == null;

            for (Map.Entry<UUID, Double> entry : results.entrySet()) {
                log.info(INDENT + "Node " + entry.getKey() + ":");

                double rateLimit = read ? entry.getValue() : taskArg.rate();

                if (rateLimit == 0)
                    log.info(DOUBLE_INDENT + "re-encryption rate is not limited.");
                else {
                    log.info(String.format("%sre-encryption rate %s limited to %.2f MB/s.",
                        DOUBLE_INDENT, (read ? "is" : "has been"), rateLimit));
                }
            }

            return null;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public VisorReencryptionRateTaskArg arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Double rateLimit = null;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("Failed to read command argument.");

            ReencryptionRateCommandArg cmdArg = CommandArgUtils.of(arg, ReencryptionRateCommandArg.class);

            if (cmdArg == ReencryptionRateCommandArg.LIMIT) {
                String rateLimitArg = argIter.nextArg("Expected decimal value for re-encryption rate.");

                try {
                    rateLimit = Double.parseDouble(rateLimitArg);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Failed to parse " + ReencryptionRateCommandArg.LIMIT +
                        " command argument. Decimal value expected.", e);
                }
            }
            else
                throw new IllegalArgumentException("Unexpected command argument: " + arg);
        }

        taskArg = new VisorReencryptionRateTaskArg(rateLimit);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "View/change re-encryption rate limit:", ENCRYPTION,
            singletonMap("limit", "Decimal value to change re-encryption rate limit (MB/s)."),
            REENCRYPTION_RATE.toString(), optional(ReencryptionRateCommandArg.LIMIT, "limit"));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return REENCRYPTION_RATE.text().toUpperCase();
    }

    /**
     * Re-encryption rate command arguments name.
     */
    private enum ReencryptionRateCommandArg implements CommandArg {
        /** Re-encryption rate limit argument. */
        LIMIT("--limit");

        /** Argument name. */
        private final String name;

        /**
         * @param name Argument name.
         */
        ReencryptionRateCommandArg(String name) {
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
