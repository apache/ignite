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
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionRateTask;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.REENCRYPTION_RATE;

/**
 * View/change cache group re-encryption rate limit subcommand.
 */
public class ReencryptionRateCommand implements Command<Double> {
    /** Re-encryption rate limit in megabytes per second.  */
    private Double rateLimit;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Map<UUID, Object> results = executeTaskByNameOnNode(
                client,
                VisorReencryptionRateTask.class.getName(),
                rateLimit,
                BROADCAST_UUID,
                clientCfg
            );

            for (Map.Entry<UUID, Object> entry : results.entrySet()) {
                boolean read = rateLimit == null;

                String msg;

                if (entry.getValue() instanceof Throwable) {
                    msg = " failed to " + (read ? "get" : "limit") + " reencryption rate (" +
                        ((Throwable)entry.getValue()).getMessage() + ").";
                }
                else {
                    double prevRate = (double)entry.getValue();
                    boolean unlimited = read ? prevRate == 0 : rateLimit == 0;

                    if (unlimited)
                        msg = "reencryption rate is not limited.";
                    else {
                        msg = "reencryption rate " + (read ?
                            "is limited to " + prevRate :
                            "has been limited to " + rateLimit) + " MB/s.";
                    }
                }

                log.info(INDENT + "Node " + entry.getKey() + ": " + msg);
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
    @Override public Double arg() {
        return rateLimit;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (argIter.hasNextArg() && !CommandArgIterator.isCommandOrOption(argIter.peekNextArg()))
            rateLimit = Double.parseDouble(argIter.nextArg("Expected decimal value for reencryption rate."));
        else
            rateLimit = null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "View/change re-encryption rate limit:", ENCRYPTION,
            singletonMap("limit", "decimal value to change rate limit (MB/s)"),
            REENCRYPTION_RATE.toString(), "[limit]");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return REENCRYPTION_RATE.name();
    }
}
