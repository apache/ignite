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

package org.apache.ignite.internal.commandline;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;

import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;

/**
 * Abstract class for all control.sh commands, has already implemented methods and abstract methods.
 * Define flow how to work with command.
 *
 * @param <T> Generic for getArg method which should return command-specific paramters which it would be run with.
 */
public interface Command<T> {
    /**
     * Method to create thin client for communication with cluster.
     *
     * @param clientCfg Thin client configuration.
     * @return Grid thin client instance which is already connected to cluster.
     * @throws Exception If error occur.
     */
    public static GridClient startClient(GridClientConfiguration clientCfg) throws Exception {
        GridClient client = GridClientFactory.start(clientCfg);

        // If connection is unsuccessful, fail before doing any operations:
        if (!client.connected())
            client.throwLastError();

        return client;
    }

    /**
     * Print command usage.
     *
     * @param logger Logger to use.
     * @param desc Command description.
     * @param args Arguments.
     */
    public static void usage(Logger logger, String desc, CommandList cmd, String... args) {
        logger.info(INDENT + desc);
        logger.info(DOUBLE_INDENT + CommandLogger.join(" ", UTILITY_NAME, cmd, CommandLogger.join(" ", args)));
        logger.info("");
    }

    /**
     * Actual command execution. Parameters for run should be already set by calling parseArguments method.
     *
     * @param clientCfg Thin client configuration if connection to cluster is necessary.
     * @param logger Logger to use.
     * @return Result of operation (mostly usable for tests).
     * @throws Exception If error occur.
     */
    public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception;

    /**
     * Prepares confirmation for the command.
     *
     * @param clientCfg Thin client configuration.
     * @throws Exception If error occur.
     */
    default void prepareConfirmation(GridClientConfiguration clientCfg) throws Exception{
        //no-op
    }

    /**
     * @return Message text to show user for. If null it means that confirmantion is not needed.
     */
    public default String confirmationPrompt() {
        return null;
    }

    /**
     * Parse command-specific arguments.
     *
     * @param argIterator Argument iterator.
     */
    public default void parseArguments(CommandArgIterator argIterator) {
        //Empty block.
    }

    /**
     * @return Command arguments which were parsed during {@link #parseArguments(CommandArgIterator)} call.
     */
    public T arg();

    /**
     * Print info for user about command (parameters, use cases and so on).
     *
     * @param logger Logger to use.
     */
    public void printUsage(Logger logger);

    /**
     * @return command name.
     */
    String name();
}
