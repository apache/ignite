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

import java.util.Comparator;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientBeforeNodeStart;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

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
    /** */
    public String EXPERIMENTAL_LABEL = "[EXPERIMENTAL]";

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
        if (!client.connected()) {
            GridClientException lastErr = client.checkLastError();

            try {
                client.close();
            }
            catch (Throwable e) {
                lastErr.addSuppressed(e);
            }

            throw lastErr;
        }

        return client;
    }

    /**
     * Method to create thin client for communication with node before it starts.
     * If node has already started, there will be an error.
     *
     * @param clientCfg Thin client configuration.
     * @return Grid thin client instance which is already connected to node before it starts.
     * @throws Exception If error occur.
     */
    public static GridClientBeforeNodeStart startClientBeforeNodeStart(
        GridClientConfiguration clientCfg
    ) throws Exception {
        GridClientBeforeNodeStart client = GridClientFactory.startBeforeNodeStart(clientCfg);

        // If connection is unsuccessful, fail before doing any operations:
        if (!client.connected()) {
            GridClientException lastErr = client.checkLastError();

            try {
                client.close();
            }
            catch (Throwable e) {
                lastErr.addSuppressed(e);
            }

            throw lastErr;
        }

        return client;
    }

    /**
     * Print command usage.
     *
     * @param logger Logger to use.
     * @param desc Command description.
     * @param cmd Command.
     * @param args Arguments.
     */
    public default void usage(IgniteLogger logger, String desc, CommandList cmd, String... args) {
        usage(logger, desc, cmd, null, args);
    }

    /**
     * Print command usage.
     *
     * @param logger Logger to use.
     * @param desc Command description.
     * @param cmd Command.
     * @param paramsDesc Description of parameters (optional).
     * @param args Arguments.
     */
    public default void usage(
        IgniteLogger logger,
        String desc,
        CommandList cmd,
        @Nullable Map<String, String> paramsDesc,
        String... args
    ) {
        usage(logger, desc, cmd.text(), paramsDesc, args);
    }

    /**
     * Print command usage.
     *
     * @param logger Logger to use.
     * @param desc Command description.
     * @param cmd Command.
     * @param paramsDesc Description of parameters (optional).
     * @param args Arguments.
     */
    public default void usage(
        IgniteLogger logger,
        String desc,
        String cmd,
        @Nullable Map<String, String> paramsDesc,
        String... args
    ) {
        logger.info("");

        if (experimental())
            logger.info(INDENT + EXPERIMENTAL_LABEL);

        logger.info(INDENT + desc);
        logger.info(DOUBLE_INDENT + CommandLogger.join(" ", UTILITY_NAME, cmd, CommandLogger.join(" ", args)));

        if (!F.isEmpty(paramsDesc)) {
            logger.info("");

            logger.info(DOUBLE_INDENT + "Parameters:");

            usageParams(paramsDesc, DOUBLE_INDENT + INDENT, logger);
        }
    }

    /**
     * Print cache command arguments usage.
     *
     * @param paramsDesc Cache command arguments description.
     * @param indent Indent string.
     * @param logger Logger to use.
     */
    public default void usageParams(Map<String, String> paramsDesc, String indent, IgniteLogger logger) {
        int maxParamLen = paramsDesc.keySet().stream().max(Comparator.comparingInt(String::length)).get().length();

        for (Map.Entry<String, String> param : paramsDesc.entrySet())
            logger.info(indent + U.extendToLen(param.getKey(), maxParamLen) + "  " + "- " + param.getValue());
    }

    /**
     * Actual command execution. Parameters for run should be already set by calling parseArguments method.
     *
     * @param clientCfg Thin client configuration if connection to cluster is necessary.
     * @param logger Logger to use.
     * @return Result of operation (mostly usable for tests).
     * @throws Exception If error occur.
     */
    public Object execute(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception;

    /**
     * Actual command execution with verbose mode if needed.
     * Implement it if your command supports verbose mode.
     *
     * @see Command#execute(GridClientConfiguration, IgniteLogger)
     *
     * @param clientCfg Thin client configuration if connection to cluster is necessary.
     * @param logger Logger to use.
     * @param verbose Use verbose mode or not
     * @return Result of operation (mostly usable for tests).
     * @throws Exception If error occur.
     */
    default Object execute(GridClientConfiguration clientCfg, IgniteLogger logger, boolean verbose) throws Exception {
        return execute(clientCfg, logger);
    }

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
    public void printUsage(IgniteLogger logger);

    /**
     * @return command name.
     */
    String name();

    /**
     * Return {@code true} if the command is experimental or {@code false}
     * otherwise.
     *
     * @return {@code true} if the command is experimental or {@code false}
     *      otherwise.
     */
    default boolean experimental() {
        return false;
    }
}
