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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.configuration.VisorTracingConfigurationTask;
import org.apache.ignite.internal.commandline.configuration.VisorTracingConfigurationTaskArg;
import org.apache.ignite.internal.commandline.configuration.VisorTracingConfigurationTaskResult;
import org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationArguments;
import org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationCommandArg;
import org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand;
import org.apache.ignite.spi.tracing.Scope;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.TRACING_CONFIGURATION;
import static org.apache.ignite.internal.commandline.CommandLogger.grouped;
import static org.apache.ignite.internal.commandline.CommandLogger.join;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.GET;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.GET_ALL;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.RESET;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.RESET_ALL;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.SET;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.of;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_NEVER;

/**
 * Commands associated with tracing configuration functionality.
 */
public class TracingConfigurationCommand implements Command<TracingConfigurationArguments> {
    /** Arguments. */
    private TracingConfigurationArguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        if (!experimentalEnabled())
            return;

        Command.usage(
            log,
            "Print tracing configuration: ",
            TRACING_CONFIGURATION);

        Command.usage(
            log,
            "Print tracing configuration: ",
            TRACING_CONFIGURATION,
            GET_ALL.text(),
            optional(TracingConfigurationCommandArg.SCOPE.argName(), join("|", Scope.values())));

        Command.usage(
            log,
            "Print specific tracing configuration based on specified " +
                TracingConfigurationCommandArg.SCOPE.argName() + " and " +
                TracingConfigurationCommandArg.LABEL.argName() + ": ",
            TRACING_CONFIGURATION,
            GET.text(),
            grouped(TracingConfigurationCommandArg.SCOPE.argName(), join("|", Scope.values())),
            CommandLogger.optional(TracingConfigurationCommandArg.LABEL.argName()));

        Command.usage(
            log,
            "Reset all specific tracing configuration the to default. If " +
                TracingConfigurationCommandArg.SCOPE.argName() +
                " is specified, then remove all label specific configuration for the given scope and reset given scope" +
                " specific configuration to the default, if " + TracingConfigurationCommandArg.SCOPE.argName() +
                " is skipped then reset all tracing configurations to the default. Print tracing configuration.",
            TRACING_CONFIGURATION,
            RESET_ALL.text(),
            optional(TracingConfigurationCommandArg.SCOPE.argName(), join("|", Scope.values())));

        Command.usage(
            log,
            "Reset specific tracing configuration to the default. If both " +
                TracingConfigurationCommandArg.SCOPE.argName() + " and " +
                TracingConfigurationCommandArg.LABEL.argName() + " are specified then remove given configuration," +
                " if only " + TracingConfigurationCommandArg.SCOPE.argName() +
                " is specified then reset given configuration to the default." +
                " Print reseted configuration.",
            TRACING_CONFIGURATION,
            RESET.text(),
            grouped(TracingConfigurationCommandArg.SCOPE.argName(), join("|", Scope.values())),
            CommandLogger.optional(TracingConfigurationCommandArg.LABEL.argName()));

        Command.usage(
            log,
            "Set new tracing configuration. If both " +
                TracingConfigurationCommandArg.SCOPE.argName() + " and " +
                TracingConfigurationCommandArg.LABEL.argName() + " are specified then add or override label" +
                " specific configuration, if only " + TracingConfigurationCommandArg.SCOPE.argName() +
                " is specified, then override scope specific configuration. Print applied configuration.",
            TRACING_CONFIGURATION,
            SET.text(),
            grouped(TracingConfigurationCommandArg.SCOPE.argName(), join("|", Scope.values()),
            CommandLogger.optional(TracingConfigurationCommandArg.LABEL.argName()),
            optional(TracingConfigurationCommandArg.SAMPLING_RATE.argName(),
                "Decimal value between 0 and 1, " +
                "where 0 means never and 1 means always. " +
                "More or less reflects the probability of sampling specific trace."),
            optional(TracingConfigurationCommandArg.INCLUDED_SCOPES.argName(),
                "Set of scopes with comma as separator ",
                join("|", Scope.values()))));
    }

    /**
     * Execute tracing-configuration command.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to execute tracing-configuration action.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        if (experimentalEnabled()) {
            try (GridClient client = Command.startClient(clientCfg)) {
                UUID crdId = client.compute()
                    //Only non client node can be coordinator.
                    .nodes(node -> !node.isClient())
                    .stream()
                    .min(Comparator.comparingLong(GridClientNode::order))
                    .map(GridClientNode::nodeId)
                    .orElse(null);

                VisorTracingConfigurationTaskResult res = executeTaskByNameOnNode(
                    client,
                    VisorTracingConfigurationTask.class.getName(),
                    toVisorArguments(args),
                    crdId,
                    clientCfg
                );

                printResult(res, log::info);

                return res;
            }
            catch (Throwable e) {
                log.severe("Failed to execute tracing-configuration command='" + args.command().text() + "'");

                throw e;
            }
        } else {
            log.warning(String.format("For use experimental command add %s=true to JVM_OPTS in %s",
                IGNITE_ENABLE_EXPERIMENTAL_COMMAND, UTILITY_NAME));

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        // If there is no arguments, use list command.
        if (!argIter.hasNextSubArg()) {
            args = new TracingConfigurationArguments.Builder(GET_ALL).build();

            return;
        }

        TracingConfigurationSubcommand cmd = of(argIter.nextArg("Expected tracing configuration action."));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct tracing configuration action.");

        TracingConfigurationArguments.Builder tracingConfigurationArgs = new TracingConfigurationArguments.Builder(cmd);

        Scope scope = null;

        String lb = null;

        double samplingRate = SAMPLING_RATE_NEVER;

        Set<Scope> includedScopes = new HashSet<>();

        while (argIter.hasNextSubArg()) {
            TracingConfigurationCommandArg arg =
                CommandArgUtils.of(argIter.nextArg(""), TracingConfigurationCommandArg.class);

            String strVal;

            assert arg != null;

            switch (arg) {
                case SCOPE: {
                    String peekedNextArg = argIter.peekNextArg();

                    if (!TracingConfigurationCommandArg.args().contains(peekedNextArg)) {
                        strVal = argIter.nextArg(
                            "The scope should be specified. The following " +
                                "values can be used: " + Arrays.toString(Scope.values()) + '.');

                        try {
                            scope = Scope.valueOf(strVal);
                        }
                        catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                "Invalid scope '" + strVal + "'. The following " +
                                    "values can be used: " + Arrays.toString(Scope.values()) + '.');
                        }
                    }

                    break;
                }
                case LABEL: {
                    lb = argIter.nextArg(
                        "The label should be specified.");

                    break;
                }
                case SAMPLING_RATE: {
                    strVal = argIter.nextArg(
                        "The sampling rate should be specified. Decimal value between 0 and 1 should be used.");

                    try {
                        samplingRate = Double.parseDouble(strVal);
                    }
                    catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                            "Invalid sampling-rate '" + strVal + "'. Decimal value between 0 and 1 should be used.");
                    }

                    if (samplingRate < SAMPLING_RATE_NEVER || samplingRate > SAMPLING_RATE_ALWAYS)
                        throw new IllegalArgumentException(
                            "Invalid sampling-rate '" + strVal + "'. Decimal value between 0 and 1 should be used.");

                    break;
                }
                case INCLUDED_SCOPES: {
                    Set<String> setStrVals = argIter.nextStringSet(
                        "At least one supported scope should be specified.");

                    for (String scopeStrVal : setStrVals) {
                        try {
                            includedScopes.add(Scope.valueOf(scopeStrVal));
                        }
                        catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                "Invalid supported scope '" + scopeStrVal + "'. The following " +
                                    "values can be used: " + Arrays.toString(Scope.values()) + '.');
                        }
                    }

                    break;
                }
            }
        }

        // Scope is a mandatory attribute for all sub-commands except get_all and reset_all.
        if ((cmd != GET_ALL && cmd != RESET_ALL) && scope == null) {
            throw new IllegalArgumentException(
                "Scope attribute is missing. Following values can be used: " + Arrays.toString(Scope.values()) + '.');
        }

        switch (cmd) {
            case GET_ALL:
            case RESET_ALL: {
                tracingConfigurationArgs.withScope(scope);

                break;
            }

            case RESET:
            case GET: {
                tracingConfigurationArgs.withScope(scope).withLabel(lb);

                break;
            }

            case SET: {
                tracingConfigurationArgs.withScope(scope).withLabel(lb).withSamplingRate(samplingRate).
                    withIncludedScopes(includedScopes);

                break;
            }

            default: {
                // We should never get here.
                assert false : "Unexpected tracing configuration argument [arg= " + cmd + ']';
            }
        }

        args = tracingConfigurationArgs.build();
    }

    /** {@inheritDoc} */
    @Override public TracingConfigurationArguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return TRACING_CONFIGURATION.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public boolean experimental() {
        return true;
    }

    /**
     * Print result.
     *
     * @param res Visor tracing configuration result.
     * @param printer Printer.
     */
    private void printResult(VisorTracingConfigurationTaskResult res, Consumer<String> printer) {
        res.print(printer);
    }

    /**
     * Prepare task argument.
     *
     * @param args Argument from command line.
     * @return Task argument.
     */
    private VisorTracingConfigurationTaskArg toVisorArguments(TracingConfigurationArguments args) {
        return new VisorTracingConfigurationTaskArg(
            args.command().visorBaselineOperation(),
            args.scope(),
            args.label(),
            args.samplingRate(),
            args.includedScopes()
        );
    }
}
