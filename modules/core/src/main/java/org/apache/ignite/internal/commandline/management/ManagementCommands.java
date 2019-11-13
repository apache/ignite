/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.ignite.internal.commandline.management;

import java.io.IOException;
import java.util.Comparator;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.management.ChangeManagementConfigurationTask;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.MANAGEMENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.management.ManagementCommandList.HELP;
import static org.apache.ignite.internal.commandline.management.ManagementCommandList.of;

/**
 * Management cluster command.
 */
public class ManagementCommands implements Command<ManagementArguments> {
    /** */
    private ManagementArguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        if (!experimentalEnabled())
            return;

        Command.usage(log, "Enable management:", MANAGEMENT, ManagementCommandList.ENABLE.text());
        Command.usage(log, "Disable management:", MANAGEMENT, ManagementCommandList.DISABLE.text());
        Command.usage(log, "Change management URI:", MANAGEMENT, getUrlOptions());
        Command.usage(log, "Get management status:", MANAGEMENT, ManagementCommandList.STATUS.text());
    }

    /** */
    private void printHelp(Logger log) {
        log.info("The '" + MANAGEMENT + " subcommand' is used to control management agent. The command has the following syntax:");
        log.info("");
        log.info(CommandLogger.join(" ", UTILITY_NAME, CommandLogger.join(" ", getCommonOptions())) + " " +
            MANAGEMENT + " <subcommand> [subcommand_arguments] ");
        log.info("");
        log.info("The commands will be executed on the coordinator node");
        log.info("");
    }

    /**
     * Management cluster.
     *
     * @param clientCfg Client configuration.
     * @throws GridClientException If failed to activate.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        if (experimentalEnabled()) {
            try (GridClient client = Command.startClient(clientCfg)) {
                UUID crdId = client.compute().nodes().stream()
                    .min(Comparator.comparingLong(GridClientNode::order))
                    .map(GridClientNode::nodeId)
                    .orElse(null);

                if (args.command() == HELP) {
                    printHelp(log);

                    return null;
                }

                ManagementConfiguration cfg = executeTaskByNameOnNode(
                    client,
                    ChangeManagementConfigurationTask.class.getName(),
                    toVisorArguments(args),
                    crdId,
                    clientCfg
                );

                print(log, cfg);
            }
            catch (Throwable e) {
                log.severe("Failed to execute management command='" + args.command().text() + "'");
                log.severe(CommandLogger.errorMessage(e));

                throw e;
            }
        }
        else {
            log.warning(String.format("For use experimental command add %s=true to JVM_OPTS in %s",
                IGNITE_ENABLE_EXPERIMENTAL_COMMAND, UTILITY_NAME));
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public ManagementArguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        ManagementCommandList cmd = of(argIter.nextArg("Arguments are expected for --management subcommand, " +
            "run '--management help' for more info."));

        if (cmd == null) {
            throw new IllegalArgumentException("Expected correct action for --management subcommand, " +
                "run '--management help' for more info.");
        }

        ManagementArguments.Builder managementArgs = new ManagementArguments.Builder(cmd);

        switch (cmd) {
            case ENABLE:
                managementArgs.setEnable(true);

                break;
            case DISABLE:
                managementArgs.setEnable(false);

                break;
            case URI:
                managementArgs.setEnable(true)
                    .setServerUris(argIter.nextStringSet("server URIs"));

                while (argIter.hasNextSubArg()) {
                    ManagementURLCommandArg uriArg = CommandArgUtils.of(
                        argIter.nextArg("Expected one of uri arguments"), ManagementURLCommandArg.class
                    );

                    if (uriArg == null)
                        throw new IllegalArgumentException("Expected one of auto-adjust arguments");

                    switch (uriArg) {
                        case KEYSTORE:
                            managementArgs.setKeyStore(readFileToString(argIter.nextArg("key store path")));

                            break;

                        case KEYSTORE_PASSWORD:
                            managementArgs.setKeyStorePassword(argIter.nextArg("key store password"));

                            break;

                        case TRUSTSTORE:
                            managementArgs.setTrustStorePassword(readFileToString(argIter.nextArg("trust store path")));

                            break;

                        case TRUSTSTORE_PASSWORD:
                            managementArgs.setTrustStorePassword(argIter.nextArg("trust store password"));

                            break;

                        case CIPHER_SUITES:
                            managementArgs.setCipherSuites(argIter.nextStringSet("cipher suites"));

                            break;

                        case SESSION_TIMEOUT:
                            managementArgs.setSessionTimeout(argIter.nextLongArg("session timeout"));

                            break;

                        case SESSION_EXPIRATION_TIMEOUT:
                            managementArgs.setSessionExpirationTimeout(argIter.nextLongArg("session expiration timeout"));

                            break;

                        default:
                            // No-op.
                    }
                }
        }

        this.args = managementArgs.build();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return MANAGEMENT.toCommandName();
    }

    /**
     * @param path Path.
     * @return File content.
     */
    private String readFileToString(String path) {
        try {
            return U.readFileToString(path, "UTF-8");
        }
        catch (IOException e) {
            throw new IgniteException("Failed to load file content: " + path, e);
        }
    }

    /**
     * Prepare task argument.
     *
     * @param args Argument from command line.
     * @return Task argument.
     */
    private ManagementConfiguration toVisorArguments(ManagementArguments args) {
        if (args.command() == ManagementCommandList.STATUS)
            return null;

        return new ManagementConfiguration()
            .setEnabled(args.isEnable())
            .setConsoleUris(args.getServerUris())
            .setCipherSuites(args.getCipherSuites())
            .setConsoleKeyStore(args.getKeyStore())
            .setConsoleKeyStorePassword(args.getKeyStorePassword())
            .setConsoleTrustStore(args.getTrustStore())
            .setConsoleTrustStorePassword(args.getTrustStorePassword())
            .setSecuritySessionTimeout(args.getSessionTimeout())
            .setSecuritySessionExpirationTimeout(args.getSessionExpirationTimeout());
    }

    /**
     * @return Transaction command options.
     */
    private String[] getUrlOptions() {
        return Stream.of(
            ManagementCommandList.URI.text(),
            "MANAGEMENT_URIS",
            optional("--management-cipher-suites", "MANAGEMENT_CIPHER_1[, MANAGEMENT_CIPHER_2, ..., MANAGEMENT_CIPHER_N]"),
            optional("--management-keystore", "MANAGEMENT_KEYSTORE_PATH"),
            optional("--management-keystore-password", "MANAGEMENT_KEYSTORE_PASSWORD"),
            optional("--management-truststore", "MANAGEMENT_TRUSTSTORE_PATH"),
            optional("--management-truststore-password", "MANAGEMENT_TRUSTSTORE_PASSWORD"),
            optional("--management-session-timeout", "MANAGEMENT_SESSION_TIMEOUT"),
            optional("--management-session-expiration-timeout", "MANAGEMENT_SESSION_EXPIRATION_TIMEOUT")
        ).toArray(String[]::new);
    }

    /**
     * @param flag Flag.
     */
    private String flag(boolean flag) {
        return flag ? "enabled" : "disabled";
    }

    /**
     * Print configuration.
     *
     * @param log Logger.
     * @param cfg Management config.
     */
    private void print(Logger log, ManagementConfiguration cfg) {
        log.info("");
        log.info("Management: " + flag(cfg.isEnabled()));
        log.info("URIs to management: " + cfg.getConsoleUris());

        if (!F.isEmpty(cfg.getCipherSuites()))
            log.info("Cipher suites: " + cfg.getCipherSuites());

        log.info("Management key store: " + flag(!F.isEmpty(cfg.getConsoleKeyStore())));
        log.info("Management trust store: " + flag(!F.isEmpty(cfg.getConsoleTrustStore())));
        log.info("Management session timeout: " + cfg.getSecuritySessionTimeout());
        log.info("Management session expiration timeout: " + cfg.getSecuritySessionExpirationTimeout());
        log.info("");
    }
}
