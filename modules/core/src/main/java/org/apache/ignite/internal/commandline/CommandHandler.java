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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.client.GridClientAuthenticationException;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientHandshakeException;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionResetException;
import org.apache.ignite.internal.client.ssl.GridSslBasicContextFactory;
import org.apache.ignite.internal.commandline.baseline.BaselineCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityCredentialsProvider;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.commandline.CommandLogger.j;
import static org.apache.ignite.internal.commandline.CommandLogger.op;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.Commands.ACTIVATE;
import static org.apache.ignite.internal.commandline.Commands.BASELINE;
import static org.apache.ignite.internal.commandline.Commands.CACHE;
import static org.apache.ignite.internal.commandline.Commands.DEACTIVATE;
import static org.apache.ignite.internal.commandline.Commands.STATE;
import static org.apache.ignite.internal.commandline.Commands.TX;
import static org.apache.ignite.internal.commandline.Commands.WAL;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_DELETE;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_PRINT;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.HELP;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

/**
 * Class that execute several commands passed via command line.
 */
public class CommandHandler {
    /** */
    static final String CMD_HELP = "--help";

    /** */
    static final String CMD_HOST = "--host";

    /** */
    static final String CMD_PORT = "--port";

    /** */
    static final String CMD_PASSWORD = "--password";

    /** */
    static final String CMD_USER = "--user";

    /** Option is used for auto confirmation. */
    static final String CMD_AUTO_CONFIRMATION = "--yes";

    /** */
    static final String CMD_PING_INTERVAL = "--ping-interval";

    /** */
    static final String CMD_PING_TIMEOUT = "--ping-timeout";

    /** One cache filter option should used message. */
    public static final String ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG = "Should use only one of option: " +
        EXCLUDE_CACHES + ", " + CACHE_FILTER + " or pass caches explicitly";

    // SSL configuration section

    /** */
    static final String CMD_SSL_PROTOCOL = "--ssl-protocol";

    /** */
    static final String CMD_SSL_KEY_ALGORITHM = "--ssl-key-algorithm";

    /** */
    static final String CMD_SSL_CIPHER_SUITES = "--ssl-cipher-suites";

    /** */
    static final String CMD_KEYSTORE = "--keystore";

    /** */
    static final String CMD_KEYSTORE_PASSWORD = "--keystore-password";

    /** */
    static final String CMD_KEYSTORE_TYPE = "--keystore-type";

    /** */
    static final String CMD_TRUSTSTORE = "--truststore";

    /** */
    static final String CMD_TRUSTSTORE_PASSWORD = "--truststore-password";

    /** */
    static final String CMD_TRUSTSTORE_TYPE = "--truststore-type";

    /** List of optional auxiliary commands. */
    private static final Set<String> AUX_COMMANDS = new HashSet<>();

    private final CommandLogger logger = new CommandLogger();

    static {
        AUX_COMMANDS.add(CMD_HELP);

        AUX_COMMANDS.add(CMD_HOST);
        AUX_COMMANDS.add(CMD_PORT);

        AUX_COMMANDS.add(CMD_PASSWORD);
        AUX_COMMANDS.add(CMD_USER);

        AUX_COMMANDS.add(CMD_AUTO_CONFIRMATION);

        AUX_COMMANDS.add(CMD_PING_INTERVAL);
        AUX_COMMANDS.add(CMD_PING_TIMEOUT);

        AUX_COMMANDS.add(CMD_SSL_PROTOCOL);
        AUX_COMMANDS.add(CMD_SSL_KEY_ALGORITHM);
        AUX_COMMANDS.add(CMD_SSL_CIPHER_SUITES);

        AUX_COMMANDS.add(CMD_KEYSTORE);
        AUX_COMMANDS.add(CMD_KEYSTORE_PASSWORD);
        AUX_COMMANDS.add(CMD_KEYSTORE_TYPE);

        AUX_COMMANDS.add(CMD_TRUSTSTORE);
        AUX_COMMANDS.add(CMD_TRUSTSTORE_PASSWORD);
        AUX_COMMANDS.add(CMD_TRUSTSTORE_TYPE);
    }

    /** */
    public static final String CONFIRM_MSG = "y";

    /** */
    static final String DELIM = "--------------------------------------------------------------------------------";

    /** */
    public static final int EXIT_CODE_OK = 0;

    /** */
    public static final int EXIT_CODE_INVALID_ARGUMENTS = 1;

    /** */
    public static final int EXIT_CODE_CONNECTION_FAILED = 2;

    /** */
    public static final int ERR_AUTHENTICATION_FAILED = 3;

    /** */
    public static final int EXIT_CODE_UNEXPECTED_ERROR = 4;

    /** */
    private static final long DFLT_PING_INTERVAL = 5000L;

    /** */
    private static final long DFLT_PING_TIMEOUT = 30_000L;

    /** */
    private static final Scanner IN = new Scanner(System.in);

    /** Utility name. */
    private static final String UTILITY_NAME = "control.sh";

    /** Common options. */
    private static final String COMMON_OPTIONS = j(" ", getCommonOptions());

    /** Utility name with common options. */
    public static final String UTILITY_NAME_WITH_COMMON_OPTIONS = j(" ", UTILITY_NAME, COMMON_OPTIONS);

    /** */
    public static final String NULL = "null";

    /** */
    private Object lastOperationRes;

    /** Check if experimental commands are enabled. Default {@code false}. */
    private final boolean enableExperimental = IgniteSystemProperties.getBoolean(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, false);

    /** Console instance. Public access needs for tests. */
    public GridConsole console = GridConsoleAdapter.getInstance();

    /**
     * @param args Arguments to parse and apply.
     */
    public static void main(String[] args) {
        CommandHandler hnd = new CommandHandler();

        System.exit(hnd.execute(Arrays.asList(args)));
    }

    /**
     * Parse and execute command.
     *
     * @param rawArgs Arguments to parse and execute.
     * @return Exit code.
     */
    public int execute(List<String> rawArgs) {
        logger.log("Control utility [ver. " + ACK_VER_STR + "]");
        logger.log(COPYRIGHT);
        logger.log("User: " + System.getProperty("user.name"));
        logger.log("Time: " + LocalDateTime.now());
        logger.log(DELIM);

        try {
            if (F.isEmpty(rawArgs) || (rawArgs.size() == 1 && CMD_HELP.equalsIgnoreCase(rawArgs.get(0)))) {
                printHelp();

                return EXIT_CODE_OK;
            }

            String arg = rawArgs.get(0).trim();

            Commands command = Commands.of(arg);

            command.command().init(new CommandArgIterator(rawArgs.iterator(), AUX_COMMANDS));

            ConnectionAndSslParameters args = new CommandArgParser(logger).
                parseAndValidate(new CommandArgIterator(rawArgs.iterator(), AUX_COMMANDS));


            if (!args.autoConfirmation() && !confirm(command.command().confirmationPrompt())) {
                logger.log("Operation cancelled.");

                return EXIT_CODE_OK;
            }

            boolean tryConnectAgain = true;

            int tryConnectMaxCount = 3;

            while (tryConnectAgain) {
                tryConnectAgain = false;

                GridClientConfiguration clientCfg = getClientConfiguration(args);

                try {
                    lastOperationRes = command.command().execute(clientCfg, logger);
                }
                catch (Throwable e) {
                    if (tryConnectMaxCount > 0 && isAuthError(e)) {
                        logger.log("Authentication error, try connection again.");

                        if (F.isEmpty(args.getUserName()))
                            args.setUserName(requestDataFromConsole("user: "));

                        args.setPassword(new String(requestPasswordFromConsole("password: ")));

                        tryConnectAgain = true;

                        tryConnectMaxCount--;
                    }
                    else {
                        if (tryConnectMaxCount == 0)
                            throw new GridClientAuthenticationException("Authentication error, maximum number of " +
                                "retries exceeded");

                        throw e;
                    }
                }
            }

            return EXIT_CODE_OK;
        }
        catch (IllegalArgumentException e) {
            logger.error("Check arguments.", e);

            return EXIT_CODE_INVALID_ARGUMENTS;
        }
        catch (Throwable e) {
            if (isAuthError(e)) {
                logger.error("Authentication error.", e);

                return ERR_AUTHENTICATION_FAILED;
            }

            if (isConnectionError(e)) {
                logger.error("Connection to cluster failed.", e);

                return EXIT_CODE_CONNECTION_FAILED;
            }

            logger.error("", e);

            return EXIT_CODE_UNEXPECTED_ERROR;
        }
    }

    @NotNull private GridClientConfiguration getClientConfiguration(ConnectionAndSslParameters args) throws IgniteCheckedException {
        GridClientConfiguration clientCfg = new GridClientConfiguration();

        clientCfg.setPingInterval(args.pingInterval());

        clientCfg.setPingTimeout(args.pingTimeout());

        clientCfg.setServers(Collections.singletonList(args.host() + ":" + args.port()));

        if (!F.isEmpty(args.getUserName()))
            clientCfg.setSecurityCredentialsProvider(getSecurityCredentialsProvider(args, clientCfg));

        if (!F.isEmpty(args.sslKeyStorePath()))
            clientCfg.setSslContextFactory(createSslSupportFactory(args));

        return clientCfg;
    }

    @NotNull private SecurityCredentialsProvider getSecurityCredentialsProvider(
        ConnectionAndSslParameters args,
        GridClientConfiguration clientCfg
    ) throws IgniteCheckedException {
        SecurityCredentialsProvider securityCredential = clientCfg.getSecurityCredentialsProvider();

        if (securityCredential == null)
            securityCredential = new SecurityCredentialsBasicProvider(
                new SecurityCredentials(args.getUserName(), args.getPassword()));

        final SecurityCredentials credential = securityCredential.credentials();
        credential.setLogin(args.getUserName());
        credential.setPassword(args.getPassword());

        return securityCredential;
    }

    @NotNull private GridSslBasicContextFactory createSslSupportFactory(ConnectionAndSslParameters args) {
        GridSslBasicContextFactory factory = new GridSslBasicContextFactory();

        List<String> sslProtocols = split(args.sslProtocol(), ",");

        String sslProtocol = F.isEmpty(sslProtocols) ? DFLT_SSL_PROTOCOL : sslProtocols.get(0);

        factory.setProtocol(sslProtocol);
        factory.setKeyAlgorithm(args.sslKeyAlgorithm());

        if (sslProtocols.size() > 1)
            factory.setProtocols(sslProtocols);

        factory.setCipherSuites(split(args.getSslCipherSuites(), ","));

        factory.setKeyStoreFilePath(args.sslKeyStorePath());

        if (args.sslKeyStorePassword() != null)
            factory.setKeyStorePassword(args.sslKeyStorePassword());
        else
            factory.setKeyStorePassword(requestPasswordFromConsole("SSL keystore password: "));

        factory.setKeyStoreType(args.sslKeyStoreType());

        if (F.isEmpty(args.sslTrustStorePath()))
            factory.setTrustManagers(GridSslBasicContextFactory.getDisabledTrustManager());
        else {
            factory.setTrustStoreFilePath(args.sslTrustStorePath());

            if (args.sslTrustStorePassword() != null)
                factory.setTrustStorePassword(args.sslTrustStorePassword());
            else
                factory.setTrustStorePassword(requestPasswordFromConsole("SSL truststore password: "));

            factory.setTrustStoreType(args.sslTrustStoreType());
        }

        return factory;
    }

    /**
     * Used for tests.
     *
     * @return Last operation result;
     */
    public <T> T getLastOperationResult() {
        return (T)lastOperationRes;
    }

    /**
     * Provides a prompt, then reads a single line of text from the console.
     *
     * @param prompt text
     * @return A string containing the line read from the console
     */
    private String readLine(String prompt) {
        System.out.print(prompt);

        return IN.nextLine();
    }


    /**
     * Requests interactive user confirmation if forthcoming operation is dangerous.
     *
     * @return {@code true} if operation confirmed (or not needed), {@code false} otherwise.
     */
    private <T> boolean confirm(String str) {
        if (str == null)
            return true;

        String prompt = str + "\nPress '" + CONFIRM_MSG + "' to continue . . . ";

        return CONFIRM_MSG.equalsIgnoreCase(readLine(prompt));
    }

    /**
     * @param e Exception to check.
     * @return {@code true} if specified exception is {@link GridClientAuthenticationException}.
     */
    private static boolean isAuthError(Throwable e) {
        return X.hasCause(e, GridClientAuthenticationException.class);
    }

    /**
     * @param e Exception to check.
     * @return {@code true} if specified exception is a connection error.
     */
    private static boolean isConnectionError(Throwable e) {
        return e instanceof GridClientClosedException ||
            e instanceof GridClientConnectionResetException ||
            e instanceof GridClientDisconnectedException ||
            e instanceof GridClientHandshakeException ||
            e instanceof GridServerUnreachableException;
    }

    /**
     * Requests password from console with message.
     *
     * @param msg Message.
     * @return Password.
     */
    private char[] requestPasswordFromConsole(String msg) {
        if (console == null)
            throw new UnsupportedOperationException("Failed to securely read password (console is unavailable): " + msg);
        else
            return console.readPassword(msg);
    }

    /**
     * Requests user data from console with message.
     *
     * @param msg Message.
     * @return Input user data.
     */
    private String requestDataFromConsole(String msg) {
        if (console != null)
            return console.readLine(msg);
        else {
            Scanner scanner = new Scanner(System.in);

            logger.log(msg);

            return scanner.nextLine();
        }
    }

    /**
     * Split string into items.
     *
     * @param s String to process.
     * @param delim Delimiter.
     * @return List with items.
     */
    private static List<String> split(String s, String delim) {
        if (F.isEmpty(s))
            return Collections.emptyList();

        return Arrays.stream(s.split(delim))
            .map(String::trim)
            .filter(item -> !item.isEmpty())
            .collect(Collectors.toList());
    }

    /** */
    private void printHelp() {
        final String constistIds = "consistentId1[,consistentId2,....,consistentIdN]";

        logger.log("Control.sh is used to execute admin commands on cluster or get common cluster info. The command has the following syntax:");
        logger.nl();

        logger.logWithIndent(j(" ", UTILITY_NAME_WITH_COMMON_OPTIONS, op("command"), "<command_parameters>"));
        logger.nl();
        logger.nl();

        logger.log("This utility can do the following commands:");

        usage("Activate cluster:", ACTIVATE);
        usage("Deactivate cluster:", DEACTIVATE, op(CMD_AUTO_CONFIRMATION));
        usage("Print current cluster state:", STATE);
        usage("Print cluster baseline topology:", BASELINE);
        usage("Add nodes into baseline topology:", BASELINE, BaselineCommand.ADD.text(), constistIds, op(CMD_AUTO_CONFIRMATION));
        usage("Remove nodes from baseline topology:", BASELINE, BaselineCommand.REMOVE.text(), constistIds, op(CMD_AUTO_CONFIRMATION));
        usage("Set baseline topology:", BASELINE, BaselineCommand.SET.text(), constistIds, op(CMD_AUTO_CONFIRMATION));
        usage("Set baseline topology based on version:", BASELINE, BaselineCommand.VERSION.text() + " topologyVersion", op(CMD_AUTO_CONFIRMATION));
        usage("Set baseline autoadjustment settings:", BASELINE, BaselineCommand.AUTO_ADJUST.text(), "disable|enable timeout <timeoutValue>", op(CMD_AUTO_CONFIRMATION));
        usage("List or kill transactions:", TX, getTxOptions());

        if (enableExperimental) {
            usage("Print absolute paths of unused archived wal segments on each node:", WAL, WAL_PRINT, "[consistentId1,consistentId2,....,consistentIdN]");
            usage("Delete unused archived wal segments on each node:", WAL, WAL_DELETE, "[consistentId1,consistentId2,....,consistentIdN]", op(CMD_AUTO_CONFIRMATION));
        }

        logger.logWithIndent("View caches information in a cluster. For more details type:");
        logger.logWithIndent(j(" ", UTILITY_NAME, CACHE, HELP), 2);
        logger.nl();

        logger.log("By default commands affecting the cluster require interactive confirmation.");
        logger.log("Use " + CMD_AUTO_CONFIRMATION + " option to disable it.");
        logger.nl();

        logger.log("Default values:");
        logger.logWithIndent("HOST_OR_IP=" + DFLT_HOST, 2);
        logger.logWithIndent("PORT=" + DFLT_PORT, 2);
        logger.logWithIndent("PING_INTERVAL=" + DFLT_PING_INTERVAL, 2);
        logger.logWithIndent("PING_TIMEOUT=" + DFLT_PING_TIMEOUT, 2);
        logger.logWithIndent("SSL_PROTOCOL=" + SslContextFactory.DFLT_SSL_PROTOCOL, 2);
        logger.logWithIndent("SSL_KEY_ALGORITHM=" + SslContextFactory.DFLT_KEY_ALGORITHM, 2);
        logger.logWithIndent("KEYSTORE_TYPE=" + SslContextFactory.DFLT_STORE_TYPE, 2);
        logger.logWithIndent("TRUSTSTORE_TYPE=" + SslContextFactory.DFLT_STORE_TYPE, 2);

        logger.nl();

        logger.log("Exit codes:");
        logger.logWithIndent(EXIT_CODE_OK + " - successful execution.", 2);
        logger.logWithIndent(EXIT_CODE_INVALID_ARGUMENTS + " - invalid arguments.", 2);
        logger.logWithIndent(EXIT_CODE_CONNECTION_FAILED + " - connection failed.", 2);
        logger.logWithIndent(ERR_AUTHENTICATION_FAILED + " - authentication failed.", 2);
        logger.logWithIndent(EXIT_CODE_UNEXPECTED_ERROR + " - unexpected error.", 2);
    }


    /**
     * Print command usage.
     *
     * @param desc Command description.
     * @param args Arguments.
     */
    private void usage(String desc, Commands cmd, String... args) {
        logger.logWithIndent(desc);
        logger.logWithIndent(j(" ", UTILITY_NAME, cmd, j(" ", args)), 2);
        logger.nl();
    }

    /**
     * Creates list of common utility options.
     *
     * @return Array of common utility options.
     */
    private static String[] getCommonOptions() {
        List<String> list = new ArrayList<>(32);

        list.add(op(CMD_HOST, "HOST_OR_IP"));
        list.add(op(CMD_PORT, "PORT"));
        list.add(op(CMD_USER, "USER"));
        list.add(op(CMD_PASSWORD, "PASSWORD"));
        list.add(op(CMD_PING_INTERVAL, "PING_INTERVAL"));
        list.add(op(CMD_PING_TIMEOUT, "PING_TIMEOUT"));

        list.add(op(CMD_SSL_PROTOCOL, "SSL_PROTOCOL[, SSL_PROTOCOL_2, ..., SSL_PROTOCOL_N]"));
        list.add(op(CMD_SSL_CIPHER_SUITES, "SSL_CIPHER_1[, SSL_CIPHER_2, ..., SSL_CIPHER_N]"));
        list.add(op(CMD_SSL_KEY_ALGORITHM, "SSL_KEY_ALGORITHM"));
        list.add(op(CMD_KEYSTORE_TYPE, "KEYSTORE_TYPE"));
        list.add(op(CMD_KEYSTORE, "KEYSTORE_PATH"));
        list.add(op(CMD_KEYSTORE_PASSWORD, "KEYSTORE_PASSWORD"));
        list.add(op(CMD_TRUSTSTORE_TYPE, "TRUSTSTORE_TYPE"));
        list.add(op(CMD_TRUSTSTORE, "TRUSTSTORE_PATH"));
        list.add(op(CMD_TRUSTSTORE_PASSWORD, "TRUSTSTORE_PASSWORD"));

        return list.toArray(new String[0]);
    }

    /**
     * @return Transaction command options.
     */
    private String[] getTxOptions() {
        List<String> list = new ArrayList<>();

        list.add(op(TxCommandArg.TX_XID, "XID"));
        list.add(op(TxCommandArg.TX_DURATION, "SECONDS"));
        list.add(op(TxCommandArg.TX_SIZE, "SIZE"));
        list.add(op(TxCommandArg.TX_LABEL, "PATTERN_REGEX"));
        list.add(op(or(TxCommandArg.TX_SERVERS, TxCommandArg.TX_CLIENTS)));
        list.add(op(TxCommandArg.TX_NODES, "consistentId1[,consistentId2,....,consistentIdN]"));
        list.add(op(TxCommandArg.TX_LIMIT, "NUMBER"));
        list.add(op(TxCommandArg.TX_ORDER, or(VisorTxSortOrder.values())));
        list.add(op(TxCommandArg.TX_KILL));
        list.add(op(CMD_AUTO_CONFIRMATION));

        return list.toArray(new String[list.size()]);
    }
}

