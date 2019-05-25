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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.GridClientAuthenticationException;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientHandshakeException;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionResetException;
import org.apache.ignite.internal.client.ssl.GridSslBasicContextFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityCredentialsProvider;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.CommonArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

/**
 * Class that execute several commands passed via command line.
 */
public class CommandHandler {
    /** Logger. */
    private final CommandLogger logger = new CommandLogger();

    /** */
    static final String CMD_HELP = "--help";

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
    public static final String UTILITY_NAME = "control.sh";

    /** */
    public static final String NULL = "null";

    /** Console instance. Public access needs for tests. */
    public GridConsole console = GridConsoleAdapter.getInstance();

    /** */
    private Object lastOperationRes;

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

            ConnectionAndSslParameters args = new CommonArgParser(logger).parseAndValidate(rawArgs.iterator());

            Command command = args.command();

            if (!args.autoConfirmation() && !confirm(command.confirmationPrompt())) {
                logger.log("Operation cancelled.");

                return EXIT_CODE_OK;
            }

            boolean tryConnectAgain = true;

            int tryConnectMaxCount = 3;

            boolean suppliedAuth = !F.isEmpty(args.userName()) && !F.isEmpty(args.password());

            GridClientConfiguration clientCfg = getClientConfiguration(args);

            while (tryConnectAgain) {
                tryConnectAgain = false;

                try {
                    lastOperationRes = command.execute(clientCfg, logger);
                }
                catch (Throwable e) {
                    if (tryConnectMaxCount > 0 && isAuthError(e)) {
                        logger.log(suppliedAuth ?
                            "Authentication error, please try again." :
                            "This cluster requires authentication.");

                        String user = clientCfg.getSecurityCredentialsProvider() == null ?
                            requestDataFromConsole("user: ") :
                            (String)clientCfg.getSecurityCredentialsProvider().credentials().getLogin();

                        clientCfg = getClientConfiguration(user, new String(requestPasswordFromConsole("password: ")),  args);

                        tryConnectAgain = true;

                        suppliedAuth = true;

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

    /**
     * @param args Common arguments.
     * @return Thin client configuration to connect to cluster.
     * @throws IgniteCheckedException If error occur.
     */
    @NotNull private GridClientConfiguration getClientConfiguration(
        ConnectionAndSslParameters args
    ) throws IgniteCheckedException {
        return getClientConfiguration(args.userName(), args.password(), args);
    }

    /**
     * @param userName User name for authorization.
     * @param password Password for authorization.
     * @param args Common arguments.
     * @return Thin client configuration to connect to cluster.
     * @throws IgniteCheckedException If error occur.
     */
    @NotNull private GridClientConfiguration getClientConfiguration(
        String userName,
        String password,
        ConnectionAndSslParameters args
    ) throws IgniteCheckedException {
        GridClientConfiguration clientCfg = new GridClientConfiguration();

        clientCfg.setPingInterval(args.pingInterval());

        clientCfg.setPingTimeout(args.pingTimeout());

        clientCfg.setServers(Collections.singletonList(args.host() + ":" + args.port()));

        if (!F.isEmpty(userName))
            clientCfg.setSecurityCredentialsProvider(getSecurityCredentialsProvider(userName, password, clientCfg));

        if (!F.isEmpty(args.sslKeyStorePath()))
            clientCfg.setSslContextFactory(createSslSupportFactory(args));

        return clientCfg;
    }

    /**
     * @param userName User name for authorization.
     * @param password Password for authorization.
     * @param clientCfg Thin client configuration to connect to cluster.
     * @return Security credentials provider with usage of given user name and password.
     * @throws IgniteCheckedException If error occur.
     */
    @NotNull private SecurityCredentialsProvider getSecurityCredentialsProvider(
        String userName,
        String password,
        GridClientConfiguration clientCfg
    ) throws IgniteCheckedException {
        SecurityCredentialsProvider securityCredential = clientCfg.getSecurityCredentialsProvider();

        if (securityCredential == null)
            return new SecurityCredentialsBasicProvider(new SecurityCredentials(userName, password));

        final SecurityCredentials credential = securityCredential.credentials();
        credential.setLogin(userName);
        credential.setPassword(password);

        return securityCredential;
    }

    /**
     * @param args Commond args.
     * @return Ssl support factory.
     */
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
        logger.log("Control.sh is used to execute admin commands on cluster or get common cluster info. " +
            "The command has the following syntax:");
        logger.nl();

        logger.logWithIndent(CommandLogger.join(" ", CommandLogger.join(" ", UTILITY_NAME, CommandLogger.join(" ", getCommonOptions())),
            optional("command"), "<command_parameters>"));
        logger.nl();
        logger.nl();

        logger.log("This utility can do the following commands:");

        Arrays.stream(CommandList.values()).forEach(c -> c.command().printUsage(logger));

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
}
