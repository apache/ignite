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

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLoggerFileHandler;
import org.apache.ignite.logger.java.JavaLoggerFormatter;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityCredentialsProvider;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.System.lineSeparator;
import static java.util.Objects.nonNull;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.errorMessage;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_VERBOSE;
import static org.apache.ignite.internal.commandline.CommonArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

/**
 * Class that execute several commands passed via command line.
 */
public class CommandHandler {
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
    private final Scanner in = new Scanner(System.in);

    /** Utility name. */
    public static final String UTILITY_NAME = "control.(sh|bat)";

    /** */
    public static final String NULL = "null";

    /** JULs logger. */
    private final Logger logger;

    /** Session. */
    protected final String ses = U.id8(UUID.randomUUID());

    /** Console instance. Public access needs for tests. */
    public GridConsole console = GridConsoleAdapter.getInstance();

    /** */
    private Object lastOperationRes;

    /** Date format. */
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    /**
     * @param args Arguments to parse and apply.
     */
    public static void main(String[] args) {
        CommandHandler hnd = new CommandHandler();

        System.exit(hnd.execute(Arrays.asList(args)));
    }

    /**
     * @return prepared JULs logger.
     */
    private Logger setupJavaLogger() {
        Logger result = initLogger(CommandHandler.class.getName() + "Log");

        // Adding logging to file.
        try {
            String absPathPattern = new File(JavaLoggerFileHandler.logDirectory(U.defaultWorkDirectory()), "control-utility-%g.log").getAbsolutePath();

            FileHandler fileHandler = new FileHandler(absPathPattern, 5 * 1024 * 1024, 5);

            fileHandler.setFormatter(new JavaLoggerFormatter());

            result.addHandler(fileHandler);
        }
        catch (Exception e) {
            System.out.println("Failed to configure logging to file");
        }

        // Adding logging to console.
        result.addHandler(setupStreamHandler());

        return result;
    }

    /**
     * @return StreamHandler with empty formatting
     */
    public static StreamHandler setupStreamHandler() {
        return new StreamHandler(System.out, new Formatter() {
            @Override public String format(LogRecord record) {
                return record.getMessage() + "\n";
            }
        });
    }

    /**
     * Initialises JULs logger with basic settings
     * @param loggerName logger name. If {@code null} anonymous logger is returned.
     * @return logger
     */
    public static Logger initLogger(@Nullable String loggerName) {
        Logger result;

        if (loggerName == null)
            result = Logger.getAnonymousLogger();
        else
            result = Logger.getLogger(loggerName);

        result.setLevel(Level.INFO);
        result.setUseParentHandlers(false);

        return result;
    }

    /**
     *
     */
    public CommandHandler() {
        logger = setupJavaLogger();
    }

    /**
     * @param logger Logger to use.
     */
    public CommandHandler(Logger logger) {
        this.logger = logger;
    }

    /**
     * Parse and execute command.
     *
     * @param rawArgs Arguments to parse and execute.
     * @return Exit code.
     */
    public int execute(List<String> rawArgs) {
        LocalDateTime startTime = LocalDateTime.now();

        Thread.currentThread().setName("session=" + ses);

        logger.info("Control utility [ver. " + ACK_VER_STR + "]");
        logger.info(COPYRIGHT);
        logger.info("User: " + System.getProperty("user.name"));
        logger.info("Time: " + startTime.format(formatter));

        String commandName = "";

        Throwable err = null;
        boolean verbose = false;

        try {
            if (F.isEmpty(rawArgs) || (rawArgs.size() == 1 && CMD_HELP.equalsIgnoreCase(rawArgs.get(0)))) {
                printHelp();

                return EXIT_CODE_OK;
            }

            verbose = F.exist(rawArgs, CMD_VERBOSE::equalsIgnoreCase);

            ConnectionAndSslParameters args = new CommonArgParser(logger).parseAndValidate(rawArgs.iterator());

            Command command = args.command();
            commandName = command.name();

            GridClientConfiguration clientCfg = getClientConfiguration(args);

            int tryConnectMaxCount = 3;

            boolean suppliedAuth = !F.isEmpty(args.userName()) && !F.isEmpty(args.password());

            boolean credentialsRequested = false;

            while (true) {
                try {
                    if (!args.autoConfirmation()) {
                        command.prepareConfirmation(clientCfg);

                        if (!confirm(command.confirmationPrompt())) {
                            logger.info("Operation cancelled.");

                            return EXIT_CODE_OK;
                        }
                    }

                    logger.info("Command [" + commandName + "] started");
                    logger.info("Arguments: " + String.join(" ", rawArgs));
                    logger.info(DELIM);

                    lastOperationRes = command.execute(clientCfg, logger);

                    break;
                }
                catch (Throwable e) {
                    if (!isAuthError(e))
                        throw e;

                    if (suppliedAuth)
                        throw new GridClientAuthenticationException("Wrong credentials.");

                    if (tryConnectMaxCount == 0) {
                        throw new GridClientAuthenticationException("Maximum number of " +
                            "retries exceeded");
                    }

                    logger.info(credentialsRequested ?
                        "Authentication error, please try again." :
                        "This cluster requires authentication.");

                    if (credentialsRequested)
                        tryConnectMaxCount--;

                    String user = retrieveUserName(args, clientCfg);

                    String pwd = new String(requestPasswordFromConsole("password: "));

                    clientCfg = getClientConfiguration(user, pwd, args);

                    credentialsRequested = true;
                }
            }

            logger.info("Command [" + commandName + "] finished with code: " + EXIT_CODE_OK);

            return EXIT_CODE_OK;
        }
        catch (IllegalArgumentException e) {
            logger.severe("Check arguments. " + errorMessage(e));
            logger.info("Command [" + commandName + "] finished with code: " + EXIT_CODE_INVALID_ARGUMENTS);

            if (verbose)
                err = e;

            return EXIT_CODE_INVALID_ARGUMENTS;
        }
        catch (Throwable e) {
            if (isAuthError(e)) {
                logger.severe("Authentication error. " + errorMessage(e));
                logger.info("Command [" + commandName + "] finished with code: " + ERR_AUTHENTICATION_FAILED);

                if (verbose)
                    err = e;

                return ERR_AUTHENTICATION_FAILED;
            }

            if (isConnectionError(e)) {
                IgniteCheckedException cause = X.cause(e, IgniteCheckedException.class);

                if (isConnectionClosedSilentlyException(e))
                    logger.severe("Connection to cluster failed. Please check firewall settings and " +
                        "client and server are using the same SSL configuration.");
                else {
                    if (isSSLMisconfigurationError(cause))
                        e = cause;

                    logger.severe("Connection to cluster failed. " + errorMessage(e));

                }

                logger.info("Command [" + commandName + "] finished with code: " + EXIT_CODE_CONNECTION_FAILED);

                if (verbose)
                    err = e;

                return EXIT_CODE_CONNECTION_FAILED;
            }

            if (X.hasCause(e, IllegalArgumentException.class)) {
                IllegalArgumentException iae = X.cause(e, IllegalArgumentException.class);

                logger.severe("Check arguments. " + errorMessage(iae));
                logger.info("Command [" + commandName + "] finished with code: " + EXIT_CODE_INVALID_ARGUMENTS);

                if (verbose)
                    err = e;

                return EXIT_CODE_INVALID_ARGUMENTS;
            }

            logger.severe(errorMessage(e));
            logger.info("Command [" + commandName + "] finished with code: " + EXIT_CODE_UNEXPECTED_ERROR);

            err = e;

            return EXIT_CODE_UNEXPECTED_ERROR;
        }
        finally {
            LocalDateTime endTime = LocalDateTime.now();

            Duration diff = Duration.between(startTime, endTime);

            if (nonNull(err))
                logger.info("Error stack trace:" + System.lineSeparator() + X.getFullStackTrace(err));

            logger.info("Control utility has completed execution at: " + endTime.format(formatter));
            logger.info("Execution time: " + diff.toMillis() + " ms");

            Arrays.stream(logger.getHandlers())
                  .filter(handler -> handler instanceof FileHandler)
                  .forEach(Handler::close);
        }
    }

    /**
     * Analyses passed exception to find out whether it is related to SSL misconfiguration issues.
     *
     * (!) Implementation depends heavily on structure of exception stack trace
     * thus is very fragile to any changes in that structure.
     *
     * @param e Exception to analyze.
     *
     * @return {@code True} if exception may be related to SSL misconfiguration issues.
     */
    private boolean isSSLMisconfigurationError(Throwable e) {
        return e != null && e.getMessage() != null && e.getMessage().contains("SSL");
    }

    /**
     * Analyses passed exception to find out whether it is caused by server closing connection silently.
     * This happens when client tries to establish unprotected connection
     * to the cluster supporting only secured communications (e.g. when server is configured to use SSL certificates
     * and client is not).
     *
     * (!) Implementation depends heavily on structure of exception stack trace
     * thus is very fragile to any changes in that structure.
     *
     * @param e Exception to analyse.
     * @return {@code True} if exception may be related to the attempt to establish unprotected connection
     * to secured cluster.
     */
    private boolean isConnectionClosedSilentlyException(Throwable e) {
        if (!(e instanceof GridClientDisconnectedException))
            return false;

        Throwable cause = e.getCause();

        if (cause == null)
            return false;

        cause = cause.getCause();

        if (cause instanceof GridClientConnectionResetException &&
            cause.getMessage() != null &&
            cause.getMessage().contains("Failed to perform handshake")
        )
            return true;

        return false;
    }

    /**
     * Does one of three things:
     * <ul>
     *     <li>returns user name from connection parameters if it is there;</li>
     *     <li>returns user name from client configuration if it is there;</li>
     *     <li>requests user input and returns entered name.</li>
     * </ul>
     *
     * @param args Connection parameters.
     * @param clientCfg Client configuration.
     * @throws IgniteCheckedException If security credetials cannot be provided from client configuration.
     */
    private String retrieveUserName(
        ConnectionAndSslParameters args,
        GridClientConfiguration clientCfg
    ) throws IgniteCheckedException {
        if (!F.isEmpty(args.userName()))
            return args.userName();
        else if (clientCfg.getSecurityCredentialsProvider() == null)
            return requestDataFromConsole("user: ");
        else
            return (String)clientCfg.getSecurityCredentialsProvider().credentials().getLogin();
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
        else {
            char[] keyStorePwd = requestPasswordFromConsole("SSL keystore password: ");

            args.sslKeyStorePassword(keyStorePwd);
            factory.setKeyStorePassword(keyStorePwd);
        }

        factory.setKeyStoreType(args.sslKeyStoreType());

        if (F.isEmpty(args.sslTrustStorePath()))
            factory.setTrustManagers(GridSslBasicContextFactory.getDisabledTrustManager());
        else {
            factory.setTrustStoreFilePath(args.sslTrustStorePath());

            if (args.sslTrustStorePassword() != null)
                factory.setTrustStorePassword(args.sslTrustStorePassword());
            else {
                char[] trustStorePwd = requestPasswordFromConsole("SSL truststore password: ");

                args.sslTrustStorePassword(trustStorePwd);
                factory.setTrustStorePassword(trustStorePwd);
            }

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

        return in.nextLine();
    }

    /**
     * Requests interactive user confirmation if forthcoming operation is dangerous.
     *
     * @return {@code true} if operation confirmed (or not needed), {@code false} otherwise.
     */
    private boolean confirm(String str) {
        if (str == null)
            return true;

        String prompt = str + lineSeparator() + "Press '" + CONFIRM_MSG + "' to continue . . . ";

        return CONFIRM_MSG.equalsIgnoreCase(readLine(prompt));
    }

    /**
     * @param e Exception to check.
     * @return {@code true} if specified exception is {@link GridClientAuthenticationException}.
     */
    public static boolean isAuthError(Throwable e) {
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

            logger.info(msg);

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
        logger.info("Control utility script is used to execute admin commands on cluster or get common cluster info. " +
            "The command has the following syntax:");
        logger.info("");

        logger.info(INDENT + CommandLogger.join(" ", CommandLogger.join(" ", UTILITY_NAME, CommandLogger.join(" ", getCommonOptions())),
            optional("command"), "<command_parameters>"));
        logger.info("");
        logger.info("");

        logger.info("This utility can do the following commands:");

        Arrays.stream(CommandList.values()).forEach(c -> c.command().printUsage(logger));

        logger.info("");
        logger.info("By default commands affecting the cluster require interactive confirmation.");
        logger.info("Use " + CMD_AUTO_CONFIRMATION + " option to disable it.");
        logger.info("");

        logger.info("Default values:");
        logger.info(DOUBLE_INDENT + "HOST_OR_IP=" + DFLT_HOST);
        logger.info(DOUBLE_INDENT + "PORT=" + DFLT_PORT);
        logger.info(DOUBLE_INDENT + "PING_INTERVAL=" + DFLT_PING_INTERVAL);
        logger.info(DOUBLE_INDENT + "PING_TIMEOUT=" + DFLT_PING_TIMEOUT);
        logger.info(DOUBLE_INDENT + "SSL_PROTOCOL=" + SslContextFactory.DFLT_SSL_PROTOCOL);
        logger.info(DOUBLE_INDENT + "SSL_KEY_ALGORITHM=" + SslContextFactory.DFLT_KEY_ALGORITHM);
        logger.info(DOUBLE_INDENT + "KEYSTORE_TYPE=" + SslContextFactory.DFLT_STORE_TYPE);
        logger.info(DOUBLE_INDENT + "TRUSTSTORE_TYPE=" + SslContextFactory.DFLT_STORE_TYPE);

        logger.info("");

        logger.info("Exit codes:");
        logger.info(DOUBLE_INDENT + EXIT_CODE_OK + " - successful execution.");
        logger.info(DOUBLE_INDENT + EXIT_CODE_INVALID_ARGUMENTS + " - invalid arguments.");
        logger.info(DOUBLE_INDENT + EXIT_CODE_CONNECTION_FAILED + " - connection failed.");
        logger.info(DOUBLE_INDENT + ERR_AUTHENTICATION_FAILED + " - authentication failed.");
        logger.info(DOUBLE_INDENT + EXIT_CODE_UNEXPECTED_ERROR + " - unexpected error.");
    }
}
