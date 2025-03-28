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

import java.lang.reflect.Field;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.BeforeNodeStartCommand;
import org.apache.ignite.internal.management.api.CliConfirmArgument;
import org.apache.ignite.internal.management.api.CliSubcommandsWithPrefix;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.HelpCommand;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.management.cache.CacheCommand;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.spring.IgniteSpringHelperImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationContext;

import static java.lang.System.lineSeparator;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_ENABLE_EXPERIMENTAL;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_VERBOSE;
import static org.apache.ignite.internal.commandline.CommandLogger.errorMessage;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.NAME_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.asOptional;
import static org.apache.ignite.internal.management.api.CommandUtils.cmdText;
import static org.apache.ignite.internal.management.api.CommandUtils.executable;
import static org.apache.ignite.internal.management.api.CommandUtils.hasDescription;
import static org.apache.ignite.internal.management.api.CommandUtils.join;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterExample;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.valueExample;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;

/**
 * Class that execute several commands passed via command line.
 */
public class CommandHandler {
    /** */
    static final String CMD_HELP = "--help";

    /** */
    public static final String CONFIRM_MSG = "y";

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
    public static final String DFLT_HOST = "127.0.0.1";

    /** */
    public static final int DFLT_PORT = 10800;

    /** */
    private final Scanner in = new Scanner(System.in);

    /** Utility name. */
    public static final String UTILITY_NAME = "control.(sh|bat)";

    /** JULs logger. */
    private final IgniteLogger logger;

    /** Supported commands. */
    private final IgniteCommandRegistry registry = new IgniteCommandRegistry();

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
    public static IgniteLogger setupJavaLogger(String appName, Class<?> cls) {
        try {
            IgniteLogger log =
                U.initLogger(null, appName, null, U.defaultWorkDirectory()).getLogger(cls.getName() + "Log");

            if (log instanceof IgniteLoggerEx)
                ((IgniteLoggerEx)log).addConsoleAppender(true);

            return log;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     *
     */
    public CommandHandler() {
        this(setupJavaLogger("control-utility", CommandHandler.class));
    }

    /**
     * @param logger Logger to use.
     */
    public CommandHandler(IgniteLogger logger) {
        this.logger = logger;

        CommandUtils.loadExternalCommands().forEach(cmd -> {
            String k = cmdText(cmd);

            if (logger.isDebugEnabled())
                logger.debug("Registering command: " + k);

            if (registry.command(k) != null) {
                throw new IllegalArgumentException("Found conflict for command " + k + ". Tries to register command " + cmd +
                    ", but this command has already been " + "registered " + registry.command(k));
            }
            else
                registry.register(cmd);
        });
    }

    /**
     * Parse and execute command.
     *
     * @param rawArgs Arguments to parse and execute.
     * @return Exit code.
     */
    public <A extends IgniteDataTransferObject> int execute(List<String> rawArgs) {
        LocalDateTime startTime = LocalDateTime.now();

        Thread.currentThread().setName("session=" + ses);

        logger.info("Control utility [ver. " + ACK_VER_STR + "]");
        logger.info(COPYRIGHT);
        logger.info("User: " + System.getProperty("user.name"));
        logger.info("Time: " + startTime.format(formatter));

        String cmdName = "";

        Throwable err = null;
        boolean verbose = false;

        try {
            if (isHelp(rawArgs)) {
                printHelp(rawArgs);

                return EXIT_CODE_OK;
            }

            verbose = F.exist(rawArgs, CMD_VERBOSE::equalsIgnoreCase);

            ConnectionAndSslParameters<A> args = new ArgumentParser(logger, registry, console).parseAndValidate(rawArgs);

            cmdName = toFormattedCommandName(args.cmdPath().peekLast().getClass()).toUpperCase();

            int tryConnectMaxCnt = 3;

            boolean suppliedAuth = !F.isEmpty(args.userName()) && !F.isEmpty(args.password());

            boolean credentialsRequested = false;

            while (true) {
                try (
                    CliIgniteClientInvoker<A> invoker =
                         new CliIgniteClientInvoker<>(args.command(), args.commandArg(), clientConfiguration(args))
                ) {
                    if (!invoker.prepare(logger::info))
                        return EXIT_CODE_OK;

                    if (!args.autoConfirmation() && !confirm(invoker.confirmationPrompt())) {
                        logger.info("Operation cancelled.");

                        return EXIT_CODE_OK;
                    }

                    logger.info("Command [" + cmdName + "] started");
                    logger.info("Arguments: " + args.safeCommandString());
                    logger.info(U.DELIM);

                    String deprecationMsg = args.command().deprecationMessage(args.commandArg());

                    if (deprecationMsg != null)
                        logger.warning(deprecationMsg);

                    if (args.command() instanceof HelpCommand)
                        printUsage(logger, args.cmdPath().peekLast());
                    else if (args.command() instanceof BeforeNodeStartCommand)
                        lastOperationRes = invoker.invokeBeforeNodeStart(logger::info);
                    else
                        lastOperationRes = invoker.invoke(logger::info);

                    break;
                }
                catch (Throwable e) {
                    if (!isAuthError(e))
                        throw e;

                    if (suppliedAuth)
                        throw new IgniteAuthenticationException("Wrong credentials.");

                    if (tryConnectMaxCnt == 0)
                        throw new IgniteAuthenticationException("Maximum number of retries exceeded");

                    logger.info(credentialsRequested ?
                        "Authentication error, please try again." :
                        "This cluster requires authentication.");

                    if (credentialsRequested)
                        tryConnectMaxCnt--;

                    if (F.isEmpty(args.userName()))
                        args.userName(requestDataFromConsole("user: "));

                    args.password(new String(requestPasswordFromConsole("password: ")));

                    credentialsRequested = true;
                }
            }

            logger.info("Command [" + cmdName + "] finished with code: " + EXIT_CODE_OK);

            return EXIT_CODE_OK;
        }
        catch (Throwable e) {
            logger.error("Failed to perform operation.");

            if (isAuthError(e)) {
                logger.error("Authentication error. " + errorMessage(e));
                logger.info("Command [" + cmdName + "] finished with code: " + ERR_AUTHENTICATION_FAILED);

                if (verbose)
                    err = e;

                return ERR_AUTHENTICATION_FAILED;
            }

            if (isConnectionError(e)) {
                IgniteCheckedException cause = X.cause(e, IgniteCheckedException.class);

                if (isConnectionClosedSilentlyException(e))
                    logger.error("Connection to cluster failed. Please check firewall settings and " +
                        "client and server are using the same SSL configuration.");
                else {
                    if (isSSLMisconfigurationError(cause))
                        e = cause;

                    logger.error("Connection to cluster failed. " + errorMessage(e));
                }

                logger.error("Make sure you are connecting to the client connector (configured on a node via '" +
                    ClientConnectorConfiguration.class.getName() + "')");

                logger.info("Command [" + cmdName + "] finished with code: " + EXIT_CODE_CONNECTION_FAILED);

                if (verbose)
                    err = e;

                return EXIT_CODE_CONNECTION_FAILED;
            }

            if (X.hasCause(e, IllegalArgumentException.class)) {
                IllegalArgumentException iae = X.cause(e, IllegalArgumentException.class);

                logger.error("Check arguments. " + errorMessage(iae));
                logger.info("Command [" + cmdName + "] finished with code: " + EXIT_CODE_INVALID_ARGUMENTS);

                if (verbose)
                    err = e;

                return EXIT_CODE_INVALID_ARGUMENTS;
            }

            logger.error(errorMessage(e));
            logger.info("Command [" + cmdName + "] finished with code: " + EXIT_CODE_UNEXPECTED_ERROR);

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

            if (logger instanceof IgniteLoggerEx)
                ((IgniteLoggerEx)logger).flush();
        }
    }

    /** @return {@code True} if arguments means "print help" command. */
    private boolean isHelp(List<String> rawArgs) {
        if (F.isEmpty(rawArgs))
            return true;

        if (rawArgs.size() > 2)
            return false;

        boolean help = false;
        boolean experimental = false;

        for (String arg : rawArgs) {
            if (CMD_HELP.equalsIgnoreCase(arg))
                help = true;
            else if (CMD_ENABLE_EXPERIMENTAL.equalsIgnoreCase(arg))
                experimental = true;
            else
                return false;
        }

        return help || experimental;
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
        return e instanceof ClientConnectionException && e.getMessage().startsWith("Channel is closed");
    }

    /**
     * @param args Common arguments.
     * @return Thin client configuration to connect to cluster.
     * @throws IgniteCheckedException If error occur.
     */
    private ClientConfiguration clientConfiguration(ConnectionAndSslParameters args) throws IgniteCheckedException {
        ClientConfiguration clientCfg = new ClientConfiguration();

        clientCfg.setAddresses(args.host() + ":" + args.port());

        if (!F.isEmpty(args.userName())) {
            clientCfg.setUserName(args.userName());
            clientCfg.setUserPassword(args.password());
        }

        if (!F.isEmpty(args.sslKeyStorePath()) || !F.isEmpty(args.sslFactoryConfigPath())) {
            if (!F.isEmpty(args.sslKeyStorePath()) && !F.isEmpty(args.sslFactoryConfigPath())) {
                throw new IllegalArgumentException("Incorrect SSL configuration. SSL factory config path should " +
                    "not be specified simultaneously with other SSL options like keystore path.");
            }

            clientCfg.setSslContextFactory(createSslSupportFactory(args));
            clientCfg.setSslMode(SslMode.REQUIRED);
        }

        clientCfg.setClusterDiscoveryEnabled(false);

        return clientCfg;
    }

    /**
     * @param args Commond args.
     * @return Ssl support factory.
     */
    @NotNull private Factory<SSLContext> createSslSupportFactory(ConnectionAndSslParameters args) throws IgniteCheckedException {
        if (!F.isEmpty(args.sslFactoryConfigPath())) {
            URL springCfg = IgniteUtils.resolveSpringUrl(args.sslFactoryConfigPath());

            ApplicationContext ctx = IgniteSpringHelperImpl.applicationContext(springCfg);

            return (Factory<SSLContext>)ctx.getBean(Factory.class);
        }

        SslContextFactory factory = new SslContextFactory();

        if (args.sslProtocol().length > 1)
            factory.setProtocols(args.sslProtocol());
        else
            factory.setProtocol(args.sslProtocol()[0]);

        factory.setKeyAlgorithm(args.sslKeyAlgorithm());
        factory.setCipherSuites(args.getSslCipherSuites());
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
            factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
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
     * @return {@code true} if specified exception is an authentication exception.
     */
    public static boolean isAuthError(Throwable e) {
        return X.hasCause(e, ClientAuthenticationException.class);
    }

    /**
     * @param e Exception to check.
     * @return {@code true} if specified exception is a connection error.
     */
    private static boolean isConnectionError(Throwable e) {
        return X.hasCause(e, ClientConnectionException.class);
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

    /** @param rawArgs Arguments. */
    private void printHelp(List<String> rawArgs) {
        boolean experimentalEnabled = rawArgs.stream().anyMatch(CMD_ENABLE_EXPERIMENTAL::equalsIgnoreCase) ||
            getBoolean(IGNITE_ENABLE_EXPERIMENTAL_COMMAND);

        logger.info("Control utility script is used to execute admin commands on cluster or get common cluster info. " +
            "The command has the following syntax:");
        logger.info("");

        logger.info(INDENT + join(" ",
            join(" ", UTILITY_NAME, join(" ", new ArgumentParser(logger, registry, null).getCommonOptions())),
            asOptional("command", true), "<command_parameters>"));
        logger.info("");
        logger.info("");

        logger.info("This utility can do the following commands:");

        registry.commands().forEachRemaining(e -> {
            if (experimentalEnabled || !e.getValue().getClass().isAnnotationPresent(IgniteExperimental.class)) {
                if (Objects.equals(e.getValue().getClass(), CacheCommand.class)) {
                    logger.info("");
                    logger.info("View caches information in a cluster. For more details type:");
                    logger.info(DOUBLE_INDENT + UTILITY_NAME + " --cache help");

                    return;
                }

                printUsage(logger, e.getValue());
            }
        });

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

    /**
     * Print info for user about command (parameters, use cases and so on).
     *
     * @param logger Logger to use.
     */
    public void printUsage(IgniteLogger logger, Command<?, ?> cmd) {
        if (cmd instanceof CacheCommand || cmd instanceof CacheCommand.CacheHelpCommand)
            printCacheHelpHeader(logger);

        usage(cmd, Collections.emptyList(), logger);

        if (cmd instanceof CacheCommand || cmd instanceof CacheCommand.CacheHelpCommand)
            logger.info("");
    }

    /** */
    private void printCacheHelpHeader(IgniteLogger logger) {
        logger.info(INDENT + "The '--cache subcommand' is used to get information about and perform actions" +
            " with caches. The command has the following syntax:");
        logger.info("");
        logger.info(INDENT + join(" ", UTILITY_NAME, join(" ", new ArgumentParser(logger, null, null).getCommonOptions())) +
            " --cache [subcommand] <subcommand_parameters>");
        logger.info("");
        logger.info(INDENT + "The subcommands that take [nodeId] as an argument ('list', 'find_garbage', " +
            "'contention' and 'validate_indexes') will be executed on the given node or on all server nodes" +
            " if the option is not specified. Other commands will run on a random server node.");
        logger.info("");
        logger.info("");
        logger.info(INDENT + "Subcommands:");
    }

    /**
     * Generates usage for base command and all of its children, if any.
     *
     * @param cmd Base command.
     * @param parents Collection of parent commands.
     * @param logger Logger to print help to.
     */
    public static void usage(Command<?, ?> cmd, List<Command<?, ?>> parents, IgniteLogger logger) {
        if (executable(cmd)) {
            logger.info("");

            if (cmd.getClass().isAnnotationPresent(IgniteExperimental.class))
                logger.info(INDENT + "[EXPERIMENTAL]");

            printExample(cmd, parents, logger);

            if (CommandUtils.hasDescribedParameters(cmd)) {
                logger.info("");
                logger.info(DOUBLE_INDENT + "Parameters:");

                LengthCalculator lenCalc = new LengthCalculator();

                visitCommandParams(cmd.argClass(), lenCalc, lenCalc, (argGrp, flds) -> flds.forEach(lenCalc));

                Consumer<Field> printer = fld -> {
                    BiConsumer<String, String> logParam = (name, description) -> {
                        if (F.isEmpty(description))
                            return;

                        logger.info(
                            DOUBLE_INDENT + INDENT + U.extendToLen(name, lenCalc.length) + "  - " + description + "."
                        );
                    };

                    logParam.accept(
                        parameterExample(fld, false),
                        fld.getAnnotation(Argument.class).description()
                    );

                    if (fld.isAnnotationPresent(EnumDescription.class)) {
                        EnumDescription enumDesc = fld.getAnnotation(EnumDescription.class);

                        String[] names = formattedEnumNames(fld);
                        String[] descriptions = enumDesc.descriptions();

                        for (int i = 0; i < names.length; i++)
                            logParam.accept(names[i], descriptions[i]);
                    }
                };

                visitCommandParams(cmd.argClass(), printer, printer, (argGrp, flds) -> {
                    flds.stream().filter(fld -> fld.isAnnotationPresent(Positional.class)).forEach(printer);
                    flds.stream().filter(fld -> !fld.isAnnotationPresent(Positional.class)).forEach(printer);
                });
            }
        }

        if (cmd instanceof CommandsRegistry) {
            List<Command<?, ?>> parents0 = new ArrayList<>(parents);

            parents0.add(cmd);

            ((CommandsRegistry<?, ?>)cmd).commands().forEachRemaining(cmd0 -> usage(cmd0.getValue(), parents0, logger));
        }
    }

    /**
     * Generates and prints example of command.
     *
     * @param cmd Command.
     * @param parents Collection of parent commands.
     * @param logger Logger to print help to.
     */
    private static void printExample(Command<?, ?> cmd, List<Command<?, ?>> parents, IgniteLogger logger) {
        logger.info(INDENT + cmd.description() + ":");

        StringBuilder bldr = new StringBuilder(DOUBLE_INDENT + UTILITY_NAME);

        CommandName name = new CommandName();

        parents.forEach(name);
        name.accept(cmd);

        bldr.append(name.name.toString());

        BiConsumer<Boolean, Field> paramPrinter = (spaceReq, fld) -> {
            if (spaceReq)
                bldr.append(' ');

            bldr.append(parameterExample(fld, true));
        };

        visitCommandParams(
            cmd.argClass(),
            fld -> bldr.append(' ').append(valueExample(fld)),
            fld -> paramPrinter.accept(true, fld),
            (argGrp, flds) -> {
                if (argGrp.onlyOneOf()) {
                    bldr.append(' ');

                    if (argGrp.optional())
                        bldr.append('[');

                    for (int i = 0; i < flds.size(); i++) {
                        if (i != 0)
                            bldr.append('|');

                        paramPrinter.accept(false, flds.get(i));
                    }

                    if (argGrp.optional())
                        bldr.append(']');
                }
                else {
                    flds.stream()
                        .filter(fld -> fld.isAnnotationPresent(Positional.class))
                        .forEach(fld -> bldr.append(' ').append(valueExample(fld)));
                    flds.stream()
                        .filter(fld -> !fld.isAnnotationPresent(Positional.class))
                        .forEach(fld -> paramPrinter.accept(true, fld));
                }
            }
        );

        if (cmd.argClass().isAnnotationPresent(CliConfirmArgument.class))
            bldr.append(' ').append(CommandUtils.asOptional(CMD_AUTO_CONFIRMATION, true));

        logger.info(bldr.toString());
    }

    /** */
    private static String[] formattedEnumNames(Field fld) {
        EnumDescription desc = fld.getAnnotation(EnumDescription.class);

        String indent = fld.isAnnotationPresent(Positional.class) ? "" : INDENT;

        return Arrays.stream(desc.names()).map(s -> indent + s).toArray(String[]::new);
    }

    /** */
    private static class LengthCalculator implements Consumer<Field> {
        /** */
        int length;

        /** {@inheritDoc} */
        @Override public void accept(Field fld) {
            if (!hasDescription(fld))
                return;

            length = Math.max(length, parameterExample(fld, false).length());

            if (fld.isAnnotationPresent(EnumDescription.class)) {
                for (String name : formattedEnumNames(fld))
                    length = Math.max(length, name.length());
            }
        }
    }

    /** */
    public static class CommandName implements Consumer<Object> {
        /** */
        private boolean prefixInclude = true;

        /** */
        private String parentPrefix;

        /** */
        StringBuilder name = new StringBuilder();

        /** {@inheritDoc} */
        @Override public void accept(Object cmd) {
            name.append(' ');

            if (prefixInclude)
                name.append(NAME_PREFIX);

            String cmdName = toFormattedCommandName(cmd.getClass());

            String parentPrefix0 = parentPrefix;

            parentPrefix = cmdName;

            if (!F.isEmpty(parentPrefix0)) {
                cmdName = cmdName.replaceFirst(parentPrefix0 + CMD_WORDS_DELIM, "");

                if (!prefixInclude)
                    cmdName = cmdName.replaceAll(CMD_WORDS_DELIM + "", PARAM_WORDS_DELIM + "");
            }

            name.append(cmdName);

            if (cmd instanceof CommandsRegistry)
                prefixInclude = cmd.getClass().isAnnotationPresent(CliSubcommandsWithPrefix.class);
        }
    }
}
