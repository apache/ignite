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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.ssl.SslContextFactory;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_INTERVAL;
import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.internal.management.api.CommandUtils.isBoolean;
import static org.apache.ignite.internal.management.api.CommandUtils.parseVal;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

/**
 * Common argument parser.
 * Also would parse high-level command and delegate parsing for its argument to the command.
 */
public class CommonArgParser {
    /** */
    private final IgniteLogger logger;

    /** */
    private final Map<String, DeclarativeCommandAdapter<?>> cmds;

    /** */
    static final String CMD_HOST = "--host";

    /** */
    static final String CMD_PORT = "--port";

    /** */
    static final String CMD_PASSWORD = "--password";

    /** */
    static final String CMD_USER = "--user";

    /** Option is used for auto confirmation. */
    public static final String CMD_AUTO_CONFIRMATION = "--yes";

    /** */
    static final String CMD_PING_INTERVAL = "--ping-interval";

    /** */
    static final String CMD_PING_TIMEOUT = "--ping-timeout";

    /** Verbose mode. */
    public static final String CMD_VERBOSE = "--verbose";

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

    /** */
    static final String CMD_ENABLE_EXPERIMENTAL = "--enable-experimental";

    /** */
    static final String CMD_SSL_FACTORY = "--ssl-factory";

    /** List of optional auxiliary commands. */
    private static final Set<String> AUX_COMMANDS = new HashSet<>();

    /** Set of sensitive arguments */
    private static final Set<String> SENSITIVE_ARGUMENTS = new HashSet<>();

    /** */
    private static final BiConsumer<String, Integer> PORT_VALIDATOR = (name, val) -> {
        if (val <= 0 || val > 65535)
            throw new IllegalArgumentException("Invalid value for " + name + ": " + val);
    };

    /** */
    private static final BiConsumer<String, Long> POSITIVE_LONG = (name, val) -> {
        if (val <= 0)
            throw new IllegalArgumentException("Invalid value for " + name + ": " + val);
    };

    /** */
    private final BiConsumer<String, String> securityWarn;

    /** */
    private final Map<String, CLIArgument<?>> args = new LinkedHashMap<>();

    static {
        AUX_COMMANDS.add(CMD_HOST);
        AUX_COMMANDS.add(CMD_PORT);

        AUX_COMMANDS.add(CMD_PASSWORD);
        AUX_COMMANDS.add(CMD_USER);

        AUX_COMMANDS.add(CMD_AUTO_CONFIRMATION);
        AUX_COMMANDS.add(CMD_VERBOSE);

        AUX_COMMANDS.add(CMD_PING_INTERVAL);
        AUX_COMMANDS.add(CMD_PING_TIMEOUT);

        AUX_COMMANDS.add(CMD_SSL_PROTOCOL);
        AUX_COMMANDS.add(CMD_SSL_KEY_ALGORITHM);
        AUX_COMMANDS.add(CMD_SSL_CIPHER_SUITES);
        AUX_COMMANDS.add(CMD_SSL_FACTORY);

        AUX_COMMANDS.add(CMD_KEYSTORE);
        AUX_COMMANDS.add(CMD_KEYSTORE_PASSWORD);
        AUX_COMMANDS.add(CMD_KEYSTORE_TYPE);

        AUX_COMMANDS.add(CMD_TRUSTSTORE);
        AUX_COMMANDS.add(CMD_TRUSTSTORE_PASSWORD);
        AUX_COMMANDS.add(CMD_TRUSTSTORE_TYPE);

        SENSITIVE_ARGUMENTS.add(CMD_PASSWORD);
        SENSITIVE_ARGUMENTS.add(CMD_KEYSTORE_PASSWORD);
        SENSITIVE_ARGUMENTS.add(CMD_TRUSTSTORE_PASSWORD);
    }

    /**
     * @param arg To check.
     * @return True if provided argument is among sensitive one and not should be displayed.
     */
    public static boolean isSensitiveArgument(String arg) {
        return SENSITIVE_ARGUMENTS.contains(arg);
    }

    /**
     * @param logger Logger.
     * @param cmds Supported commands.
     */
    public CommonArgParser(IgniteLogger logger, Map<String, DeclarativeCommandAdapter<?>> cmds) {
        this.logger = logger;
        this.cmds = cmds;
        this.securityWarn = (name, val) -> logger.info(String.format("Warning: %s is insecure. " +
            "Whenever possible, use interactive prompt for password (just discard %s option).", val, val));

        arg(CMD_HOST, String.class, "HOST_OR_IP", DFLT_HOST);
        arg(CMD_PORT, Integer.class, "PORT", DFLT_PORT, PORT_VALIDATOR);
        arg(CMD_USER, String.class, "USER", null);
        arg(CMD_PASSWORD, String.class, "PASSWORD", null, securityWarn);
        arg(CMD_PING_INTERVAL, Long.class, "PING_INTERVAL", DFLT_PING_INTERVAL, POSITIVE_LONG);
        arg(CMD_PING_TIMEOUT, Long.class, "PING_TIMEOUT", DFLT_PING_TIMEOUT, POSITIVE_LONG);
        arg(CMD_VERBOSE, boolean.class, CMD_VERBOSE, false);
        arg(CMD_SSL_PROTOCOL, String.class, "SSL_PROTOCOL[, SSL_PROTOCOL_2, ..., SSL_PROTOCOL_N]", DFLT_SSL_PROTOCOL);
        arg(CMD_SSL_CIPHER_SUITES, String.class, "SSL_CIPHER_1[, SSL_CIPHER_2, ..., SSL_CIPHER_N]", "");
        arg(CMD_SSL_KEY_ALGORITHM, String.class, "SSL_KEY_ALGORITHM", SslContextFactory.DFLT_KEY_ALGORITHM);
        arg(CMD_SSL_FACTORY, String.class, "SSL_FACTORY_PATH", null);
        arg(CMD_KEYSTORE_TYPE, String.class, "KEYSTORE_TYPE", SslContextFactory.DFLT_STORE_TYPE);
        arg(CMD_KEYSTORE, String.class, "KEYSTORE_PATH", null);
        arg(CMD_KEYSTORE_PASSWORD, String.class, "KEYSTORE_PASSWORD", null, securityWarn);
        arg(CMD_TRUSTSTORE_TYPE, String.class, "TRUSTSTORE_TYPE", SslContextFactory.DFLT_STORE_TYPE);
        arg(CMD_TRUSTSTORE, String.class, "TRUSTSTORE_PATH", null);
        arg(CMD_TRUSTSTORE_PASSWORD, String.class, "TRUSTSTORE_PASSWORD", null, securityWarn);
        arg(CMD_AUTO_CONFIRMATION, boolean.class, CMD_AUTO_CONFIRMATION, false);
        arg(
            CMD_ENABLE_EXPERIMENTAL,
            Boolean.class,
            CMD_ENABLE_EXPERIMENTAL,
            IgniteSystemProperties.getBoolean(IGNITE_ENABLE_EXPERIMENTAL_COMMAND)
        );
    }

    /**
     * Creates list of common utility options.
     *
     * @return Array of common utility options.
     */
    public String[] getCommonOptions() {
        List<String> list = new ArrayList<>();

        for (CLIArgument<?> arg : args.values()) {
            if (arg.name().equals(CMD_AUTO_CONFIRMATION))
                continue;

            if (isBoolean(arg.type()))
                list.add(optional(arg.name()));
            else
                list.add(optional(arg.name(), arg.usage()));
        }

        return list.toArray(new String[0]);
    }

    /**
     * Parses and validates arguments.
     *
     * @param rawArgIter Iterator of arguments.
     * @return Arguments bean.
     * @throws IllegalArgumentException In case arguments aren't valid.
     */
    ConnectionAndSslParameters parseAndValidate(Iterator<String> rawArgIter) {
        CommandArgIterator argIter = new CommandArgIterator(rawArgIter, AUX_COMMANDS, cmds);

        DeclarativeCommandAdapter<?> command = null;

        Map<String, Object> vals = new HashMap<>();

        while (argIter.hasNextArg()) {
            String str = argIter.nextArg("").toLowerCase();

            DeclarativeCommandAdapter<?> cmd = cmds.get(str);

            if (cmd != null) {
                if (command != null)
                    throw new IllegalArgumentException("Only one action can be specified, but found at least two:" +
                        cmd.toString() + ", " + command.toString());

                cmd.parseArguments(argIter);

                command = cmd;
            }
            else if (args.containsKey(str))
                vals.put(str, parseCommonArg(argIter, args.get(str)));
        }

        if (command == null)
            throw new IllegalArgumentException("No action was specified");

        if (!this.<Boolean>val(vals, CMD_ENABLE_EXPERIMENTAL) && command.experimental()) {
            logger.warning(String.format("To use experimental command add --enable-experimental parameter for %s",
                UTILITY_NAME));

            throw new IllegalArgumentException("Experimental commands disabled");
        }

        return new ConnectionAndSslParameters(
            command,
            val(vals, CMD_HOST),
            val(vals, CMD_PORT),
            val(vals, CMD_USER),
            val(vals, CMD_PASSWORD),
            val(vals, CMD_PING_INTERVAL),
            val(vals, CMD_PING_TIMEOUT),
            val(vals, CMD_AUTO_CONFIRMATION),
            val(vals, CMD_VERBOSE),
            val(vals, CMD_SSL_PROTOCOL),
            val(vals, CMD_SSL_CIPHER_SUITES),
            val(vals, CMD_SSL_KEY_ALGORITHM),
            val(vals, CMD_KEYSTORE),
            val(vals, CMD_KEYSTORE_PASSWORD),
            val(vals, CMD_KEYSTORE_TYPE),
            val(vals, CMD_TRUSTSTORE),
            val(vals, CMD_TRUSTSTORE_PASSWORD),
            val(vals, CMD_TRUSTSTORE_TYPE),
            val(vals, CMD_SSL_FACTORY)
        );
    }

    /** */
    private <T> T val(Map<String, Object> vals, String name) {
        return (T)vals.computeIfAbsent(name, key -> args.get(name).defaultValueSupplier().apply(null));
    }

    /** */
    private static <T> T parseCommonArg(
        CommandArgIterator argIter,
        CLIArgument<T> arg
    ) {
        if (isBoolean(arg.type()))
            return (T)Boolean.TRUE;

        T val = parseVal(argIter.nextArg("Expected " + arg.name() + " value"), arg.type());

        arg.validator().accept(arg.name(), val);

        return val;
    }

    /** */
    private <T> void arg(String name, Class<T> type, String usage, T dflt, BiConsumer<String, T> validator) {
        args.put(name, optionalArg(name, usage, type, t -> dflt, validator));
    }

    /** */
    private <T> void arg(String name, Class<T> type, String usage, T dflt) {
        args.put(name, optionalArg(name, usage, type, () -> dflt));
    }
}
