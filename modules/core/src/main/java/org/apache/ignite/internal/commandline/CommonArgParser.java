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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.ssl.SslContextFactory;

import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_INTERVAL;
import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

/**
 * Common argument parser.
 * Also would parse high-level command and delegate parsing for its argument to the command.
 */
public class CommonArgParser {
    /** */
    private final Logger logger;

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

    /** List of optional auxiliary commands. */
    private static final Set<String> AUX_COMMANDS = new HashSet<>();

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

        AUX_COMMANDS.add(CMD_KEYSTORE);
        AUX_COMMANDS.add(CMD_KEYSTORE_PASSWORD);
        AUX_COMMANDS.add(CMD_KEYSTORE_TYPE);

        AUX_COMMANDS.add(CMD_TRUSTSTORE);
        AUX_COMMANDS.add(CMD_TRUSTSTORE_PASSWORD);
        AUX_COMMANDS.add(CMD_TRUSTSTORE_TYPE);
    }

    /**
     * @param logger Logger.
     */
    public CommonArgParser(Logger logger) {
        this.logger = logger;
    }

    /**
     * Creates list of common utility options.
     *
     * @return Array of common utility options.
     */
    public static String[] getCommonOptions() {
        List<String> list = new ArrayList<>(32);

        list.add(optional(CMD_HOST, "HOST_OR_IP"));
        list.add(optional(CMD_PORT, "PORT"));
        list.add(optional(CMD_USER, "USER"));
        list.add(optional(CMD_PASSWORD, "PASSWORD"));
        list.add(optional(CMD_PING_INTERVAL, "PING_INTERVAL"));
        list.add(optional(CMD_PING_TIMEOUT, "PING_TIMEOUT"));
        list.add(optional(CMD_VERBOSE));
        list.add(optional(CMD_SSL_PROTOCOL, "SSL_PROTOCOL[, SSL_PROTOCOL_2, ..., SSL_PROTOCOL_N]"));
        list.add(optional(CMD_SSL_CIPHER_SUITES, "SSL_CIPHER_1[, SSL_CIPHER_2, ..., SSL_CIPHER_N]"));
        list.add(optional(CMD_SSL_KEY_ALGORITHM, "SSL_KEY_ALGORITHM"));
        list.add(optional(CMD_KEYSTORE_TYPE, "KEYSTORE_TYPE"));
        list.add(optional(CMD_KEYSTORE, "KEYSTORE_PATH"));
        list.add(optional(CMD_KEYSTORE_PASSWORD, "KEYSTORE_PASSWORD"));
        list.add(optional(CMD_TRUSTSTORE_TYPE, "TRUSTSTORE_TYPE"));
        list.add(optional(CMD_TRUSTSTORE, "TRUSTSTORE_PATH"));
        list.add(optional(CMD_TRUSTSTORE_PASSWORD, "TRUSTSTORE_PASSWORD"));

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
        String host = DFLT_HOST;

        String port = DFLT_PORT;

        String user = null;

        String pwd = null;

        Long pingInterval = DFLT_PING_INTERVAL;

        Long pingTimeout = DFLT_PING_TIMEOUT;

        boolean autoConfirmation = false;

        boolean verbose = false;

        String sslProtocol = DFLT_SSL_PROTOCOL;

        String sslCipherSuites = "";

        String sslKeyAlgorithm = SslContextFactory.DFLT_KEY_ALGORITHM;

        String sslKeyStoreType = SslContextFactory.DFLT_STORE_TYPE;

        String sslKeyStorePath = null;

        char sslKeyStorePassword[] = null;

        String sslTrustStoreType = SslContextFactory.DFLT_STORE_TYPE;

        String sslTrustStorePath = null;

        char sslTrustStorePassword[] = null;

        CommandArgIterator argIter = new CommandArgIterator(rawArgIter, AUX_COMMANDS);

        CommandList command = null;

        while (argIter.hasNextArg()) {
            String str = argIter.nextArg("").toLowerCase();

            CommandList cmd = CommandList.of(str);

            if (cmd != null) {
                if (command != null)
                    throw new IllegalArgumentException("Only one action can be specified, but found at least two:" +
                        cmd.toString() + ", " + command.toString());

                cmd.command().parseArguments(argIter);

                command = cmd;
            }
            else {

                switch (str) {
                    case CMD_HOST:
                        host = argIter.nextArg("Expected host name");

                        break;

                    case CMD_PORT:
                        port = argIter.nextArg("Expected port number");

                        try {
                            int p = Integer.parseInt(port);

                            if (p <= 0 || p > 65535)
                                throw new IllegalArgumentException("Invalid value for port: " + port);
                        }
                        catch (NumberFormatException ignored) {
                            throw new IllegalArgumentException("Invalid value for port: " + port);
                        }

                        break;

                    case CMD_PING_INTERVAL:
                        pingInterval = argIter.nextNonNegativeLongArg("ping interval");

                        break;

                    case CMD_PING_TIMEOUT:
                        pingTimeout = argIter.nextNonNegativeLongArg("ping timeout");

                        break;

                    case CMD_USER:
                        user = argIter.nextArg("Expected user name");

                        break;

                    case CMD_PASSWORD:
                        pwd = argIter.nextArg("Expected password");

                        logger.info(securityWarningMessage(CMD_PASSWORD));

                        break;

                    case CMD_SSL_PROTOCOL:
                        sslProtocol = argIter.nextArg("Expected SSL protocol");

                        break;

                    case CMD_SSL_CIPHER_SUITES:
                        sslCipherSuites = argIter.nextArg("Expected SSL cipher suites");

                        break;

                    case CMD_SSL_KEY_ALGORITHM:
                        sslKeyAlgorithm = argIter.nextArg("Expected SSL key algorithm");

                        break;

                    case CMD_KEYSTORE:
                        sslKeyStorePath = argIter.nextArg("Expected SSL key store path");

                        break;

                    case CMD_KEYSTORE_PASSWORD:
                        sslKeyStorePassword = argIter.nextArg("Expected SSL key store password").toCharArray();

                        logger.info(securityWarningMessage(CMD_KEYSTORE_PASSWORD));

                        break;

                    case CMD_KEYSTORE_TYPE:
                        sslKeyStoreType = argIter.nextArg("Expected SSL key store type");

                        break;

                    case CMD_TRUSTSTORE:
                        sslTrustStorePath = argIter.nextArg("Expected SSL trust store path");

                        break;

                    case CMD_TRUSTSTORE_PASSWORD:
                        sslTrustStorePassword = argIter.nextArg("Expected SSL trust store password").toCharArray();

                        logger.info(securityWarningMessage(CMD_TRUSTSTORE_PASSWORD));

                        break;

                    case CMD_TRUSTSTORE_TYPE:
                        sslTrustStoreType = argIter.nextArg("Expected SSL trust store type");

                        break;

                    case CMD_AUTO_CONFIRMATION:
                        autoConfirmation = true;

                        break;

                    case CMD_VERBOSE:
                        verbose = true;
                        break;

                    default:
                        throw new IllegalArgumentException("Unexpected argument: " + str);
                }
            }
        }

        if (command == null)
            throw new IllegalArgumentException("No action was specified");

        return new ConnectionAndSslParameters(command.command(), host, port, user, pwd,
                pingTimeout, pingInterval, autoConfirmation, verbose,
                sslProtocol, sslCipherSuites,
                sslKeyAlgorithm, sslKeyStorePath, sslKeyStorePassword, sslKeyStoreType,
                sslTrustStorePath, sslTrustStorePassword, sslTrustStoreType);
    }

    /**
     * @param password Parsed password.
     * @return String with warning to show for user.
     */
    private String securityWarningMessage(String password) {
        final String pwdArgWarnFmt = "Warning: %s is insecure. " +
            "Whenever possible, use interactive prompt for password (just discard %s option).";

        return String.format(pwdArgWarnFmt, password, password);
    }
}
