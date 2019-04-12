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
import java.util.List;
import org.apache.ignite.ssl.SslContextFactory;

import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_INTERVAL;
import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_KEYSTORE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_KEYSTORE_TYPE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_PING_INTERVAL;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_SSL_CIPHER_SUITES;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_SSL_KEY_ALGORITHM;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_SSL_PROTOCOL;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_TRUSTSTORE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_TRUSTSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_TRUSTSTORE_TYPE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_USER;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

public class CommandArgParser {
    private final CommandLogger logger;

    public CommandArgParser(CommandLogger logger) {
        this.logger = logger;
    }

    /**
     * Parses and validates arguments.
     *
     * @param argIter Iterator of arguments.
     * @return Arguments bean.
     * @throws IllegalArgumentException In case arguments aren't valid.
     */
    ConnectionAndSslParameters parseAndValidate(CommandArgIterator argIter) {
        String host = DFLT_HOST;

        String port = DFLT_PORT;

        String user = null;

        String pwd = null;

        Long pingInterval = DFLT_PING_INTERVAL;

        Long pingTimeout = DFLT_PING_TIMEOUT;

        boolean autoConfirmation = false;

        List<Commands> commands = new ArrayList<>();

        String sslProtocol = DFLT_SSL_PROTOCOL;

        String sslCipherSuites = "";

        String sslKeyAlgorithm = SslContextFactory.DFLT_KEY_ALGORITHM;

        String sslKeyStoreType = SslContextFactory.DFLT_STORE_TYPE;

        String sslKeyStorePath = null;

        char sslKeyStorePassword[] = null;

        String sslTrustStoreType = SslContextFactory.DFLT_STORE_TYPE;

        String sslTrustStorePath = null;

        char sslTrustStorePassword[] = null;

        final String pwdArgWarnFmt = "Warning: %s is insecure. " +
            "Whenever possible, use interactive prompt for password (just discard %s option).";

        while (argIter.hasNextArg()) {
            String str = argIter.nextArg("").toLowerCase();

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
                    pingInterval = argIter.nextLongArg("ping interval");

                    break;

                case CMD_PING_TIMEOUT:
                    pingTimeout = argIter.nextLongArg("ping timeout");

                    break;

                case CMD_USER:
                    user = argIter.nextArg("Expected user name");

                    break;

                case CMD_PASSWORD:
                    pwd = argIter.nextArg("Expected password");

                    logger.log(String.format(pwdArgWarnFmt, CMD_PASSWORD, CMD_PASSWORD));

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

                    logger.log(String.format(pwdArgWarnFmt, CMD_KEYSTORE_PASSWORD, CMD_KEYSTORE_PASSWORD));

                    break;

                case CMD_KEYSTORE_TYPE:
                    sslKeyStoreType = argIter.nextArg("Expected SSL key store type");

                    break;

                case CMD_TRUSTSTORE:
                    sslTrustStorePath = argIter.nextArg("Expected SSL trust store path");

                    break;

                case CMD_TRUSTSTORE_PASSWORD:
                    sslTrustStorePassword = argIter.nextArg("Expected SSL trust store password").toCharArray();

                    logger.log(String.format(pwdArgWarnFmt, CMD_TRUSTSTORE_PASSWORD, CMD_TRUSTSTORE_PASSWORD));

                    break;

                case CMD_TRUSTSTORE_TYPE:
                    sslTrustStoreType = argIter.nextArg("Expected SSL trust store type");

                    break;

                case CMD_AUTO_CONFIRMATION:
                    autoConfirmation = true;

                    break;
            }
        }

        int sz = commands.size();

        if (sz < 1)
            throw new IllegalArgumentException("No action was specified");

        if (sz > 1)
            throw new IllegalArgumentException("Only one action can be specified, but found: " + sz);

        return new ConnectionAndSslParameters(host, port, user, pwd,
            pingTimeout, pingInterval, autoConfirmation,
            sslProtocol, sslCipherSuites,
            sslKeyAlgorithm, sslKeyStorePath, sslKeyStorePassword, sslKeyStoreType,
            sslTrustStorePath, sslTrustStorePassword, sslTrustStoreType);
    }
}
