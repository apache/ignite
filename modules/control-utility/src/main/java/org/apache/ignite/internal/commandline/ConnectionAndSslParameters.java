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

import java.util.Deque;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsRegistry;

import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_HOST;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_KEYSTORE;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_KEYSTORE_TYPE;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_PORT;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_SSL_CIPHER_SUITES;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_SSL_FACTORY;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_SSL_KEY_ALGORITHM;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_SSL_PROTOCOL;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_TRUSTSTORE;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_TRUSTSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_TRUSTSTORE_TYPE;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_USER;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_VERBOSE;

/**
 * Container with common parsed and validated arguments.
 */
public class ConnectionAndSslParameters<A extends IgniteDataTransferObject> {
    /** User. */
    private String user;

    /** Password. */
    private String pwd;

    /** Keystore Password. */
    private char[] sslKeyStorePassword;

    /** Truststore Password. */
    private char[] sslTrustStorePassword;

    /** Command to execute. */
    private final Deque<Command<?, ?>> cmdPath;

    /** Command argument. */
    private final A arg;

    /** */
    private final CLIArgumentParser parser;

    /**String builder for logging arguments with hidden confidential values*/
    private final String argumentsToString;

    /**
     * @param cmdPath Path to the command in {@link CommandsRegistry} hierarchy.
     * @param arg Command argument.
     * @param parser CLI arguments parser.
     */
    public ConnectionAndSslParameters(
        Deque<Command<?, ?>> cmdPath,
        A arg,
        CLIArgumentParser parser,
        String argumentsToString
    ) {
        this.cmdPath = cmdPath;
        this.arg = arg;
        this.parser = parser;
        this.argumentsToString = argumentsToString;

        this.user = parser.get(CMD_USER);
        this.pwd = parser.get(CMD_PASSWORD);
        this.sslKeyStorePassword = parser.get(CMD_KEYSTORE_PASSWORD);
        this.sslTrustStorePassword = parser.get(CMD_TRUSTSTORE_PASSWORD);
    }

    /**
     * @return High-level command which were defined by user to run.
     */
    public Command<A, ?> command() {
        return (Command<A, ?>)cmdPath.peek();
    }

    /**
     * @return High-level command which were defined by user to run.
     */
    public Deque<Command<?, ?>> cmdPath() {
        return cmdPath;
    }

    /** */
    public A commandArg() {
        return arg;
    }

    /**
     * @return host name
     */
    public String host() {
        return parser.get(CMD_HOST);
    }

    /**
     * @return port number
     */
    public int port() {
        return parser.get(CMD_PORT);
    }

    /**
     * @return user name
     */
    public String userName() {
        return user;
    }

    /**
     * @param user New user name.
     */
    public void userName(String user) {
        this.user = user;
    }

    /**
     * @return password
     */
    public String password() {
        return pwd;
    }

    /**
     * @param pwd New password.
     */
    public void password(String pwd) {
        this.pwd = pwd;
    }

    /**
     * @return Auto confirmation option.
     */
    public boolean autoConfirmation() {
        return parser.get(CMD_AUTO_CONFIRMATION);
    }

    /**
     * @return SSL protocol
     */
    public String[] sslProtocol() {
        return parser.get(CMD_SSL_PROTOCOL);
    }

    /**
     * @return SSL cipher suites.
     */
    public String[] getSslCipherSuites() {
        return parser.get(CMD_SSL_CIPHER_SUITES);
    }

    /**
     * @return SSL Key Algorithm
     */
    public String sslKeyAlgorithm() {
        return parser.get(CMD_SSL_KEY_ALGORITHM);
    }

    /**
     * @return Keystore
     */
    public String sslKeyStorePath() {
        return parser.get(CMD_KEYSTORE);
    }

    /**
     * @return Keystore type
     */
    public String sslKeyStoreType() {
        return parser.get(CMD_KEYSTORE_TYPE);
    }

    /**
     * @return Keystore password
     */
    public char[] sslKeyStorePassword() {
        return sslKeyStorePassword;
    }

    /**
     * Set keystore password.
     *
     * @param sslKeyStorePassword Keystore password.
     */
    public void sslKeyStorePassword(char[] sslKeyStorePassword) {
        this.sslKeyStorePassword = sslKeyStorePassword;
    }

    /**
     * @return Truststore
     */
    public String sslTrustStorePath() {
        return parser.get(CMD_TRUSTSTORE);
    }

    /**
     * @return Truststore type
     */
    public String sslTrustStoreType() {
        return parser.get(CMD_TRUSTSTORE_TYPE);
    }

    /**
     * @return Truststore password
     */
    public char[] sslTrustStorePassword() {
        return sslTrustStorePassword;
    }

    /**
     * Set truststore password.
     *
     * @param sslTrustStorePassword Truststore password.
     */
    public void sslTrustStorePassword(char[] sslTrustStorePassword) {
        this.sslTrustStorePassword = sslTrustStorePassword;
    }

    /**
     * @return Predefined SSL Factory config.
     */
    public String sslFactoryConfigPath() {
        return parser.get(CMD_SSL_FACTORY);
    }

    /**
     * Returns {@code true} if verbose mode is enabled.
     *
     * @return {@code True} if verbose mode is enabled.
     */
    public boolean verbose() {
        return parser.get(CMD_VERBOSE);
    }

    /**
     * Return sting of arguments for logging with hidden confidential values
     *
     * @return arguments to string
     */
    public String getArgumentsToString() {
        return argumentsToString;
    }
}
