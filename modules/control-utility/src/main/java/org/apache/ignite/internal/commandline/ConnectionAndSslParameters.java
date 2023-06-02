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

import java.util.Map;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_HOST;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_KEYSTORE;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_KEYSTORE_TYPE;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PING_INTERVAL;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PORT;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_SSL_CIPHER_SUITES;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_SSL_FACTORY;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_SSL_KEY_ALGORITHM;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_SSL_PROTOCOL;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_TRUSTSTORE;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_TRUSTSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_TRUSTSTORE_TYPE;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_USER;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_VERBOSE;

/**
 * Container with common parsed and validated arguments.
 */
public class ConnectionAndSslParameters {
    /** User. */
    private String user;

    /** Password. */
    private String pwd;

    /** Keystore Password. */
    private char[] sslKeyStorePassword;

    /** Truststore Password. */
    private char[] sslTrustStorePassword;

    /** High-level command. */
    private final DeclarativeCommandAdapter<?> command;

    /** */
    private final Map<String, CLIArgument<?>> args;

    /** */
    private final Map<String, Object> vals;

    /**
     * @param command Command.
     */
    public ConnectionAndSslParameters(
        DeclarativeCommandAdapter command,
        Map<String, CLIArgument<?>> args,
        Map<String, Object> vals
    ) {
        this.command = command;
        this.args = args;
        this.vals = vals;

        this.user = val(vals, CMD_USER);
        this.pwd = val(vals, CMD_PASSWORD);
        this.sslKeyStorePassword = val(vals, CMD_KEYSTORE_PASSWORD);
        this.sslTrustStorePassword = val(vals, CMD_TRUSTSTORE_PASSWORD);
    }

    /** */
    private <T> T val(Map<String, Object> vals, String name) {
        return (T)vals.computeIfAbsent(name, key -> args.get(name).defaultValueSupplier().apply(null));
    }

    /**
     * @return High-level command which were defined by user to run.
     */
    public DeclarativeCommandAdapter<?> command() {
        return command;
    }

    /**
     * @return host name
     */
    public String host() {
        return val(vals, CMD_HOST);
    }

    /**
     * @return port number
     */
    public int port() {
        return val(vals, CMD_PORT);
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
     * See {@link GridClientConfiguration#getPingInterval()}.
     *
     * @return Ping timeout.
     */
    public long pingTimeout() {
        return val(vals, CMD_PING_TIMEOUT);
    }

    /**
     * See {@link GridClientConfiguration#getPingInterval()}.
     *
     * @return Ping interval.
     */
    public long pingInterval() {
        return val(vals, CMD_PING_INTERVAL);
    }

    /**
     * @return Auto confirmation option.
     */
    public boolean autoConfirmation() {
        return val(vals, CMD_AUTO_CONFIRMATION);
    }

    /**
     * @return SSL protocol
     */
    public String sslProtocol() {
        return val(vals, CMD_SSL_PROTOCOL);
    }

    /**
     * @return SSL cipher suites.
     */
    public String getSslCipherSuites() {
        return val(vals, CMD_SSL_CIPHER_SUITES);
    }

    /**
     * @return SSL Key Algorithm
     */
    public String sslKeyAlgorithm() {
        return val(vals, CMD_SSL_KEY_ALGORITHM);
    }

    /**
     * @return Keystore
     */
    public String sslKeyStorePath() {
        return val(vals, CMD_KEYSTORE);
    }

    /**
     * @return Keystore type
     */
    public String sslKeyStoreType() {
        return val(vals, CMD_KEYSTORE_TYPE);
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
        return val(vals, CMD_TRUSTSTORE);
    }

    /**
     * @return Truststore type
     */
    public String sslTrustStoreType() {
        return val(vals, CMD_TRUSTSTORE_TYPE);
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
        return val(vals, CMD_SSL_FACTORY);
    }

    /**
     * Returns {@code true} if verbose mode is enabled.
     *
     * @return {@code True} if verbose mode is enabled.
     */
    public boolean verbose() {
        return val(vals, CMD_VERBOSE);
    }
}
