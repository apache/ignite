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

import org.apache.ignite.internal.client.GridClientConfiguration;

/**
 * Container with common parsed and validated arguments.
 */
public class ConnectionAndSslParameters {
    /** Host. */
    private String host;

    /** Port. */
    private String port;

    /** User. */
    private String user;

    /** Password. */
    private String pwd;

    /** Force option is used for auto confirmation. */
    private boolean autoConfirmation;

    /** Ping timeout for grid client. See {@link GridClientConfiguration#getPingTimeout()}. */
    private long pingTimeout;

    /** Ping interval for grid client. See {@link GridClientConfiguration#getPingInterval()}. */
    private long pingInterval;

    /** Verbose mode. */
    private boolean verbose;

    /** SSL Protocol. */
    private String sslProtocol;

    /** SSL Cipher suites. */
    private String sslCipherSuites;

    /** SSL Key Algorithm. */
    private String sslKeyAlgorithm;

    /** Keystore. */
    private String sslKeyStorePath;

    /** Keystore Type. */
    private String sslKeyStoreType;

    /** Keystore Password. */
    private char[] sslKeyStorePassword;

    /** Truststore. */
    private String sslTrustStorePath;

    /** Truststore Type. */
    private String sslTrustStoreType;

    /** Truststore Password. */
    private char[] sslTrustStorePassword;

    /** High-level command. */
    private Command command;

    /**
     * @param command Command.
     * @param host Host.
     * @param port Port.
     * @param user User.
     * @param pwd Password.
     * @param pingTimeout Ping timeout. See {@link GridClientConfiguration#getPingTimeout()}.
     * @param pingInterval Ping interval. See {@link GridClientConfiguration#getPingInterval()}.
     * @param autoConfirmation Auto confirmation flag.
     * @param verbose Verbose mode.
     * @param sslProtocol SSL Protocol.
     * @param sslCipherSuites SSL cipher suites.
     * @param sslKeyAlgorithm SSL Key Algorithm.
     * @param sslKeyStorePath Keystore.
     * @param sslKeyStorePassword Keystore Password.
     * @param sslKeyStoreType Keystore Type.
     * @param sslTrustStorePath Truststore.
     * @param sslTrustStorePassword Truststore Password.
     * @param sslTrustStoreType Truststore Type.
     */
    public ConnectionAndSslParameters(Command command, String host, String port, String user, String pwd,
        Long pingTimeout, Long pingInterval, boolean autoConfirmation, boolean verbose,
        String sslProtocol, String sslCipherSuites, String sslKeyAlgorithm,
        String sslKeyStorePath, char[] sslKeyStorePassword, String sslKeyStoreType,
        String sslTrustStorePath, char[] sslTrustStorePassword, String sslTrustStoreType
    ) {
        this.command = command;
        this.host = host;
        this.port = port;
        this.user = user;
        this.pwd = pwd;

        this.pingTimeout = pingTimeout;
        this.pingInterval = pingInterval;

        this.autoConfirmation = autoConfirmation;
        this.verbose = verbose;

        this.sslProtocol = sslProtocol;
        this.sslCipherSuites = sslCipherSuites;

        this.sslKeyAlgorithm = sslKeyAlgorithm;
        this.sslKeyStorePath = sslKeyStorePath;
        this.sslKeyStoreType = sslKeyStoreType;
        this.sslKeyStorePassword = sslKeyStorePassword;

        this.sslTrustStorePath = sslTrustStorePath;
        this.sslTrustStoreType = sslTrustStoreType;
        this.sslTrustStorePassword = sslTrustStorePassword;
    }

    /**
     * @return High-level command which were defined by user to run.
     */
    public Command command() {
        return command;
    }

    /**
     * @return host name
     */
    public String host() {
        return host;
    }

    /**
     * @return port number
     */
    public String port() {
        return port;
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
        return pingTimeout;
    }

    /**
     * See {@link GridClientConfiguration#getPingInterval()}.
     *
     * @return Ping interval.
     */
    public long pingInterval() {
        return pingInterval;
    }

    /**
     * @return Auto confirmation option.
     */
    public boolean autoConfirmation() {
        return autoConfirmation;
    }

    /**
     * @return SSL protocol
     */
    public String sslProtocol() {
        return sslProtocol;
    }

    /**
     * @return SSL cipher suites.
     */
    public String getSslCipherSuites() {
        return sslCipherSuites;
    }

    /**
     * @return SSL Key Algorithm
     */
    public String sslKeyAlgorithm() {
        return sslKeyAlgorithm;
    }

    /**
     * @return Keystore
     */
    public String sslKeyStorePath() {
        return sslKeyStorePath;
    }

    /**
     * @return Keystore type
     */
    public String sslKeyStoreType() {
        return sslKeyStoreType;
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
        return sslTrustStorePath;
    }

    /**
     * @return Truststore type
     */
    public String sslTrustStoreType() {
        return sslTrustStoreType;
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
     * Returns {@code true} if verbose mode is enabled.
     *
     * @return {@code True} if verbose mode is enabled.
     */
    public boolean verbose() {
        return verbose;
    }
}
