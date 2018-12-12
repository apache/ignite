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
import org.apache.ignite.internal.commandline.cache.CacheArguments;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;

/**
 * Bean with all parsed and validated arguments.
 */
public class Arguments {
    /** Command. */
    private Command cmd;

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

    /**
     * Action for baseline command.
     */
    private String baselineAct;

    /**
     * Arguments for baseline command.
     */
    private String baselineArgs;

    /** Transaction arguments. */
    private final VisorTxTaskArg txArg;

    /**
     * Arguments for --cache subcommand.
     */
    private CacheArguments cacheArgs;

    /**
     * Action for WAL command.
     */
    private String walAct;

    /**
     * Arguments for WAL command.
     */
    private String walArgs;

    /** Ping timeout for grid client. See {@link GridClientConfiguration#pingTimeout}. */
    private long pingTimeout;

    /** Ping interval for grid client. See {@link GridClientConfiguration#pingInterval}. */
    private long pingInterval;

    /** */
    private boolean sslEnable;

    /** */
    private String sslProtocol;

    /** */
    private String sslAlgorithm;

    /** */
    private String sslCipherSuites;

    /** */
    private String sslKeyStorePath;

    /** */
    private String sslKeyStoreType;

    /** */
    private char sslKeyStorePassword[];

    /** */
    private String sslTrustStorePath;

    /** */
    private String sslTrustStoreType;

    /** */
    private char sslTrustStorePassword[];

    /**
     * @param cmd Command.
     * @param host Host.
     * @param port Port.
     * @param user User.
     * @param pwd Password.
     * @param baselineAct Baseline action.
     * @param baselineArgs Baseline args.
     * @param txArg TX arg.
     * @param cacheArgs --cache subcommand arguments.
     * @param walAct WAL action.
     * @param walArgs WAL args.
     * @param pingTimeout Ping timeout. See {@link GridClientConfiguration#pingTimeout}.
     * @param pingInterval Ping interval. See {@link GridClientConfiguration#pingInterval}.
     * @param autoConfirmation Auto confirmation flag.
     * @param sslEnable SSL flag.
     * @param sslProtocol SSL protocol.
     * @param sslAlgorithm SSL algorithm.
     * @param sslCipherSuites SSL cipher suites.
     * @param sslKeyStorePath Key store path.
     * @param sslKeyStoreType Key store type.
     * @param sslKeyStorePassword Key store password.
     * @param sslTrustStorePath Trust store path.
     * @param sslTrustStoreType Trust store type.
     * @param sslTrustStorePassword Trust store password.
     */
    public Arguments(Command cmd, String host, String port, String user, String pwd,
        String baselineAct, String baselineArgs,
        VisorTxTaskArg txArg, CacheArguments cacheArgs, String walAct, String walArgs,
        Long pingTimeout, Long pingInterval, boolean autoConfirmation,
        boolean sslEnable, String sslProtocol, String sslAlgorithm, String sslCipherSuites,
        String sslKeyStorePath, String sslKeyStoreType, char sslKeyStorePassword[],
        String sslTrustStorePath, String sslTrustStoreType, char sslTrustStorePassword[]
    ) {
        this.cmd = cmd;
        this.host = host;
        this.port = port;
        this.user = user;
        this.pwd = pwd;

        this.baselineAct = baselineAct;
        this.baselineArgs = baselineArgs;

        this.txArg = txArg;
        this.cacheArgs = cacheArgs;

        this.walAct = walAct;
        this.walArgs = walArgs;

        this.pingTimeout = pingTimeout;
        this.pingInterval = pingInterval;

        this.autoConfirmation = autoConfirmation;

        this.sslEnable = sslEnable;
        this.sslProtocol = sslProtocol;
        this.sslAlgorithm = sslAlgorithm;
        this.sslCipherSuites = sslCipherSuites;

        this.sslKeyStorePath = sslKeyStorePath;
        this.sslKeyStoreType = sslKeyStoreType;
        this.sslKeyStorePassword = sslKeyStorePassword;

        this.sslTrustStorePath = sslTrustStorePath;
        this.sslTrustStoreType = sslTrustStoreType;
        this.sslTrustStorePassword = sslTrustStorePassword;
    }

    /**
     * @return command
     */
    public Command command() {
        return cmd;
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
    public String getUserName() {
        return user;
    }

    /**
     * @param user New user name.
     */
    public void setUserName(String user) {
        this.user = user;
    }

    /**
     * @return password
     */
    public String getPassword() {
        return pwd;
    }

    /**
     * @param pwd New password.
     */
    public void setPassword(String pwd) {
        this.pwd = pwd;
    }

    /**
     * @return baseline action
     */
    public String baselineAction() {
        return baselineAct;
    }

    /**
     * @return baseline arguments
     */
    public String baselineArguments() {
        return baselineArgs;
    }

    /**
     * @return Transaction arguments.
     */
    public VisorTxTaskArg transactionArguments() {
        return txArg;
    }

    /**
     * @return Arguments for --cache subcommand.
     */
    public CacheArguments cacheArgs() {
        return cacheArgs;
    }

    /**
     * @return WAL action.
     */
    public String walAction() {
        return walAct;
    }

    /**
     * @return WAL arguments.
     */
    public String walArguments() {
        return walArgs;
    }

    /**
     * See {@link GridClientConfiguration#pingTimeout}.
     *
     * @return Ping timeout.
     */
    public long pingTimeout() {
        return pingTimeout;
    }

    /**
     * See {@link GridClientConfiguration#pingInterval}.
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

    /** */
    public boolean isSslEnable() {
        return sslEnable;
    }

    /** */
    public String getSslProtocol() {
        return sslProtocol;
    }

    /** */
    public String getSslAlgorithm() {
        return sslAlgorithm;
    }

    /** */
    public String getSslCipherSuites() {
        return sslCipherSuites;
    }

    /** */
    public String getSslKeyStorePath() {
        return sslKeyStorePath;
    }

    /** */
    public String getSslKeyStoreType() {
        return sslKeyStoreType;
    }

    /** */
    public char[] getSslKeyStorePassword() {
        return sslKeyStorePassword;
    }

    /** */
    public String getSslTrustStorePath() {
        return sslTrustStorePath;
    }

    /** */
    public String getSslTrustStoreType() {
        return sslTrustStoreType;
    }

    /** */
    public char[] getSslTrustStorePassword() {
        return sslTrustStorePassword;
    }
}
