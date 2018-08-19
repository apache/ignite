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

    /** Use SSL. */
    private boolean ssl;

    /** SSL Protocol. */
    private String sslProtocol;

    /** SSL Key Algorithm. */
    private String sslKeyAlgorithm;

    /** Keystore. */
    private String keystore;

    /** Keystore Password. */
    private String keystorePassword;

    /** Keystore Type. */
    private String keystoreType;

    /** Truststore. */
    private String truststore;

    /** Truststore Password. */
    private String truststorePassword;
    
    /** Truststore Type. */
    private String truststoreType;

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

    /** Ping timeout for grid client. See {@link GridClientConfiguration#pingTimeout}.*/
    private long pingTimeout;

    private long pingInterval;

    /**
     * @param cmd Command.
     * @param host Host.
     * @param port Port.
     * @param user User.
     * @param pwd Password.
     * @param ssl Use SSL.
     * @param sslProtocol SSL Protocol.
     * @param sslKeyAlgorithm SSL Key Algorithm.
     * @param keystore Keystore.
     * @param keystorePassword Keystore Password.
     * @param keystoreType Keystore Type.
     * @param truststore Truststore.
     * @param truststorePassword Truststore Password.
     * @param truststoreType Truststore Type.
     * @param baselineAct Baseline action.
     * @param baselineArgs Baseline args.
     * @param txArg TX arg.
     * @param cacheArgs --cache subcommand arguments.
     * @param walAct WAL action.
     * @param walArgs WAL args.
     * @param pingTimeout Ping timeout. See {@link GridClientConfiguration#pingTimeout}.
     * @param pingInterval Ping interval. See {@link GridClientConfiguration#pingInterval}.
     * @param autoConfirmation Auto confirmation flag.
     */
    public Arguments(Command cmd, String host, String port, String user, String pwd,
    	             boolean ssl, String sslProtocol, String sslKeyAlgorithm, String keystore, 
                     String keystorePassword, String keystoreType, String truststore, String truststorePassword, 
                     String truststoreType, String baselineAct, String baselineArgs, VisorTxTaskArg txArg, CacheArguments cacheArgs,
                     String walAct, String walArgs, Long pingTimeout, Long pingInterval, boolean autoConfirmation) {
        this.cmd = cmd;
        this.host = host;
        this.port = port;
        this.user = user;
        this.pwd = pwd;
        this.ssl = ssl;
        this.sslProtocol = sslProtocol;
        this.sslKeyAlgorithm = sslKeyAlgorithm;
        this.keystore = keystore;
        this.keystorePassword = keystorePassword;
        this.keystoreType = keystoreType;
        this.truststore = truststore;
        this.truststorePassword = truststorePassword;
        this.truststoreType = truststoreType;
        this.baselineAct = baselineAct;
        this.baselineArgs = baselineArgs;
        this.txArg = txArg;
        this.cacheArgs = cacheArgs;
        this.walAct = walAct;
        this.walArgs = walArgs;
        this.pingTimeout = pingTimeout;
        this.pingInterval = pingInterval;
        this.autoConfirmation = autoConfirmation;
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
     * @return ssl
     */
    public boolean ssl() {
        return ssl;
    }   
    
    /**
     * @return SSL protocol
     */
    public String sslProtocol() {
        return sslProtocol;
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
    public String keystore() {
        return keystore;
    }   
    
    /**
     * @return Keystore password
     */
    public String keystorePassword() {
        return keystorePassword;
    }   
    
    /**
     * @return Keystore type
     */
    public String keystoreType() {
        return keystoreType;
    }   
    
    /**
     * @return Truststore
     */
    public String truststore() {
        return truststore;
    }   
    
    /**
     * @return Truststore password
     */
    public String truststorePassword() {
        return truststorePassword;
    }   
    
    /**
     * @return Truststore type
     */
    public String truststoreType() {
        return truststoreType;
    }   

    /**
     * @return user name
     */
    public String user() {
        return user;
    }

    /**
     * @return password
     */
    public String password() {
        return pwd;
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
}
