/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const FS = require('fs');
const Util = require('util');
const Errors = require('./Errors');
const ArgumentChecker = require('./internal/ArgumentChecker');

/**
 * Class representing Ignite client configuration.
 *
 * The configuration includes:
 *   - (mandatory) Ignite node endpoint(s)
 *   - (optional) user credentials for authentication
 *   - (optional) TLS enabling
 *   - (optional) connection options
 */
class IgniteClientConfiguration {

    /**
     * Creates an instance of Ignite client configuration
     * with the provided mandatory settings and default optional settings.
     *
     * By default, the client does not use authentication and secure connection.
     *
     * @param {...string} endpoints - Ignite node endpoint(s).
     *  The client randomly connects/reconnects to one of the specified node.
     *
     * @return {IgniteClientConfiguration} - new client configuration instance.
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(...endpoints) {
        ArgumentChecker.notEmpty(endpoints, 'endpoints');
        this._endpoints = endpoints;
        this._userName = null;
        this._password = null;
        this._useTLS = false;
        this._options = null;
    }


    /**
     * Sets username which will be used for authentication during the client's connection.
     *
     * If username is not set, the client does not use authentication during connection.
     *
     * @param {string} userName - username. If null, authentication is disabled.
     *
     * @return {IgniteClientConfiguration} - the same instance of the IgniteClientConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setUserName(userName) {
        this._userName = userName;
        return this;
    }

    /**
     * Sets password which will be used for authentication during the client's connection.
     *
     * Password is ignored, if username is not set.
     * If password is not set, it is considered empty.
     *
     * @param {string} password - password. If null, password is empty.
     *
     * @return {IgniteClientConfiguration} - the same instance of the IgniteClientConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setPassword(password) {
        this._password = password;
        return this;
    }

    /**
     * Sets connection options.
     *
     * By default the client establishes a non-secure connection with default connection options defined by nodejs.
     *
     * @param {boolean} useTLS - if true, secure connection will be established;
     *                           if false, non-secure connection will be established.
     * @param {object} [connectionOptions=null] - connection options.
     *   - For non-secure connection options defined here {@link https://nodejs.org/api/net.html#net_net_createconnection_options_connectlistener}
     *   - For secure connection options defined here {@link https://nodejs.org/api/tls.html#tls_tls_connect_options_callback}
     *
     * @return {IgniteClientConfiguration} - the same instance of the IgniteClientConfiguration.
     */
    setConnectionOptions(useTLS, connectionOptions = null) {
        this._useTLS = useTLS;
        this._options = connectionOptions;
        return this;
    }
}

module.exports = IgniteClientConfiguration;
