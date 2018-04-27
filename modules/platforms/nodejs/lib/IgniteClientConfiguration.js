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
 *   - (optional) networking and connection settings
 *   - (optional) TLS settings
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
        this._tcpNoDelay = true;
        this._timeout = 0;
        this._tlsConfiguration = null;
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

    setTcpNoDelay(tcpNoDelay) {
        this._tcpNoDelay = tcpNoDelay;
        return this;
    }

    setTimeout(timeout) {
        this._timeout = timeout;
        return this;
    }

    /**
     * Sets TLS configuration which will be used to establish a secure connection.
     *
     * If TLS configuration is not set, secure connection is not used.
      *
     * @param {TlsConfiguration} tlsConfiguration - TLS configuration.
     *   If null, secure connection is disabled.
     *
     * @return {IgniteClientConfiguration} - the same instance of the IgniteClientConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setTlsConfiguration(tlsConfiguration) {
        ArgumentChecker.hasType(tlsConfiguration, 'tlsConfiguration', false, TlsConfiguration);
        this._tlsConfiguration = tlsConfiguration;
        return this;
    }
}

/**
 * TLS Protocol versions.
 * @typedef TlsConfiguration.PROTOCOL
 * @enum
 * @readonly
 * @property TLS       Supports multiple versions of TLS.
 * @property TLS_V1    Supports RFC 2246: TLS version 1.0.
 * @property TLS_V1_1  Supports RFC 4346: TLS version 1.1.
 * @property TLS_V1_2  Supports RFC 5246: TLS version 1.2.
 */
const PROTOCOL = Object.freeze({
    TLS : 'SSLv23',
    TLS_V1 : 'TLSv1',
    TLS_V1_1 : 'TLSv1_1',
    TLS_V1_2 : 'TLSv1_2'
});

/**
 * Class representing TLS configuration.
 *
 * The configuration includes:
 *   - (mandatory) TLS protocol version
 *   - (optional) certificate settings
 */
class TlsConfiguration {

    /**
     * Creates an instance of Ignite client TlsConfiguration. ???
     *
     * @param {TlsConfiguration.PROTOCOL} [protocol] - TLS protocol version,
     *     one of the {@link TlsConfiguration.PROTOCOL} values. ???
     *
     * @return {TlsConfiguration} - new TLS configuration instance.
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(protocol = TlsConfiguration.PROTOCOL.TLS) {
        this._protocol = protocol;
        this._key = null;
        this._cert = null;
        this._ca = null;
        this._keyPassword = null;
        this._trustAll = false;
    }

    static get PROTOCOL() {
        return PROTOCOL;
    }

    /**
     * Sets TLS key file path.
     *
     * @param {string} keyFile - path to TLS key file in PEM format.
     *
     * @return {TlsConfiguration} - the same instance of the TlsConfiguration.
     *
     * @throws {IgniteClientError} if the file doesn't exist.
     */
    setKeyFile(keyFile) {
        ArgumentChecker.notEmpty(keyFile, 'keyFile');
        this._key = this._readFile(keyFile);
        return this;
    }

    /**
     * Sets TLS certificate file path.
     *
     * @param {string} certFile - path to TLS certificate file in PEM format.
     *
     * @return {TlsConfiguration} - the same instance of the TlsConfiguration.
     *
     * @throws {IgniteClientError} if the file doesn't exist.
     */
    setCertFile(certFile) {
        ArgumentChecker.notEmpty(certFile, 'certFile');
        this._cert = this._readFile(certFile);
        return this;
    }

    /**
     * Sets TLS certificate authority file path.
     *
     * @param {string} caFile - TLS certificate authority file path.
     *
     * @return {TlsConfiguration} - the same instance of the TlsConfiguration.
     *
     * @throws {IgniteClientError} if the file doesn't exist.
     */
    setCaFile(caFile) {
        ArgumentChecker.notEmpty(caFile, 'caFile');
        this._ca = this._readFile(caFile);
        return this;
    }

    /**
     * Sets TLS key file password.
     *
     * @param {string} keyPassword - TLS key file password.
     *
     * @return {TlsConfiguration} - the same instance of the TlsConfiguration.
     */
    setKeyPassword(keyPassword) {
        this._keyPassword = keyPassword;
        return this;
    }

    /**
     * Sets to true to trust any server certificate (revoked, expired or self-signed TLS certificates).
     * This may be useful for testing with self-signed certificates.
     * Default is false.
     *
     * @param {boolean} trustAll - trust all certificates flag.
     *
     * @return {TlsConfiguration} - the same instance of the TlsConfiguration.
     */
    setTrustAll(trustAll) {
        this._trustAll = trustAll;
        return this;
    }

    /** Private methods */

    /**
     * @ignore
     */
    _readFile(fileName) {
        try {
            return FS.readFileSync(fileName);
        } catch (error) {
            throw Errors.IgniteClientError.illegalArgumentError(Util.format('File error: %s', error.message));
        }
        return null;
    }
}

module.exports = IgniteClientConfiguration;
module.exports.TlsConfiguration = TlsConfiguration;
