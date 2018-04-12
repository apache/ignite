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
 *   - (optional or mandatory - TBD) user credentials for authentication
 *   - (optional) networking and connection settings - TBD 
 */
class IgniteClientConfiguration {

    /**
     * Creates an instance of Ignite client configuration
     * with the provided mandatory settings and default optional settings.
     *
     * @param {...string} endpoints - Ignite node endpoint(s).
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
        this._sslConfiguration = null;
    }

    /* Optional configuration settings - TBD */

    setUserName(userName) {
        this._userName = userName;
        return this;
    }

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

    setSslConfiguration(sslConfiguration) {
        ArgumentChecker.hasType(sslConfiguration, 'sslConfiguration', false, SslConfiguration);
        this._sslConfiguration = sslConfiguration;
        return this;
    }
}

/**
 * SSL Protocol versions.
 * @typedef SslConfiguration.PROTOCOL
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
 * ???
 */
class SslConfiguration {

    /**
     * Creates an instance of Ignite client SslConfiguration. ???
     *
     * @return {SslConfiguration} - new SSL configuration instance.
     *
     * @throws {IgniteClientError} if error.
     */
    constructor() {
        this._protocol = SslConfiguration.PROTOCOL.TLS;
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
     * Sets SSL protocol version.
     *
     * @param {SslConfiguration.PROTOCOL} protocol - SSL protocol version, one of the {@link SslConfiguration.PROTOCOL} values.
     *
     * @return {SslConfiguration} - the same instance of the SslConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setProtocol(protocol) {
        ArgumentChecker.hasValueFrom(protocol, 'protocol', false, SslConfiguration.PROTOCOL);
        this._protocol = protocol;
        return this;
    }

    /**
     * Sets SSL key file path.
     *
     * @param {string} keyFile - path to SSL key file in PEM format.
     *
     * @return {SslConfiguration} - the same instance of the SslConfiguration.
     *
     * @throws {IgniteClientError} if the file doesn't exist.
     */
    setKeyFile(keyFile) {
        ArgumentChecker.notEmpty(keyFile, 'keyFile');
        this._key = this._readFile(keyFile);
        return this;
    }

    /**
     * Sets SSL certificate file path.
     *
     * @param {string} certFile - path to SSL certificate file in PEM format.
     *
     * @return {SslConfiguration} - the same instance of the SslConfiguration.
     *
     * @throws {IgniteClientError} if the file doesn't exist.
     */
    setCertFile(certFile) {
        ArgumentChecker.notEmpty(certFile, 'certFile');
        this._cert = this._readFile(certFile);
        return this;
    }

    /**
     * Sets SSL certificate authority file path.
     *
     * @param {string} caFile - SSL certificate authority file path.
     *
     * @return {SslConfiguration} - the same instance of the SslConfiguration.
     *
     * @throws {IgniteClientError} if the file doesn't exist.
     */
    setCaFile(caFile) {
        ArgumentChecker.notEmpty(caFile, 'caFile');
        this._ca = this._readFile(caFile);
        return this;
    }

    /**
     * Sets SSL key file password.
     *
     * @param {string} keyPassword - SSL key file password.
     *
     * @return {SslConfiguration} - the same instance of the SslConfiguration.
     */
    setKeyPassword(keyPassword) {
        this._keyPassword = keyPassword;
        return this;
    }

    /**
     * Sets to true to trust any server certificate (revoked, expired or self-signed SSL certificates).
     * This may be useful for testing with self-signed certificates.
     * Default is false.
     *
     * @param {boolean} trustAll - trust all certificates flag.
     *
     * @return {SslConfiguration} - the same instance of the SslConfiguration.
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
module.exports.SslConfiguration = SslConfiguration;
