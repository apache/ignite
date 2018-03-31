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
    }

    get endpoints() {
        return this._endpoints;
    }

    get userName() {
        return this._userName;
    }

    get password() {
        return this._password;
    }

    get tcpNoDelay() {
        return this._tcpNoDelay;
    }

    get timeout() {
        return this._timeout;
    }
}

module.exports = IgniteClientConfiguration;
