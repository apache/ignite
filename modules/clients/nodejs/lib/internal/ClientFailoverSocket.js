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

const Util = require('util');
const Errors = require('../Errors');
const IgniteClient = require('../IgniteClient');
const ClientSocket = require('./ClientSocket');
const Logger = require('./Logger');

/** Socket wrapper with failover functionality: reconnects on failure. */
class ClientFailoverSocket {

    constructor(onStateChanged) {
        this._socket = null;
        this._state = IgniteClient.STATE.DISCONNECTED;
        this._onStateChanged = onStateChanged;
    }

    async connect(config) {
        if (this._state !== IgniteClient.STATE.DISCONNECTED) {
            throw new Errors.IllegalStateError();
        }
        this._config = config;
        this._socket = new ClientSocket(this._config.endpoints[0], this._onSocketDisconnect.bind(this));
        this._changeState(IgniteClient.STATE.CONNECTING);
        await this._socket.connect();
        this._changeState(IgniteClient.STATE.CONNECTED);
    }

    async send(opCode, payloadWriter, payloadReader = null) {
        if (this._state !== IgniteClient.STATE.CONNECTED) {
            throw new Errors.IllegalStateError();
        }
        await this._socket.sendRequest(opCode, payloadWriter, payloadReader);
    }

    disconnect() {
        if (this._state !== IgniteClient.STATE.DISCONNECTED) {
            this._changeState(IgniteClient.STATE.DISCONNECTED);
            if (this._socket) {
                this._socket.disconnect();
                this._socket = null;
            }
        }
    }

    _onSocketDisconnect(error = null) {
        this._changeState(IgniteClient.STATE.DISCONNECTED, error);
        this._socket = null;
    }

    _changeState(state, reason = null) {
        if (Logger.debug && this._socket) {
            Logger.logDebug(Util.format('Socket %s: %s -> %s'),
                this._socket._endpoint, this._getState(this._state), this._getState(state));
        }
        this._state = state;
        if (this._onStateChanged) {
            this._onStateChanged(state, reason);
        }
    }

    _getState(state) {
        switch (state) {
            case IgniteClient.STATE.DISCONNECTED:
                return 'DISCONNECTED';
            case IgniteClient.STATE.CONNECTING:
                return 'CONNECTING';
            case IgniteClient.STATE.CONNECTED:
                return 'CONNECTED';
            default:
                return 'UNKNOWN';
        }
    }
}

module.exports = ClientFailoverSocket;
