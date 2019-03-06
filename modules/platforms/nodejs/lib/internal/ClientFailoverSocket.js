/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
        this._endpointsNumber = this._config._endpoints.length;
        this._endpointIndex = this._getRandomInt(this._endpointsNumber - 1);
        await this._connect();
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

    async _onSocketDisconnect(error = null) {
        this._changeState(IgniteClient.STATE.CONNECTING, null, error);        
        this._socket = null;
        this._endpointIndex++;
        try {
            await this._connect();
        }
        catch (err) {
        }
    }

    async _connect() {
        const errors = new Array();
        let index, endpoint;
        for (let i = 0; i < this._endpointsNumber; i++) {
            index = (this._endpointIndex + i) % this._endpointsNumber;
            endpoint = this._config._endpoints[index];
            try {
                this._changeState(IgniteClient.STATE.CONNECTING, endpoint);
                this._socket = new ClientSocket(
                    endpoint, this._config, this._onSocketDisconnect.bind(this));
                await this._socket.connect();
                this._changeState(IgniteClient.STATE.CONNECTED, endpoint);
                return;
            }
            catch (err) {
                errors.push(Util.format('[%s] %s', endpoint, err.message));
            }
        }
        const error = errors.join('; ');
        this._changeState(IgniteClient.STATE.DISCONNECTED, endpoint, error);
        this._socket = null;
        throw new Errors.IgniteClientError(error);
    }

    _changeState(state, endpoint = null, reason = null) {
        if (Logger.debug) {
            Logger.logDebug(Util.format('Socket %s: %s -> %s'),
                endpoint ? endpoint : this._socket ? this._socket._endpoint : '',
                this._getState(this._state),
                this._getState(state));
        }
        if (this._state !== state) {
            this._state = state;
            if (this._onStateChanged) {
                this._onStateChanged(state, reason);
            }
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

    // returns a random integer between 0 and max
    _getRandomInt(max) {
        if (max === 0) {
            return 0;
        }
        return Math.floor(Math.random() * (max + 1));
    }
}

module.exports = ClientFailoverSocket;
