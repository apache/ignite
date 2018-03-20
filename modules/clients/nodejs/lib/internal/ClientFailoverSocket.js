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

const Errors = require('../Errors');
const ClientSocket = require('./ClientSocket');

const STATE = Object.freeze({
    DISCONNECTED : 0,
    CONNECTING : 1,
    CONNECTED : 2,
    RECONNECTING : 3,
    DISCONNECTING : 4
});

/** Socket wrapper with failover functionality: reconnects on failure. */
class ClientFailoverSocket {

    constructor() {
        this._socket = null;
        this._state = STATE.DISCONNECTED;
    }

    async connect(config, onDisconnect = null) {
        if (this._state !== STATE.DISCONNECTED) {
            throw new Errors.IllegalStateError('The client is already connected');
        }
        this._state = STATE.CONNECTING;
        this._onDisconnect = onDisconnect;
        this._config = config;
        this._socket = new ClientSocket(this._config.endpoints[0], this._onSocketDisconnect.bind(this));
        await this._socket.connect();
        this._state = STATE.CONNECTED;
    }

    async send(opCode, payloadWriter, payloadReader = null) {
        if (this._state === STATE.RECONNECTING) {
            throw new Errors.LostConnectionError();
        }
        else if (this._state !== STATE.CONNECTED) {
            throw new Errors.IllegalStateError();
        }
        await this._socket.sendRequest(opCode, payloadWriter, payloadReader);
    }

    disconnect() {
        if (this._state !== STATE.DISCONNECTED && this._state !== STATE.DISCONNECTING) {
            this._state = STATE.DISCONNECTING;
            if (this._socket) {
                this._socket.disconnect();
            }
            else {
                this._onSocketDisconnect();
            }
        }
    }

    _onSocketDisconnect(error = null) {
        this._state = STATE.DISCONNECTED;
        this._socket = null;
        if (this._onDisconnect) {
            this._onDisconnect(error);
        }
    }
}

module.exports = ClientFailoverSocket;
