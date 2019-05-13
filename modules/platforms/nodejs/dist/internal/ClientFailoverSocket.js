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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const Util = require("util");
const internal_1 = require("../internal");
/** Socket wrapper with failover functionality: reconnects on failure. */
class ClientFailoverSocket {
    constructor(onStateChanged) {
        this._socket = null;
        this._state = internal_1.IgniteClient.STATE.DISCONNECTED;
        this._onStateChanged = onStateChanged;
    }
    connect(config) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._state !== internal_1.IgniteClient.STATE.DISCONNECTED) {
                throw new internal_1.Errors.IllegalStateError();
            }
            this._config = config;
            this._endpointsNumber = this._config._endpoints.length;
            this._endpointIndex = this._getRandomInt(this._endpointsNumber - 1);
            yield this._connect();
        });
    }
    send(opCode, payloadWriter, payloadReader = null) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._state !== internal_1.IgniteClient.STATE.CONNECTED) {
                throw new internal_1.Errors.IllegalStateError();
            }
            yield this._socket.sendRequest(opCode, payloadWriter, payloadReader);
        });
    }
    disconnect() {
        if (this._state !== internal_1.IgniteClient.STATE.DISCONNECTED) {
            this._changeState(internal_1.IgniteClient.STATE.DISCONNECTED);
            if (this._socket) {
                this._socket.disconnect();
                this._socket = null;
            }
        }
    }
    _onSocketDisconnect(error = null) {
        return __awaiter(this, void 0, void 0, function* () {
            this._changeState(internal_1.IgniteClient.STATE.CONNECTING, null, error);
            this._socket = null;
            this._endpointIndex++;
            try {
                yield this._connect();
            }
            catch (err) {
            }
        });
    }
    _connect() {
        return __awaiter(this, void 0, void 0, function* () {
            const errors = new Array();
            let index, endpoint;
            for (let i = 0; i < this._endpointsNumber; i++) {
                index = (this._endpointIndex + i) % this._endpointsNumber;
                endpoint = this._config._endpoints[index];
                try {
                    this._changeState(internal_1.IgniteClient.STATE.CONNECTING, endpoint);
                    this._socket = new internal_1.ClientSocket(endpoint, this._config, this._onSocketDisconnect.bind(this));
                    yield this._socket.connect();
                    this._changeState(internal_1.IgniteClient.STATE.CONNECTED, endpoint);
                    return;
                }
                catch (err) {
                    errors.push(Util.format('[%s] %s', endpoint, err.message));
                }
            }
            const error = errors.join('; ');
            this._changeState(internal_1.IgniteClient.STATE.DISCONNECTED, endpoint, error);
            this._socket = null;
            throw new internal_1.Errors.IgniteClientError(error);
        });
    }
    _changeState(state, endpoint = null, reason = null) {
        if (internal_1.Logger.debug) {
            internal_1.Logger.logDebug(Util.format('Socket %s: %s -> %s'), endpoint ? endpoint : this._socket ? this._socket._endpoint : '', this._getState(this._state), this._getState(state));
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
            case internal_1.IgniteClient.STATE.DISCONNECTED:
                return 'DISCONNECTED';
            case internal_1.IgniteClient.STATE.CONNECTING:
                return 'CONNECTING';
            case internal_1.IgniteClient.STATE.CONNECTED:
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
exports.ClientFailoverSocket = ClientFailoverSocket;
//# sourceMappingURL=ClientFailoverSocket.js.map