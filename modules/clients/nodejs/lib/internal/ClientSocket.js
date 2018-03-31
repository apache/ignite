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

const net = require('net');
const URL = require('url');
const Long = require('long');
const ObjectType = require('../ObjectType');
const Errors = require('../Errors');
const MessageBuffer = require('./MessageBuffer');
const BinaryUtils = require('./BinaryUtils');
const BinaryReader = require('./BinaryReader');
const ArgumentChecker = require('./ArgumentChecker');
const Logger = require('./Logger');

const HANDSHAKE_SUCCESS_STATUS_CODE = 1;
const REQUEST_SUCCESS_STATUS_CODE = 0;
const HANDSHAKE_REQUEST_ID = Long.ZERO;
const PORT_DEFAULT = 10800;

const STATE = Object.freeze({
    INITIAL : 0,
    HANDSHAKE : 1,
    CONNECTED : 2,
    DISCONNECTED : 3
});

class ClientSocket {

    constructor(endpoint, onSocketDisconnect) {
        ArgumentChecker.notEmpty(endpoint, 'endpoints');
        this._parseEndpoint(endpoint);
        this._socket = new net.Socket();
        this._state = STATE.INITIAL;
        this._requestId = HANDSHAKE_REQUEST_ID;
        this._requests = new Map();
        this._onSocketDisconnect = onSocketDisconnect;
        this._error = null;
        this._wasConnected = false;
        this._initSocket();
    }

    async connect() {
        if (this._state !== STATE.INITIAL) {
            return;
        }
        return new Promise((resolve, reject) => {
            const handshakePayloadWriter = (payload) => {
                // Handshake code
                payload.writeByte(1);
                // Protocol version 1.0.0
                payload.writeShort(1);
                payload.writeShort(0);
                payload.writeShort(0);
                // Client code
                payload.writeByte(2);
            };
            const handshakeRequest = new Request(this.requestId, null, handshakePayloadWriter, null, resolve, reject);
            this._addRequest(handshakeRequest);
            this._socket.connect({ host : this._host, port : this._port, version : this._version }, () => {
                this._state = STATE.HANDSHAKE;
                // send handshake
                this._sendRequest(handshakeRequest);
            });
        });
    }

    disconnect() {
        this._state = STATE.DISCONNECTED;
        this._socket.end();
    }

    get requestId() {
        const id = this._requestId;
        this._requestId = this._requestId.add(1);
        return id;
    }

    async sendRequest(opCode, payloadWriter, payloadReader = null) {
        if (this._state === STATE.CONNECTED) {
            return new Promise((resolve, reject) => {
                const request = new Request(this.requestId, opCode, payloadWriter, payloadReader, resolve, reject);
                this._addRequest(request);
                this._sendRequest(request);
            });
        }
        else {
            throw new Errors.LostConnectionError();
        }
    }

    _addRequest(request) {
        this._requests.set(request.id.toString(), request);
    }

    _sendRequest(request) {
        const message = request.message;
        this._logMessage(request.id.toString(), true, message);

        if (!this._socket.write(message)) {
            this._error = new Errors.LostConnectionError();
            this.disconnect();
        }
    }

    _processResponse(message) {
        const buffer = MessageBuffer.from(message);
        // Response length
        const length = buffer.readInteger();
        let requestId, isSuccess;
        const isHandshake = this._state === STATE.HANDSHAKE;

        if (isHandshake) {
            // Handshake status
            isSuccess = (buffer.readByte() === HANDSHAKE_SUCCESS_STATUS_CODE)
            requestId = HANDSHAKE_REQUEST_ID.toString();
        }
        else {
            // Request id
            requestId = buffer.readLong().toString();
            // Status code
            isSuccess = (buffer.readInteger() === REQUEST_SUCCESS_STATUS_CODE);
        }

        this._logMessage(requestId, false, buffer.data);

        if (this._requests.has(requestId)) {
            const request = this._requests.get(requestId);
            this._requests.delete(requestId);
            this._finalizeResponse(buffer, request, isSuccess, isHandshake);
        }
        else {
            throw Errors.IgniteClientError.internalError('Invalid thin client response id: ' + requestId);
        }
    }

    _finalizeResponse(buffer, request, isSuccess, isHandshake) {
        if (!isSuccess) {
            if (isHandshake) {
                // Protocol version
                buffer.readShort();
                buffer.readShort();
                buffer.readShort();
            }
            // Error message
            const errMessage = BinaryReader.readObject(buffer);
            request.reject(new Errors.OperationError(errMessage));
            if (isHandshake) {
                this.disconnect();
            }
        }
        else {
            try {
                if (request.payloadReader) {
                    request.payloadReader(buffer);
                }
                if (isHandshake) {
                    this._state = STATE.CONNECTED;
                    this._wasConnected = true;
                }
                request.resolve();
            }
            catch (err) {
                request.reject(err);
            }
        }
    }

    _disconnect() {
        this._requests.forEach((request, id) => {
            request.reject(this._error ? this._error : new Errors.LostConnectionError());
            this._requests.delete(id);
        });
        if (this._wasConnected) {
            this._onSocketDisconnect(this._error);
        }
    }

    _initSocket() {
        this._socket.on('data', this._processResponse.bind(this));
        this._socket.on('close', () => {
            this._disconnect();
        });
        this._socket.on('error', (error) => {
            this._error = new Errors.LostConnectionError(error);
            this.disconnect();
        });
    }

    _parseEndpoint(endpoint) {
        endpoint = endpoint.trim();
        this._host = endpoint;
        this._port = null;
        const colonCnt = endpoint.split(':').length - 1;
        if (colonCnt > 1) {
            // IPv6 address
            this._version = 6;
            const index = endpoint.lastIndexOf(']:');
            if (index >= 0) {
                this._host = endpoint.substring(0, index + 1);
                this._port = endpoint.substring(index + 2);
            }
            if (this._host.startsWith('[') || this._host.endsWith(']')) {
                if (this._host.startsWith('[') && this._host.endsWith(']')) {
                    this._host = this._host.substring(1, this._host.length - 1);
                }
                else {
                    throw Errors.IgniteClientError.illegalArgumentError('Incorrect endpoint format: ' + endpoint);
                }
            }
        }
        else {
            // IPv4 address
            this._version = 4;
            const index = endpoint.lastIndexOf(':');
            if (index >= 0) {
                this._host = endpoint.substring(0, index);
                this._port = endpoint.substring(index + 1);
            }
        }
        if (!this._port) {
            this._port = PORT_DEFAULT;
        }
        else {
            this._port = parseInt(this._port);
            if (isNaN(this._port)) {
                throw Errors.IgniteClientError.illegalArgumentError('Incorrect endpoint format: ' + endpoint);
            }
        }
    }

    _logMessage(requestId, isRequest, message) {
        if (Logger.debug) {
            Logger.logDebug((isRequest ? 'Request: ' : 'Response: ') + requestId);
            Logger.logDebug(message);
        }
    }
}

class Request {
    constructor(id, opCode, payloadWriter, payloadReader, resolve, reject) {
        this._id = id;
        this._opCode = opCode;
        this._payloadWriter = payloadWriter;
        this._payloadReader = payloadReader;
        this._resolve = resolve;
        this._reject = reject;
    }

    get id() {
        return this._id;
    }

    get payloadReader() {
        return this._payloadReader;
    }

    get resolve() {
        return this._resolve;
    }

    get reject() {
        return this._reject;
    }

    get message() {
        const message = new MessageBuffer();
        // Skip message length
        const messageStartPos = BinaryUtils.getSize(ObjectType.TYPE_CODE.INTEGER);
        message.position = messageStartPos;
        if (this._opCode !== null) {
            // Op code
            message.writeShort(this._opCode);
            // Request id
            message.writeLong(this._id);
        }
        if (this._payloadWriter) {
            // Payload
            this._payloadWriter(message);
        }
        // Message length
        message.position = 0;
        message.writeInteger(message.length - messageStartPos);
        return message.data;
    }
}

module.exports = ClientSocket;