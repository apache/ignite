/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Server } from 'ws';
import {RequestHook} from 'testcafe';
import getPort from 'get-port';
import uuidv4 from 'uuid/v4';

export class WebSocket {
    constructor(socket) {
        this.socket = socket;
    }

    /**
     * @param {string} eventType
     * @param {(any)=>any} listener
     */
    on(eventType, listener) {
        this.socket.on('message', (msg) => {
            const e = JSON.parse(msg);

            if (e.eventType === eventType) {
                const res = listener(e.payload);

                if (res)
                    this.emit(e.eventType, res, e.requestId);
            }
        });

        return this;
    }

    /**
     * @param {string} eventType
     * @param {any} payload
     * @param {string} requestId
     */
    emit(eventType, payload, requestId = uuidv4()) {
        this.socket.send(JSON.stringify({requestId, eventType, payload}));

        return this;
    }
}

export class WebSocketHook extends RequestHook {
    constructor() {
        super(([/browsers/]));

        this._port = getPort();
    }

    async onRequest(e) {
        e.requestOptions.host = `localhost:${await this._port}`;
        e.requestOptions.hostname = 'localhost';
        e.requestOptions.port = await this._port;
    }

    async onResponse() {}

    destroy() {
        this._io.close();
    }

    /**
     * @param {Array<(ws: WebSocket) => any>} hooks
     */
    use(...hooks) {
        this._port.then((port) => {
            this._io = new Server({port});

            this._io.on('connection', (socket) => {
                const ws = new WebSocket(socket);

                hooks.forEach((hook) => hook(ws));
            });
        });

        return this;
    }
}
