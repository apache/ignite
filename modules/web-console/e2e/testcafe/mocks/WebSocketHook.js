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

import Server from 'socket.io';
import {RequestHook} from 'testcafe';
import PortPool from 'port-pool';

const portPool = new PortPool(3000, 3100);
const getPort = () => new Promise((resolve, reject) => {
    portPool.getNext((port) => {
        if (port === null) reject(new Error('No free ports available'));
        resolve(port);
    });
});

export class WebSocketHook extends RequestHook {
    constructor() {
        super(([/socket\.io/]));
        this._port = getPort();
        this._io = Server();
        this._port.then((port) => this._io.listen(port));
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
     * @param {string} event
     * @param {()=>void} listener
     */
    on(event, listener) {
    	this._io.on(event, listener);
    	return this;
    }

    /**
     * @param {string} event
     * @param {any} data
     */
    emit(event, data) {
    	this._io.emit(event, data);
    }

    /**
     * @param {Array<(hook: WebSocketHook) => any>} hooks
     */
    use(...hooks) {
        hooks.forEach((hook) => hook(this));
        return this;
    }
}
