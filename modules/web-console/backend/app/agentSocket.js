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

const _ = require('lodash');

// Fire me up!

/**
 * Module interaction with agents.
 */
module.exports = {
    implements: 'agent-socket'
};

/**
 * @returns {AgentSocket}
 */
module.exports.factory = function() {
    /**
     * Connected agent descriptor.
     */
    class AgentSocket {
        /**
         * @param {Socket} socket Socket for interaction.
         * @param {Array.<String>} tokens Agent tokens.
         * @param {String} demoEnabled Demo enabled.
         */
        constructor(socket, tokens, demoEnabled) {
            Object.assign(this, {
                socket,
                tokens,
                cluster: null,
                demo: {
                    enabled: demoEnabled,
                    browserSockets: []
                }
            });
        }

        /**
         * Send event to agent.
         *
         * @this {AgentSocket}
         * @param {String} event Event name.
         * @param {Array.<Object>} args - Transmitted arguments.
         * @param {Function} [callback] on finish
         */
        _emit(event, args, callback) {
            if (!this.socket.connected) {
                if (callback)
                    callback('org.apache.ignite.agent.AgentException: Connection is closed');

                return;
            }

            this.socket.emit(event, ...args, callback);
        }

        /**
         * Send event to agent.
         *
         * @param {String} event - Event name.
         * @param {Object?} args - Transmitted arguments.
         * @returns {Promise}
         */
        emitEvent(event, ...args) {
            return new Promise((resolve, reject) =>
                this._emit(event, args, (resErr, res) => {
                    if (resErr)
                        return reject(resErr);

                    resolve(res);
                })
            );
        }

        restResultParse(res) {
            if (res.status === 0)
                return JSON.parse(res.data);

            if (res.status === 2)
                throw new Error('AgentSocket failed to authenticate in grid. Please check agent\'s login and password or node port.');

            throw new Error(res.error);
        }

        /**
         * @param {String} token
         * @param {Array.<Socket>} browserSockets
         */
        runDemoCluster(token, browserSockets) {
            this.emitEvent('demo:broadcast:start')
                .then(() => {
                    this.demo.tokens.push(token);
                    this.demo.browserSockets.push(...browserSockets);

                    this.socket.on('demo:topology', (res) => {
                        try {
                            const top = this.restResultParse(res);

                            _.forEach(this.demo.browserSockets, (sock) => sock.emit('topology', top));
                        } catch (err) {
                            _.forEach(this.demo.browserSockets, (sock) => sock.emit('topology:err', err));
                        }
                    });
                });
        }

        /**
         * @param {Socket} browserSocket
         */
        attachToDemoCluster(browserSocket) {
            this.demo.browserSockets.push(...browserSocket);
        }
    }

    return AgentSocket;
};
