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

// Fire me up!

/**
 * Module interaction with agents.
 */
module.exports = {
    implements: 'agent-socket',
    inject: ['require(lodash)']
};

/**
 * Helper class to contract REST command.
 */
class Command {
    /**
     * @param {Boolean} demo Is need run command on demo node.
     * @param {String} name Command name.
     */
    constructor(demo, name) {
        this.demo = demo;

        /**
         * Command name.
         * @type {String}
         */
        this._name = name;

        /**
         * Command parameters.
         * @type {Array.<Object.<String, String>>}
         */
        this._params = [];

        this._paramsLastIdx = 1;
    }

    /**
     * Add parameter to command.
     * @param {Object} value Parameter value.
     * @returns {Command}
     */
    addParam(value) {
        this._params.push({key: `p${this._paramsLastIdx++}`, value});

        return this;
    }

    /**
     * Add parameter to command.
     * @param {String} key Parameter key.
     * @param {Object} value Parameter value.
     * @returns {Command}
     */
    addNamedParam(key, value) {
        this._params.push({key, value});

        return this;
    }
}

/**
 * @param _
 * @returns {AgentSocket}
 */
module.exports.factory = function(_) {
    /**
     * Connected agent descriptor.
     */
    class AgentSocket {
        /**
         * @param {Socket} socket Socket for interaction.
         * @param {String} tokens Active tokens.
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
         * @param {String} event Command name.
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
         * @param {Array.<Object>?} args - Transmitted arguments.
         * @returns {Promise}
         */
        emitEvent(event, ...args) {
            return new Promise((resolve, reject) =>
                this._emit(event, args, (error, res) => {
                    if (error)
                        return reject(error);

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

        startCollectTopology(timeout) {
            return this.emitEvent('start:collect:topology', timeout);
        }

        stopCollectTopology(demo) {
            return this.emitEvent('stop:collect:topology', demo);
        }

        /**
         * Execute REST request on node.
         *
         * @param {Boolean} demo Is need run command on demo node.
         * @param {String} cmd REST command.
         * @param {Array.<String>} args - REST command arguments.
         * @return {Promise}
         */
        restCommand(demo, cmd, ...args) {
            const params = {cmd};

            _.forEach(args, (arg, idx) => {
                params[`p${idx + 1}`] = args[idx];
            });

            return this.emitEvent('node:rest', {uri: 'ignite', demo, params, method: 'GET'})
                .then(this.restResultParse);
        }

        gatewayCommand(demo, nids, taskCls, argCls, ...args) {
            const cmd = new Command(demo, 'exe')
                .addNamedParam('name', 'org.apache.ignite.internal.visor.compute.VisorGatewayTask')
                .addParam(nids)
                .addParam(taskCls)
                .addParam(argCls);

            _.forEach(args, (arg) => cmd.addParam(arg));

            return this.restCommand(cmd);
        }

        /**
         * @param {Boolean} demo Is need run command on demo node.
         * @param {Boolean} attr Get attributes, if this parameter has value true. Default value: true.
         * @param {Boolean} mtr Get metrics, if this parameter has value true. Default value: false.
         * @returns {Promise}
         */
        topology(demo, attr, mtr) {
            const cmd = new Command(demo, 'top')
                .addNamedParam('attr', attr !== false)
                .addNamedParam('mtr', !!mtr);

            return this.restCommand(cmd);
        }

        /**
         * @param {Boolean} demo Is need run command on demo node.
         * @param {String} cacheName Cache name.
         * @returns {Promise}
         */
        metadata(demo, cacheName) {
            const cmd = new Command(demo, 'metadata')
                .addNamedParam('cacheName', cacheName);

            return this.restCommand(cmd);
        }
    }

    return AgentSocket;
};
