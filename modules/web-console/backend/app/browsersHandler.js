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
const socketio = require('socket.io');

// Fire me up!

/**
 * Module interaction with browsers.
 */
module.exports = {
    implements: 'browsers-handler',
    inject: ['configure', 'errors', 'mongo'],
    factory: (configure, errors, mongo) => {
        class BrowserSockets {
            constructor() {
                this.sockets = new Map();
            }

            /**
             * @param {Socket} sock
             */
            add(sock) {
                const token = sock.request.user.token;

                if (this.sockets.has(token))
                    this.sockets.get(token).push(sock);
                else
                    this.sockets.set(token, [sock]);

                return this.sockets.get(token);
            }

            /**
             * @param {Socket} sock
             */
            remove(sock) {
                const token = sock.request.user.token;

                const sockets = this.sockets.get(token);

                _.pull(sockets, sock);

                return sockets;
            }

            get(token) {
                if (this.sockets.has(token))
                    return this.sockets.get(token);

                return [];
            }

            demo(token) {
                return _.filter(this.sockets.get(token), (sock) => sock.request._query.IgniteDemoMode === 'true');
            }
        }

        class BrowsersHandler {
            /**
             * @constructor
             */
            constructor() {
                /**
                 * Connected browsers.
                 * @type {BrowserSockets}
                 */
                this._browserSockets = new BrowserSockets();

                /**
                 * Registered Visor task.
                 * @type {Map}
                 */
                this._visorTasks = new Map();
            }

            /**
             * @param {Error} err
             * @return {{code: number, message: *}}
             */
            errorTransformer(err) {
                return {
                    code: err.code || 1,
                    message: err.message || err
                };
            }

            /**
             * @param {String} token
             * @param {Array.<Socket>} [socks]
             */
            agentStats(token, socks = this._browserSockets.get(token)) {
                return this._agentHnd.agents(token)
                    .then((agentSocks) => {
                        const stat = _.reduce(agentSocks, (acc, agentSock) => {
                            acc.count += 1;
                            acc.hasDemo |= _.get(agentSock, 'demo.enabled');

                            if (agentSock.cluster)
                                acc.clusters.push(agentSock.cluster);

                            return acc;
                        }, {count: 0, hasDemo: false, clusters: []});

                        stat.clusters = _.uniqWith(stat.clusters, _.isEqual);

                        return stat;
                    })
                    .catch(() => ({count: 0, hasDemo: false, clusters: []}))
                    .then((stat) => _.forEach(socks, (sock) => sock.emit('agents:stat', stat)));
            }

            clusterChanged(token, cluster) {
                const socks = this._browserSockets.get(token);

                _.forEach(socks, (sock) => sock.emit('cluster:changed', cluster));
            }

            pushInitialData(sock) {
                // Send initial data.
            }

            emitNotification(sock) {
                sock.emit('user:notifications', this.notification);
            }

            /**
             * @param {String} notification Notification message.
             */
            updateNotification(notification) {
                this.notification = notification;

                for (const socks of this._browserSockets.sockets.values()) {
                    for (const sock of socks)
                        this.emitNotification(sock);
                }
            }

            executeOnAgent(token, demo, event, ...args) {
                const cb = _.last(args);

                return this._agentHnd.agent(token, demo)
                    .then((agentSock) => agentSock.emitEvent(event, ..._.dropRight(args)))
                    .then((res) => cb(null, res))
                    .catch((err) => cb(this.errorTransformer(err)));
            }

            agentListeners(sock) {
                const demo = sock.request._query.IgniteDemoMode === 'true';
                const token = () => sock.request.user.token;

                // Return available drivers to browser.
                sock.on('schemaImport:drivers', (...args) => {
                    this.executeOnAgent(token(), demo, 'schemaImport:drivers', ...args);
                });

                // Return schemas from database to browser.
                sock.on('schemaImport:schemas', (...args) => {
                    this.executeOnAgent(token(), demo, 'schemaImport:schemas', ...args);
                });

                // Return tables from database to browser.
                sock.on('schemaImport:metadata', (...args) => {
                    this.executeOnAgent(token(), demo, 'schemaImport:metadata', ...args);
                });
            }

            /**
             * @param {Promise.<AgentSocket>} agent
             * @param {Boolean} demo
             * @param {{sessionId: String}|{'login': String, 'password': String}} credentials
             * @param {Object.<String, String>} params
             * @return {Promise.<T>}
             */
            executeOnNode(agent, demo, credentials, params) {
                return agent
                    .then((agentSock) => agentSock.emitEvent('node:rest',
                        {uri: 'ignite', demo, params: _.merge({}, credentials, params)}));
            }

            registerVisorTask(taskId, taskCls, ...argCls) {
                this._visorTasks.set(taskId, {
                    taskCls,
                    argCls
                });
            }

            nodeListeners(sock) {
                // Return command result from grid to browser.
                sock.on('node:rest', (arg, cb) => {
                    const {clusterId, params, credentials} = arg || {};

                    if (!_.isFunction(cb))
                        cb = console.log;

                    const demo = _.get(sock, 'request._query.IgniteDemoMode') === 'true';

                    if ((_.isNil(clusterId) && !demo) || _.isNil(params)) {
                        console.log('Received invalid message: "node:rest" on socket:', JSON.stringify(sock.handshake));

                        return cb('Invalid format of message: "node:rest"');
                    }

                    const token = sock.request.user.token;

                    const agent = this._agentHnd.agent(token, demo, clusterId);

                    this.executeOnNode(agent, demo, credentials, params)
                        .then((data) => cb(null, data))
                        .catch((err) => cb(this.errorTransformer(err)));
                });

                const internalVisor = (postfix) => `org.apache.ignite.internal.visor.${postfix}`;

                this.registerVisorTask('querySql', internalVisor('query.VisorQueryTask'), internalVisor('query.VisorQueryArg'));
                this.registerVisorTask('querySqlV2', internalVisor('query.VisorQueryTask'), internalVisor('query.VisorQueryArgV2'));
                this.registerVisorTask('querySqlV3', internalVisor('query.VisorQueryTask'), internalVisor('query.VisorQueryArgV3'));
                this.registerVisorTask('querySqlX2', internalVisor('query.VisorQueryTask'), internalVisor('query.VisorQueryTaskArg'));

                this.registerVisorTask('queryScanX2', internalVisor('query.VisorScanQueryTask'), internalVisor('query.VisorScanQueryTaskArg'));

                this.registerVisorTask('queryFetch', internalVisor('query.VisorQueryNextPageTask'), 'org.apache.ignite.lang.IgniteBiTuple', 'java.lang.String', 'java.lang.Integer');
                this.registerVisorTask('queryFetchX2', internalVisor('query.VisorQueryNextPageTask'), internalVisor('query.VisorQueryNextPageTaskArg'));

                this.registerVisorTask('queryClose', internalVisor('query.VisorQueryCleanupTask'), 'java.util.Map', 'java.util.UUID', 'java.util.Set');
                this.registerVisorTask('queryCloseX2', internalVisor('query.VisorQueryCleanupTask'), internalVisor('query.VisorQueryCleanupTaskArg'));

                this.registerVisorTask('toggleClusterState', internalVisor('misc.VisorChangeGridActiveStateTask'), internalVisor('misc.VisorChangeGridActiveStateTaskArg'));

                // Return command result from grid to browser.
                sock.on('node:visor', (arg, cb) => {
                    const {clusterId, params, credentials} = arg || {};

                    if (!_.isFunction(cb))
                        cb = console.log;

                    const demo = _.get(sock, 'request._query.IgniteDemoMode') === 'true';

                    if ((_.isNil(clusterId) && !demo) || _.isNil(params)) {
                        console.log('Received invalid message: "node:visor" on socket:', JSON.stringify(sock.handshake));

                        return cb('Invalid format of message: "node:visor"');
                    }

                    const token = sock.request.user.token;

                    const {taskId, nids, args = []} = params;

                    const desc = this._visorTasks.get(taskId);

                    if (_.isNil(desc))
                        return cb(this.errorTransformer(new errors.IllegalArgumentException(`Failed to find Visor task for id: ${taskId}`)));

                    const exeParams = {
                        cmd: 'exe',
                        name: 'org.apache.ignite.internal.visor.compute.VisorGatewayTask',
                        p1: nids,
                        p2: desc.taskCls
                    };

                    _.forEach(_.concat(desc.argCls, args), (param, idx) => { exeParams[`p${idx + 3}`] = param; });

                    const agent = this._agentHnd.agent(token, demo, clusterId);

                    this.executeOnNode(agent, demo, credentials, exeParams)
                        .then((data) => {
                            if (data.finished && !data.zipped)
                                return cb(null, data.result);

                            return cb(null, data);
                        })
                        .catch((err) => cb(this.errorTransformer(err)));
                });
            }

            /**
             *
             * @param server
             * @param {AgentsHandler} agentHnd
             */
            attach(server, agentHnd) {
                this._agentHnd = agentHnd;

                if (this.io)
                    throw 'Browser server already started!';

                mongo.Notifications.findOne().sort('-date').exec()
                    .then((notification) => {
                        this.notification = notification;
                    })
                    .then(() => {
                        const io = socketio(server);

                        configure.socketio(io);

                        // Handle browser connect event.
                        io.sockets.on('connection', (sock) => {
                            this._browserSockets.add(sock);

                            // Handle browser disconnect event.
                            sock.on('disconnect', () => {
                                this._browserSockets.remove(sock);

                                const demo = sock.request._query.IgniteDemoMode === 'true';

                                // Stop demo if latest demo tab for this token.
                                demo && agentHnd.tryStopDemo(sock);
                            });

                            this.agentListeners(sock);
                            this.nodeListeners(sock);

                            this.pushInitialData(sock);
                            this.agentStats(sock.request.user.token, [sock]);
                            this.emitNotification(sock);
                        });
                    });
            }
        }

        return new BrowsersHandler();
    }
};
