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
                const key = sock.request.user._id.toString();

                if (this.sockets.has(key))
                    this.sockets.get(key).push(sock);
                else
                    this.sockets.set(key, [sock]);

                return this.sockets.get(sock.request.user);
            }

            /**
             * @param {Socket} sock
             */
            remove(sock) {
                const key = sock.request.user._id.toString();

                const sockets = this.sockets.get(key);

                _.pull(sockets, sock);

                return sockets;
            }

            get(account) {
                const key = account._id.toString();

                let sockets = this.sockets.get(key);

                if (_.isEmpty(sockets))
                    this.sockets.set(key, sockets = []);

                return sockets;
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
             * @param {String} account
             * @param {Array.<Socket>} [socks]
             */
            agentStats(account, socks = this._browserSockets.get(account)) {
                return this._agentHnd.agents(account)
                    .then((agentSocks) => {
                        const stat = _.reduce(agentSocks, (acc, agentSock) => {
                            acc.count += 1;
                            acc.hasDemo = acc.hasDemo || _.get(agentSock, 'demo.enabled');

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

            clusterChanged(account, cluster) {
                const socks = this._browserSockets.get(account);

                _.forEach(socks, (sock) => {
                    if (sock)
                        sock.emit('cluster:changed', cluster);
                    else
                        console.log(`Fount closed socket [account=${account}, cluster=${cluster}]`);
                });
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

            executeOnAgent(account, demo, event, ...args) {
                const cb = _.last(args);

                return this._agentHnd.agent(account, demo)
                    .then((agentSock) => agentSock.emitEvent(event, ..._.dropRight(args)))
                    .then((res) => cb(null, res))
                    .catch((err) => cb(this.errorTransformer(err)));
            }

            agentListeners(sock) {
                const demo = sock.request._query.IgniteDemoMode === 'true';
                const account = () => sock.request.user;

                // Return available drivers to browser.
                sock.on('schemaImport:drivers', (...args) => {
                    this.executeOnAgent(account(), demo, 'schemaImport:drivers', ...args);
                });

                // Return schemas from database to browser.
                sock.on('schemaImport:schemas', (...args) => {
                    this.executeOnAgent(account(), demo, 'schemaImport:schemas', ...args);
                });

                // Return tables from database to browser.
                sock.on('schemaImport:metadata', (...args) => {
                    this.executeOnAgent(account(), demo, 'schemaImport:metadata', ...args);
                });
            }

            /**
             * @param {Promise.<AgentSocket>} agent
             * @param {Boolean} demo
             * @param {{sessionId: String}|{'login': String, 'password': String}} credentials
             * @param {Object.<String, String>} params
             * @return {Promise.<T>}
             */
            executeOnNode(agent, token, demo, credentials, params) {
                return agent
                    .then((agentSock) => agentSock.emitEvent('node:rest',
                        {uri: 'ignite', token, demo, params: _.merge({}, credentials, params)}));
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

                    const agent = this._agentHnd.agent(sock.request.user, demo, clusterId);

                    const token = sock.request.user.token;

                    this.executeOnNode(agent, token, demo, credentials, params)
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

                this.registerVisorTask('queryFetchFirstPage', internalVisor('query.VisorQueryFetchFirstPageTask'), internalVisor('query.VisorQueryNextPageTaskArg'));
                this.registerVisorTask('queryPing', internalVisor('query.VisorQueryPingTask'), internalVisor('query.VisorQueryNextPageTaskArg'));

                this.registerVisorTask('queryClose', internalVisor('query.VisorQueryCleanupTask'), 'java.util.Map', 'java.util.UUID', 'java.util.Set');
                this.registerVisorTask('queryCloseX2', internalVisor('query.VisorQueryCleanupTask'), internalVisor('query.VisorQueryCleanupTaskArg'));

                this.registerVisorTask('toggleClusterState', internalVisor('misc.VisorChangeGridActiveStateTask'), internalVisor('misc.VisorChangeGridActiveStateTaskArg'));

                this.registerVisorTask('cacheNamesCollectorTask', internalVisor('cache.VisorCacheNamesCollectorTask'), 'java.lang.Void');

                this.registerVisorTask('cacheNodesTask', internalVisor('cache.VisorCacheNodesTask'), 'java.lang.String');
                this.registerVisorTask('cacheNodesTaskX2', internalVisor('cache.VisorCacheNodesTask'), internalVisor('cache.VisorCacheNodesTaskArg'));

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

                    const agent = this._agentHnd.agent(sock.request.user, demo, clusterId);

                    const token = sock.request.user.token;

                    this.executeOnNode(agent, token, demo, credentials, exeParams)
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
                            });

                            this.agentListeners(sock);
                            this.nodeListeners(sock);

                            this.pushInitialData(sock);
                            this.agentStats(sock.request.user, [sock]);
                            this.emitNotification(sock);
                        });
                    });
            }
        }

        return new BrowsersHandler();
    }
};
