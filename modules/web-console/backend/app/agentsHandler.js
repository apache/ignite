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

const uuid = require('uuid/v4');

const fs = require('fs');
const path = require('path');
const JSZip = require('jszip');
const socketio = require('socket.io');
const _ = require('lodash');

// Fire me up!

/**
 * Module interaction with agents.
 */
module.exports = {
    implements: 'agents-handler',
    inject: ['settings', 'mongo', 'agent-socket']
};

/**
 * @param settings
 * @param mongo
 * @param {AgentSocket} AgentSocket
 * @returns {AgentsHandler}
 */
module.exports.factory = function(settings, mongo, AgentSocket) {
    class AgentSockets {
        constructor() {
            /**
             * @type {Map.<String, Array.<String>>}
             */
            this.sockets = new Map();
        }

        get(account) {
            let sockets = this.sockets.get(account._id.toString());

            if (_.isEmpty(sockets))
                this.sockets.set(account._id.toString(), sockets = []);

            return sockets;
        }

        /**
         * @param {AgentSocket} sock
         * @param {String} account
         * @return {Array.<AgentSocket>}
         */
        add(account, sock) {
            const sockets = this.get(account);

            sockets.push(sock);
        }

        /**
         * @param {Socket} browserSocket
         * @return {AgentSocket}
         */
        find(browserSocket) {
            const {_id} = browserSocket.request.user;

            const sockets = this.sockets.get(_id);

            return _.find(sockets, (sock) => _.includes(sock.demo.browserSockets, browserSocket));
        }
    }

    class Cluster {
        constructor(top) {
            const clusterName = top.clusterName;

            this.id = _.isEmpty(top.clusterId) ? uuid() : top.clusterId;
            this.name = _.isEmpty(clusterName) ? `Cluster ${this.id.substring(0, 8).toUpperCase()}` : clusterName;
            this.nids = top.nids;
            this.addresses = top.addresses;
            this.clients = top.clients;
            this.clusterVersion = top.clusterVersion;
            this.active = top.active;
            this.secured = top.secured;
        }

        isSameCluster(top) {
            return _.intersection(this.nids, top.nids).length > 0;
        }

        update(top) {
            this.clusterVersion = top.clusterVersion;
            this.nids = top.nids;
            this.addresses = top.addresses;
            this.clients = top.clients;
            this.clusterVersion = top.clusterVersion;
            this.active = top.active;
            this.secured = top.secured;
        }

        same(top) {
            return _.difference(this.nids, top.nids).length === 0 &&
                _.isEqual(this.addresses, top.addresses) &&
                this.clusterVersion === top.clusterVersion &&
                this.active === top.active;
        }
    }

    /**
     * Connected agents manager.
     */
    class AgentsHandler {
        /**
         * @constructor
         */
        constructor() {
            /**
             * Connected agents.
             * @type {AgentSockets}
             */
            this._agentSockets = new AgentSockets();

            this.clusters = [];
            this.topLsnrs = [];
        }

        /**
         * Collect supported agents list.
         * @private
         */
        _collectSupportedAgents() {
            const jarFilter = (file) => path.extname(file) === '.jar';

            const agentArchives = fs.readdirSync(settings.agent.dists)
                .filter((file) => path.extname(file) === '.zip');

            const agentsPromises = _.map(agentArchives, (fileName) => {
                const filePath = path.join(settings.agent.dists, fileName);

                return JSZip.loadAsync(fs.readFileSync(filePath))
                    .then((zip) => {
                        const jarPath = _.find(_.keys(zip.files), jarFilter);

                        return JSZip.loadAsync(zip.files[jarPath].async('nodebuffer'))
                            .then((jar) => jar.files['META-INF/MANIFEST.MF'].async('string'))
                            .then((lines) =>
                                _.reduce(lines.split(/\s*\n+\s*/), (acc, line) => {
                                    if (!_.isEmpty(line)) {
                                        const arr = line.split(/\s*:\s*/);

                                        acc[arr[0]] = arr[1];
                                    }

                                    return acc;
                                }, {}))
                            .then((manifest) => {
                                const ver = manifest['Implementation-Version'];
                                const buildTime = manifest['Build-Time'];

                                if (ver && buildTime)
                                    return { fileName, filePath, ver, buildTime };
                            });
                    });
            });

            return Promise.all(agentsPromises)
                .then((descs) => {
                    const agentDescs = _.keyBy(_.remove(descs, null), 'ver');

                    const latestVer = _.head(Object.keys(agentDescs).sort((a, b) => {
                        const aParts = a.split('.');
                        const bParts = b.split('.');

                        for (let i = 0; i < aParts.length; ++i) {
                            if (aParts[i] !== bParts[i])
                                return aParts[i] < bParts[i] ? 1 : -1;
                        }

                        if (aParts.length === bParts.length)
                            return 0;

                        return aParts.length < bParts.length ? 1 : -1;
                    }));

                    // Latest version of agent distribution.
                    if (latestVer)
                        agentDescs.current = agentDescs[latestVer];

                    return agentDescs;
                });
        }

        getOrCreateCluster(top) {
            let cluster = _.find(this.clusters, (c) => c.isSameCluster(top));

            if (_.isNil(cluster)) {
                cluster = new Cluster(top);

                this.clusters.push(cluster);
            }

            return cluster;
        }

        /**
         * Add topology listener.
         *
         * @param lsnr
         */
        addTopologyListener(lsnr) {
            this.topLsnrs.push(lsnr);
        }

        /**
         * Link agent with browsers by account.
         *
         * @param {Socket} sock
         * @param {Array.<mongo.Account>} accounts
         * @param {Array.<String>} tokens
         * @param {boolean} demoEnabled
         *
         * @private
         */
        onConnect(sock, accounts, tokens, demoEnabled) {
            const agentSocket = new AgentSocket(sock, accounts, tokens, demoEnabled);

            _.forEach(accounts, (account) => {
                this._agentSockets.add(account, agentSocket);

                this._browsersHnd.agentStats(account);
            });

            sock.on('disconnect', () => {
                _.forEach(accounts, (account) => {
                    _.pull(this._agentSockets.get(account), agentSocket);

                    this._browsersHnd.agentStats(account);
                });
            });

            sock.on('cluster:topology', (top) => {
                if (_.isNil(top)) {
                    console.log('Invalid format of message: "cluster:topology"');

                    return;
                }

                const cluster = this.getOrCreateCluster(top);

                _.forEach(this.topLsnrs, (lsnr) => lsnr(agentSocket, cluster, top));

                if (agentSocket.cluster !== cluster) {
                    agentSocket.cluster = cluster;

                    _.forEach(accounts, (account) => {
                        this._browsersHnd.agentStats(account);
                    });
                }
                else {
                    const changed = !cluster.same(top);

                    if (changed) {
                        cluster.update(top);

                        _.forEach(accounts, (account) => {
                            this._browsersHnd.clusterChanged(account, cluster);
                        });
                    }
                }
            });

            sock.on('cluster:disconnected', () => {
                const newTop = _.assign({}, agentSocket.cluster, {nids: []});

                _.forEach(this.topLsnrs, (lsnr) => lsnr(agentSocket, agentSocket.cluster, newTop));

                agentSocket.cluster = null;

                _.forEach(accounts, (account) => {
                    this._browsersHnd.agentStats(account);
                });
            });

            return agentSocket;
        }

        getAccounts(tokens) {
            return mongo.Account.find({token: {$in: tokens}}, '_id token').lean().exec()
                .then((accounts) => ({accounts, activeTokens: _.uniq(_.map(accounts, 'token'))}));
        }

        /**
         * @param {http.Server|https.Server} srv Server instance that we want to attach agent handler.
         * @param {BrowsersHandler} browsersHnd
         */
        attach(srv, browsersHnd) {
            this._browsersHnd = browsersHnd;

            this._collectSupportedAgents()
                .then((supportedAgents) => {
                    this.currentAgent = _.get(supportedAgents, 'current');

                    if (this.io)
                        throw 'Agent server already started!';

                    this.io = socketio(srv, {path: '/agents'});

                    this.io.on('connection', (sock) => {
                        const sockId = sock.id;

                        console.log('Connected agent with socketId: ', sockId);

                        sock.on('disconnect', (reason) => {
                            console.log(`Agent disconnected with [socketId=${sockId}, reason=${reason}]`);
                        });

                        sock.on('agent:auth', ({ver, bt, tokens, disableDemo} = {}, cb) => {
                            console.log(`Received authentication request [socketId=${sockId}, tokens=${tokens}, ver=${ver}].`);

                            if (_.isEmpty(tokens))
                                return cb('Tokens not set. Please reload agent archive or check settings');

                            if (ver && bt && !_.isEmpty(supportedAgents)) {
                                const btDistr = _.get(supportedAgents, [ver, 'buildTime']);

                                if (_.isEmpty(btDistr) || btDistr !== bt)
                                    return cb('You are using an older version of the agent. Please reload agent');
                            }

                            return this.getAccounts(tokens)
                                .then(({accounts, activeTokens}) => {
                                    if (_.isEmpty(activeTokens))
                                        return cb(`Failed to authenticate with token(s): ${tokens.join(',')}. Please reload agent archive or check settings`);

                                    cb(null, activeTokens);

                                    return this.onConnect(sock, accounts, activeTokens, disableDemo);
                                })
                                // TODO IGNITE-1379 send error to web master.
                                .catch(() => cb(`Invalid token(s): ${tokens.join(',')}. Please reload agent archive or check settings`));
                        });
                    });
                })
                .catch(() => {
                    console.log('Failed to collect supported agents');
                });
        }

        agent(account, demo, clusterId) {
            if (!this.io)
                return Promise.reject(new Error('Agent server not started yet!'));

            const socks = this._agentSockets.get(account);

            if (_.isEmpty(socks))
                return Promise.reject(new Error('Failed to find connected agent for this account'));

            if (demo || _.isNil(clusterId))
                return Promise.resolve(_.head(socks));

            const sock = _.find(socks, (agentSock) => _.get(agentSock, 'cluster.id') === clusterId);

            if (_.isEmpty(sock))
                return Promise.reject(new Error('Failed to find agent connected to cluster'));

            return Promise.resolve(sock);
        }

        agents(account) {
            if (!this.io)
                return Promise.reject(new Error('Agent server not started yet!'));

            const socks = this._agentSockets.get(account);

            if (_.isEmpty(socks))
                return Promise.reject(new Error('Failed to find connected agent for this token'));

            return Promise.resolve(socks);
        }

        /**
         * Try stop agent for token if not used by others.
         *
         * @param {mongo.Account} account
         */
        onTokenReset(account) {
            if (_.isNil(this.io))
                return;

            const agentSockets = this._agentSockets.get(account);

            _.forEach(agentSockets, (sock) => sock.resetToken(account.token));
        }
    }

    return new AgentsHandler();
};
