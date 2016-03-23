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
    implements: 'agent-manager',
    inject: ['require(lodash)', 'require(ws)', 'require(fs)', 'require(path)', 'require(jszip)', 'require(socket.io)', 'require(apache-ignite)', 'settings', 'mongo']
};

/**
 * @param _
 * @param fs
 * @param path
 * @param JSZip
 * @param socketio
 * @param apacheIgnite
 * @param settings
 * @param mongo
 * @returns {AgentManager}
 */
module.exports.factory = function(_, ws, fs, path, JSZip, socketio, apacheIgnite, settings, mongo) {
    /**
     * Creates an instance of server for Ignite.
     */
    class AgentServer {
        /**
         * @this {AgentServer}
         * @param {Agent} agent Connected agent
         * @param {Boolean} demo Use demo node for request
         */
        constructor(agent, demo) {
            this._agent = agent;
            this._demo = !!demo;
        }

        /**
         * Run http request
         *
         * @this {AgentServer}
         * @param {cmd} cmd Command
         * @param {callback} callback on finish
         */
        runCommand(cmd, callback) {
            const params = {cmd: cmd.name()};

            for (const param of cmd._params)
                params[param.key] = param.value;

            let body;

            let headers;

            let method = 'GET';

            if (cmd._isPost()) {
                body = cmd.postData();

                method = 'POST';

                headers = {JSONObject: 'application/json'};
            }

            this._agent.executeRest('ignite', params, this._demo, method, headers, body, callback);
        }
    }

    /**
     * Connected agent descriptor.
     */
    class Agent {
        /**
         * @param {socketIo.Socket} socket - Agent socket for interaction.
         */
        constructor(socket) {
            /**
             * Agent socket for interaction.
             *
             * @type {socketIo.Socket}
             * @private
             */
            this._socket = socket;

            /**
             * Executor for grid.
             *
             * @type {apacheIgnite.Ignite}
             * @private
             */
            this._cluster = new apacheIgnite.Ignite(new AgentServer(this));

            /**
             * Executor for demo node.
             *
             * @type {apacheIgnite.Ignite}
             * @private
             */
            this._demo = new apacheIgnite.Ignite(new AgentServer(this, true));
        }

        /**
         * Send message to agent.
         *
         * @param {String} event - Event name.
         * @param {Object} data - Transmitted data.
         * @returns {Promise}
         */
        _exec(event, data) {
            const self = this;

            return new Promise((resolve, reject) =>
                self._emit(event, data, (error, res) => {
                    if (error)
                        return reject(error);

                    resolve(res);
                })
            );
        }

        /**
         * Send message to agent.
         *
         * @this {Agent}
         * @param {String} event Command name.
         * @param {Object} data Command params.
         * @param {Function} [callback] on finish
         */
        _emit(event, data, callback) {
            if (!this._socket.connected) {
                if (callback)
                    callback('org.apache.ignite.agent.AgentException: Connection is closed');

                return;
            }

            this._socket.emit(event, data, callback);
        }

        /**
         * Execute rest request on node.
         *
         * @param {String} uri - REST endpoint uri.
         * @param {Object} params - REST request parameters.
         * @param {Boolean} demo - true if execute on demo node.
         * @param {String} [method] - Request method GET or POST.
         * @param {Object} [headers] - REST request headers.
         * @param {String} [body] - REST request body
         * @param {Function} [cb] Callback. Take 3 arguments: {Number} successStatus, {String} error,  {String} response.
         */
        executeRest(uri, params, demo, method, headers, body, cb) {
            if (typeof (params) !== 'object')
                throw '"params" argument must be an object';

            if (typeof (cb) !== 'function')
                throw 'callback must be a function';

            if (body && typeof (body) !== 'string')
                throw 'body must be a string';

            if (headers && typeof (headers) !== 'object')
                throw 'headers must be an object';

            method = method ? method.toUpperCase() : 'GET';

            if (method !== 'GET' && method !== 'POST')
                throw 'Unknown HTTP method: ' + method;

            const _cb = (error, restResult) => {
                if (error)
                    return cb(error);

                error = restResult.error;

                const code = restResult.code;

                if (code !== 200) {
                    if (code === 401)
                        return cb({code, message: 'Failed to authenticate on node.'});

                    return cb({code, message: error || 'Failed connect to node and execute REST command.'});
                }

                try {
                    const response = JSON.parse(restResult.data);

                    if (response.successStatus === 0)
                        return cb(null, response.response);

                    switch (response.successStatus) {
                        case 1:
                            return cb({code: 500, message: response.error});
                        case 2:
                            return cb({code: 401, message: response.error});
                        case 3:
                            return cb({code: 403, message: response.error});
                        default:
                            return cb(response.error);
                    }
                }
                catch (e) {
                    return cb(e);
                }
            };

            this._emit('node:rest', {uri, params, demo, method, headers, body}, _cb);
        }

        /**
         * @param {String} driverPath
         * @param {String} driverClass
         * @param {String} url
         * @param {Object} info
         * @returns {Promise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
         */
        metadataSchemas(driverPath, driverClass, url, info) {
            return this._exec('schemaImport:schemas', {driverPath, driverClass, url, info});
        }

        /**
         * @param {String} driverPath
         * @param {String} driverClass
         * @param {String} url
         * @param {Object} info
         * @param {Array} schemas
         * @param {Boolean} tablesOnly
         * @returns {Promise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
         */
        metadataTables(driverPath, driverClass, url, info, schemas, tablesOnly) {
            return this._exec('schemaImport:metadata', {driverPath, driverClass, url, info, schemas, tablesOnly});
        }

        /**
         * @returns {Promise} Promise on list of jars from driver folder.
         */
        availableDrivers() {
            return this._exec('schemaImport:drivers');
        }

        /**
         * @returns {apacheIgnite.Ignite}
         */
        ignite(demo) {
            return demo ? this._demo : this._cluster;
        }
    }

    /**
     * Connected agents manager.
     */
    class AgentManager {
        /**
         * @constructor
         */
        constructor() {
            /**
             * Connected agents by user id.
             * @type {Object.<ObjectId, Array.<Agent>>}
             */
            this._agents = {};

            /**
             * Connected browsers by user id.
             * @type {Object.<ObjectId, Array.<Socket>>}
             */
            this._browsers = {};

            const agentArchives = fs.readdirSync(settings.agent.dists)
                .filter((file) => path.extname(file) === '.zip');

            /**
             * Supported agents distribution.
             * @type {Object.<String, String>}
             */
            this.supportedAgents = {};

            const jarFilter = (file) => path.extname(file) === '.jar';

            for (const archive of agentArchives) {
                const filePath = path.join(settings.agent.dists, archive);

                const zip = new JSZip(fs.readFileSync(filePath));

                const jarPath = _.find(_.keys(zip.files), jarFilter);

                const jar = new JSZip(zip.files[jarPath].asNodeBuffer());

                const manifest = jar.files['META-INF/MANIFEST.MF']
                    .asText()
                    .trim()
                    .split(/\s*\n+\s*/)
                    .map((line, r) => {
                        r = line.split(/\s*:\s*/);

                        this[r[0]] = r[1];

                        return this;
                    }, {})[0];

                const ver = manifest['Implementation-Version'];

                if (ver) {
                    this.supportedAgents[ver] = {
                        fileName: archive,
                        filePath,
                        buildTime: manifest['Build-Time']
                    };
                }
            }

            const latest = _.first(Object.keys(this.supportedAgents).sort((a, b) => {
                const aParts = a.split('.');
                const bParts = b.split('.');

                for (let i = 0; i < aParts.length; ++i) {
                    if (bParts.length === i)
                        return 1;

                    if (aParts[i] === aParts[i])
                        continue;

                    return aParts[i] > bParts[i] ? 1 : -1;
                }
            }));

            // Latest version of agent distribution.
            if (latest)
                this.supportedAgents.latest = this.supportedAgents[latest];
        }

        attachLegacy(server) {
            const wsSrv = new ws.Server({server});

            wsSrv.on('connection', (_wsClient) => {
                _wsClient.send(JSON.stringify({
                    method: 'authResult',
                    args: ['You are using an older version of the agent. Please reload agent archive']
                }));
            });
        }

        /**
         * @param {http.Server|https.Server} srv Server instance that we want to attach agent handler.
         */
        attach(srv) {
            if (this._server)
                throw 'Agent server already started!';

            this._server = srv;

            /**
             * @type {WebSocketServer}
             */
            this._socket = socketio(this._server);

            this._socket.on('connection', (socket) => {
                socket.on('agent:auth', (data, cb) => {
                    if (!_.isEmpty(this.supportedAgents)) {
                        const ver = data.ver;
                        const bt = data.bt;

                        if (_.isEmpty(ver) || _.isEmpty(bt) || _.isEmpty(this.supportedAgents[ver]) ||
                            this.supportedAgents[ver].buildTime > bt)
                            return cb('You are using an older version of the agent. Please reload agent archive');
                    }

                    mongo.Account.findOne({token: data.token}, (err, account) => {
                        // TODO IGNITE-1379 send error to web master.
                        if (err)
                            cb('Failed to authorize user');
                        else if (!account)
                            cb('Invalid token, user not found');
                        else {
                            const agent = new Agent(socket);

                            socket.on('disconnect', () => {
                                this._removeAgent(account._id, agent);
                            });

                            this._addAgent(account._id, agent);

                            cb();
                        }
                    });
                });
            });
        }

        /**
         * @param {ObjectId} userId
         * @param {Socket} user
         * @returns {int} connected agent count.
         */
        addAgentListener(userId, user) {
            let users = this._browsers[userId];

            if (!users)
                this._browsers[userId] = users = [];

            users.push(user);

            const agents = this._agents[userId];

            return agents ? agents.length : 0;
        }

        /**
         * @param {ObjectId} userId
         * @param {Socket} user
         * @returns {int} connected agent count.
         */
        removeAgentListener(userId, user) {
            const users = this._browsers[userId];

            _.remove(users, (_user) => _user === user);
        }

        /**
         * @param {ObjectId} userId
         * @returns {Promise.<Agent>}
         */
        findAgent(userId) {
            if (!this._server)
                return Promise.reject('Agent server not started yet!');

            const agents = this._agents[userId];

            if (!agents || agents.length === 0)
                return Promise.reject('Failed to connect to agent');

            return Promise.resolve(agents[0]);
        }

        /**
         * Close connections for all user agents.
         * @param {ObjectId} userId
         */
        close(userId) {
            if (!this._server)
                throw 'Agent server not started yet!';

            const agents = this._agents[userId];

            this._agents[userId] = [];

            for (const agent of agents)
                agent._emit('agent:close', 'Security token was changed for user');
        }

        /**
         * @param userId
         * @param {Agent} agent
         */
        _removeAgent(userId, agent) {
            const agents = this._agents[userId];

            _.remove(agents, (_agent) => _agent === agent);

            const users = this._browsers[userId];

            _.forEach(users, (user) => user.emit('agent:count', {count: agents.length}));
        }

        /**
         * @param {ObjectId} userId
         * @param {Agent} agent
         */
        _addAgent(userId, agent) {
            let agents = this._agents[userId];

            if (!agents)
                this._agents[userId] = agents = [];

            agents.push(agent);

            const users = this._browsers[userId];

            _.forEach(users, (user) => user.emit('agent:count', {count: agents.length}));
        }
    }

    return new AgentManager();
};
