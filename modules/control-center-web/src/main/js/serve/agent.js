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

module.exports = {
    implements: 'agent',
    inject: ['require(lodash)', 'require(fs)', 'require(socket.io)', 'require(apache-ignite)', 'mongo']
};

/**
 * @param _
 * @param fs
 * @param socketio
 * @param apacheIgnite
 * @param mongo
 * @returns {AgentManager}
 */
module.exports.factory = function(_, fs, socketio, apacheIgnite, mongo) {
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

    class Agent {
        /**
         * @param {socketIo.Socket} _wsSrv
         * @param {AgentManager} manager
         */
        constructor(_wsSrv, manager) {
            const self = this;

            this._manager = manager;

            /**
             * @type {socketIo.Socket}
             * @private
             */
            this._wsSocket = _wsSrv;

            this._wsSocket.on('disconnect', () => {
                if (self._user)
                    self._manager._removeAgent(self._user._id, self);
            });

            this._wsSocket.on('agent:auth', (msg, cb) => self._processAuth(msg, cb));

            this._wsSocket.on('message', (msgStr) => {
                const msg = JSON.parse(msgStr);

                self['_rmt' + msg.type](msg);
            });

            this._reqCounter = 0;

            this._cbMap = {};
        }

        /**
         *
         * @param event
         * @param data
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
         * @param {String} uri
         * @param {Object} params
         * @param {Boolean} demo
         * @param {String} [method]
         * @param {Object} [headers]
         * @param {String} [body]
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

            if (!method)
                method = 'GET';
            else
                method = method.toUpperCase();

            if (method !== 'GET' && method !== 'POST')
                throw 'Unknown HTTP method: ' + method;

            const _cb = (error, restResult) => {
                if (error)
                    return cb(error);

                error = restResult.error;

                if (error)
                    return cb(error);

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
         * Run http request
         *
         * @this {Agent}
         * @param {String} event Command name.
         * @param {Object} data Command params.
         * @param {?Function} callback on finish
         */
        _emit(event, data, callback) {
            if (!this._wsSocket.connected) {
                if (callback)
                    callback('org.apache.ignite.agent.AgentException: Connection is closed');

                return;
            }

            this._wsSocket.emit(event, data, callback);
        }

        /**
         * Process auth request.
         *
         * @param {Object} data
         * @param {Function} cb
         * @private
         */
        _processAuth(data, cb) {
            const self = this;

            fs.stat('public/agent/ignite-web-agent-1.5.0.final.zip', (errFs, stats) => {
                let relDate = 0;

                if (!errFs)
                    relDate = stats.birthtime.getTime();

                if ((data.relDate || 0) < relDate)
                    cb('You are using an older version of the agent. Please reload agent archive');

                mongo.Account.findOne({token: data.token}, (err, account) => {
                    // TODO IGNITE-1379 send error to web master.
                    if (err)
                        cb('Failed to authorize user');
                    else if (!account)
                        cb('Invalid token, user not found');
                    else {
                        self._user = account;

                        self._manager._addAgent(account._id, self);

                        self._cluster = new apacheIgnite.Ignite(new AgentServer(self));

                        self._demo = new apacheIgnite.Ignite(new AgentServer(self, true));

                        cb();
                    }
                });
            });
        }

        /**
         * @returns {Ignite}
         */
        ignite(demo) {
            return demo ? this._demo : this._cluster;
        }
    }

    class AgentManager {
        /**
         * @constructor
         */
        constructor() {
            /**
             * @type {Object.<ObjectId, Array.<Agent>>}
             */
            this._agents = {};
        }

        /**
         * @param {http.Server|https.Server} srv
         */
        listen(srv) {
            if (this._server)
                throw 'Agent server already started!';

            this._server = srv;

            /**
             * @type {WebSocketServer}
             */
            this._wsSocket = socketio(this._server);

            const self = this;

            this._wsSocket.on('connection', (_wsSrv) => new Agent(_wsSrv, self));
        }

        /**
         * @param {ObjectId} userId
         * @returns {Agent}
         */
        findAgent(userId) {
            if (!this._server)
                throw 'Agent server not started yet!';

            const agents = this._agents[userId];

            if (!agents || agents.length === 0)
                return null;

            return agents[0];
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
                agent._emit('close', 'Security token was changed for user');
        }

        /**
         * @param userId
         * @param {Agent} agent
         */
        _removeAgent(userId, agent) {
            const agents = this._agents[userId];

            _.remove(agents, (_agent) => _agent === agent);
        }

        /**
         * @param {ObjectId} userId
         * @param {Agent} agent
         */
        _addAgent(userId, agent) {
            const agents = this._agents[userId];

            if (agents)
                return agents.push(agent);

            this._agents[userId] = [agent];
        }
    }

    return new AgentManager();
};
