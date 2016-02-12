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
    inject: ['require(fs)', 'require(ws)', 'require(apache-ignite)', 'mongo']
};

module.exports.factory = function(fs, ws, apacheIgnite, mongo) {
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
         * @param {AgentManager} manager
         * @param {WebSocket} _wsSrv
         */
        constructor(_wsSrv, manager) {
            const self = this;

            this._manager = manager;

            this._wsSrv = _wsSrv;

            this._wsSrv.on('close', () => {
                if (self._user)
                    self._manager._removeClient(self._user._id, self);
            });

            this._wsSrv.on('message', (msgStr) => {
                const msg = JSON.parse(msgStr);

                self['_rmt' + msg.type](msg);
            });

            this._reqCounter = 0;

            this._cbMap = {};
        }

        _runCommand(method, args) {
            const self = this;

            return new Promise((resolve, reject) =>
                self._invokeRmtMethod(method, args, (error, res) => {
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

                const restError = restResult.error;

                if (restError)
                    return cb(restError);

                const restCode = restResult.restCode;

                if (restCode !== 200) {
                    if (restCode === 401)
                        return cb.call({code: restCode, message: 'Failed to authenticate on node.'});

                    return cb.call({
                        code: restCode,
                        message: restError || 'Failed connect to node and execute REST command.'
                    });
                }

                try {
                    const nodeResponse = JSON.parse(restResult.response);

                    if (nodeResponse.successStatus === 0)
                        return cb(null, nodeResponse.response);

                    switch (nodeResponse.successStatus) {
                        case 1:
                            return cb({code: 500, message: nodeResponse.error});
                        case 2:
                            return cb({code: 401, message: nodeResponse.error});
                        case 3:
                            return cb({code: 403, message: nodeResponse.error});
                        default:
                            return cb(nodeResponse.error);
                    }
                }
                catch (e) {
                    return cb(e);
                }
            };

            this._invokeRmtMethod('executeRest', [uri, params, demo, method, headers, body], _cb);
        }

        /**
         * @param {?String} error
         */
        authResult(error) {
            return this._runCommand('authResult', [error]);
        }

        /**
         * @param {String} driverPath
         * @param {String} driverClass
         * @param {String} url
         * @param {Object} info
         * @returns {Promise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
         */
        metadataSchemas(driverPath, driverClass, url, info) {
            return this._runCommand('schemas', [driverPath, driverClass, url, info]);
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
            return this._runCommand('metadata', [driverPath, driverClass, url, info, schemas, tablesOnly]);
        }

        /**
         * @returns {Promise} Promise on list of jars from driver folder.
         */
        availableDrivers() {
            return this._runCommand('availableDrivers', []);
        }

        /**
         * Run http request
         *
         * @this {AgentServer}
         * @param {String} method Command name.
         * @param {Array} args Command params.
         * @param {Function} callback on finish
         */
        _invokeRmtMethod(method, args, callback) {
            if (this._wsSrv.readyState !== 1) {
                if (callback)
                    callback('org.apache.ignite.agent.AgentException: Connection is closed');

                return;
            }

            const msg = {method, args};

            if (callback) {
                const reqId = this._reqCounter++;

                this._cbMap[reqId] = callback;

                msg.reqId = reqId;
            }

            this._wsSrv.send(JSON.stringify(msg));
        }

        _rmtAuthMessage(msg) {
            const self = this;

            fs.stat('public/agent/ignite-web-agent-1.5.0.final.zip', (errFs, stats) => {
                let relDate = 0;

                if (!errFs)
                    relDate = stats.birthtime.getTime();

                if ((msg.relDate || 0) < relDate)
                    self.authResult('You are using an older version of the agent. Please reload agent archive');

                mongo.Account.findOne({token: msg.token}, (err, account) => {
                    // TODO IGNITE-1379 send error to web master.
                    if (err)
                        self.authResult('Failed to authorize user');
                    else if (!account)
                        self.authResult('Invalid token, user not found');
                    else {
                        self.authResult(null);

                        self._user = account;

                        self._manager._addAgent(account._id, self);

                        self._cluster = new apacheIgnite.Ignite(new AgentServer(self));

                        self._demo = new apacheIgnite.Ignite(new AgentServer(self, true));
                    }
                });
            });
        }

        _rmtCallRes(msg) {
            const callback = this._cbMap[msg.reqId];

            if (!callback)
                return;

            delete this._cbMap[msg.reqId];

            callback(msg.error, msg.response);
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
            this._agents = {};
        }

        /**
         *
         */
        listen(srv) {
            if (this._server)
                throw 'Agent server already started!';

            this._server = srv;

            this._wsSrv = new ws.Server({server: this._server});

            const self = this;

            this._wsSrv.on('connection', (_wsSrv) => new Agent(_wsSrv, self));
        }

        /**
         * @param userId
         * @param {Agent} client
         */
        _removeClient(userId, client) {
            const agents = this._agents[userId];

            if (agents) {
                let idx;

                while ((idx = agents.indexOf(client)) !== -1)
                    agents.splice(idx, 1);

                if (agents.length === 0)
                    delete this._agents[userId];
            }
        }

        /**
         * @param {ObjectId} userId
         * @param {Agent} agent
         */
        _addAgent(userId, agent) {
            let agents = this._agents[userId];

            if (!agents) {
                agents = [];

                this._agents[userId] = agents;
            }

            agents.push(agent);
        }

        /**
         * @param {ObjectId} userId
         * @returns {Agent}
         */
        findClient(userId) {
            const agents = this._agents[userId];

            if (!agents || agents.length === 0)
                return null;

            return agents[0];
        }
    }

    return new AgentManager();
};
