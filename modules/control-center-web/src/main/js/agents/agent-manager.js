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

var WebSocketServer = require('ws').Server;

var apacheIgnite = require('apache-ignite');

var db = require('../db');

var AgentServer = require('./agent-server').AgentServer;

/**
 * @constructor
 */
function AgentManager(srv) {
    this._clients = {};

    this._server = srv;

    this._wss = new WebSocketServer({ server: this._server });

    var self = this;

    this._wss.on('connection', function(ws) {
        new Client(ws, self);
    });
}

/**
 * @param userId
 * @param {Client} client
 */
AgentManager.prototype._removeClient = function(userId, client) {
    var connections = this._clients[userId];

    if (connections) {
        removeFromArray(connections, client);

        if (connections.length == 0)
            delete this._clients[userId];
    }
};

/**
 * @param userId
 * @param {Client} client
 */
AgentManager.prototype._addClient = function(userId, client) {
    var existingConnections = this._clients[userId];

    if (!existingConnections) {
        existingConnections = [];

        this._clients[userId] = existingConnections;
    }

    existingConnections.push(client);
};

/**
 * @param userId
 * @returns {Client}
 */
AgentManager.prototype.findClient = function(userId) {
    var clientsList = this._clients[userId];

    if (!clientsList || clientsList.length == 0)
        return null;

    return clientsList[0];
};

/**
 * @constructor
 * @param {AgentManager} manager
 * @param {WebSocket} ws
 */
function Client(ws, manager) {
    var self = this;

    this._manager = manager;
    this._ws = ws;

    ws.on('close', function() {
        if (self._user) {
            self._manager._removeClient(self._user._id, self);
        }
    });

    ws.on('message', function (msgStr) {
        var msg = JSON.parse(msgStr);

        self['_rmt' + msg.type](msg);
    });

    this._reqCounter = 0;

    this._cbMap = {};
}

Client.prototype._runCommand = function(method, args) {
    var self = this;

    return new Promise(function(resolve, reject) {
        self._invokeRmtMethod(method, args, function(error, res) {
            if (error != null)
                return reject(error);

            resolve(res);
        });
    });
};

/**
 * @param {String} uri
 * @param {Object} params
 * @param {Boolean} demo
 * @param {String} [method]
 * @param {Object} [headers]
 * @param {String} [body]
 * @param {callback} [callback] Callback. Take 3 arguments: {Number} successStatus, {String} error,  {String} response.
 */
Client.prototype.executeRest = function(uri, params, demo, method, headers, body, callback) {
    if (typeof(params) != 'object')
        throw '"params" argument must be an object';

    if (typeof(callback) != 'function')
        throw 'callback must be a function';

    if (body && typeof(body) != 'string')
        throw 'body must be a string';

    if (headers && typeof(headers) != 'object')
        throw 'headers must be an object';

    if (!method)
        method = 'GET';
    else
        method = method.toUpperCase();

    if (method != 'GET' && method != 'POST')
        throw 'Unknown HTTP method: ' + method;

    const cb = function(error, restResult) {
        if (error)
            return callback(error);

        const restError = restResult.error;

        if (restError)
            return callback(restError);

        const restCode = restResult.restCode;

        if (restCode !== 200) {
            if (restCode === 401)
                return callback.call({code: restCode, message: "Failed to authenticate on node."});

            return callback.call({code: restCode, message: restError || "Failed connect to node and execute REST command."});
        }

        try {
            var nodeResponse = JSON.parse(restResult.response);

            if (nodeResponse.successStatus === 0)
                return callback(null, nodeResponse.response);

            switch (nodeResponse.successStatus) {
                case 1:
                    return callback({code: 500, message: nodeResponse.error});
                case 2:
                    return callback({code: 401, message: nodeResponse.error});
                case 3:
                    return callback({code: 403, message: nodeResponse.error});
            }

            callback(nodeResponse.error);
        }
        catch (e) {
            callback(e);
        }
    };

    this._invokeRmtMethod('executeRest', [uri, params, demo, method, headers, body], cb);
};

/**
 * @param {string} error
 */
Client.prototype.authResult = function(error) {
    return this._runCommand('authResult', [].slice.call(arguments));
};

/**
 * @param {String} driverPath
 * @param {String} driverClass
 * @param {String} url
 * @param {Object} info
 * @returns {Promise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
 */
Client.prototype.metadataSchemas = function(driverPath, driverClass, url, info) {
    return this._runCommand('schemas', [].slice.call(arguments));
};

/**
 * @param {String} driverPath
 * @param {String} driverClass
 * @param {String} url
 * @param {Object} info
 * @param {Array} schemas
 * @param {Boolean} tablesOnly
 * @returns {Promise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
 */
Client.prototype.metadataTables = function(driverPath, driverClass, url, info, schemas, tablesOnly) {
    return this._runCommand('metadata', [].slice.call(arguments));
};

/**
 * @returns {Promise} Promise on list of jars from driver folder.
 */
Client.prototype.availableDrivers = function() {
    return this._runCommand('availableDrivers', [].slice.call(arguments));
};

/**
 * Run http request
 *
 * @this {AgentServer}
 * @param {String} method Command name.
 * @param {Array} args Command params.
 * @param {Function} callback on finish
 */
Client.prototype._invokeRmtMethod = function(method, args, callback) {
    if (this._ws.readyState != 1) {
        if (callback)
            callback('org.apache.ignite.agent.AgentException: Connection is closed');

        return;
    }

    var msg = {
        method: method,
        args: args
    };

    if (callback) {
        var reqId = this._reqCounter++;

        this._cbMap[reqId] = callback;

        msg.reqId = reqId;
    }

    this._ws.send(JSON.stringify(msg))
};

Client.prototype._rmtAuthMessage = function(msg) {
    var self = this;

    var fs = require('fs');

    fs.stat('public/agent/ignite-web-agent-1.5.0.final.zip', function(err, stats) {
        var relDate = 0;

        if (!err)
            relDate = stats.birthtime;

        if ((msg.relDate || 0) < relDate)
            self.authResult('Used old version on agent. Please reload agent archive');

        db.Account.findOne({ token: msg.token }, function (err, account) {
            if (err) {
                self.authResult('Failed to authorize user');
                // TODO IGNITE-1379 send error to web master.
            }
            else if (!account)
                self.authResult('Invalid token, user not found');
            else {
                self.authResult(null);

                self._user = account;

                self._manager._addClient(account._id, self);

                self._cluster = new apacheIgnite.Ignite(new AgentServer(self));

                self._demo = new apacheIgnite.Ignite(new AgentServer(self, true));
            }
        });
    });
};

Client.prototype._rmtCallRes = function(msg) {
    var callback = this._cbMap[msg.reqId];

    if (!callback) return;

    delete this._cbMap[msg.reqId];

    callback(msg.error, msg.response);
};

/**
 * @returns {Ignite}
 */
Client.prototype.ignite = function(demo) {
    return demo ? this._demo : this._cluster;
};

function removeFromArray(arr, val) {
    var idx;

    while ((idx = arr.indexOf(val)) !== -1) {
        arr.splice(idx, 1);
    }
}

exports.AgentManager = AgentManager;

/**
 * @type {AgentManager}
 */
var manager = null;

exports.createManager = function(srv) {
    if (manager)
        throw 'Agent manager already cleared!';

    manager = new AgentManager(srv);
};

/**
 * @returns {AgentManager}
 */
exports.getAgentManager = function() {
    return manager;
};
