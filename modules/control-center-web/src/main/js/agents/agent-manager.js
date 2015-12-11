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
 * @return {Client}
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

/**
 * @param {String} path
 * @param {Object} params
 * @param {String} [method]
 * @param {Object} [headers]
 * @param {String} [body]
 * @param {Function} [cb] Callback. Take 3 arguments: {String} error, {number} httpCode, {string} response.
 */
Client.prototype.executeRest = function(path, params, method, headers, body, cb) {
    if (typeof(params) != 'object')
        throw '"params" argument must be an object';

    if (typeof(cb) != 'function')
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

    var newArgs = argsToArray(arguments);

    newArgs[5] = function(ex, res) {
        if (ex)
            cb(ex.message);
        else
            cb(null, res.code, res.message)
    };

    this._invokeRmtMethod('executeRest', newArgs);
};

/**
 * @param {string} error
 */
Client.prototype.authResult = function(error) {
    this._invokeRmtMethod('authResult', arguments)
};

/**
 * @param {String} jdbcDriverJarPath
 * @param {String} jdbcDriverClass
 * @param {String} jdbcUrl
 * @param {Object} jdbcInfo
 * @param {Function} cb Callback. Take two arguments: {Object} exception, {Object} result.
 * @return {Array} List of tables (see org.apache.ignite.schema.parser.DbTable java class)
 */
Client.prototype.metadataSchemas = function(jdbcDriverJarPath, jdbcDriverClass, jdbcUrl, jdbcInfo, cb) {
    this._invokeRmtMethod('schemas', arguments)
};

/**
 * @param {String} jdbcDriverJarPath
 * @param {String} jdbcDriverClass
 * @param {String} jdbcUrl
 * @param {Object} jdbcInfo
 * @param {Array} schemas
 * @param {Boolean} tablesOnly
 * @param {Function} cb Callback. Take two arguments: {Object} exception, {Object} result.
 * @return {Array} List of tables (see org.apache.ignite.schema.parser.DbTable java class)
 */
Client.prototype.metadataTables = function(jdbcDriverJarPath, jdbcDriverClass, jdbcUrl, jdbcInfo, schemas, tablesOnly, cb) {
    this._invokeRmtMethod('metadata', arguments)
};

/**
 * @param {Function} cb Callback. Take two arguments: {Object} exception, {Object} result.
 * @return {Array} List of jars from driver folder.
 */
Client.prototype.availableDrivers = function(cb) {
    this._invokeRmtMethod('availableDrivers', arguments)
};

Client.prototype._invokeRmtMethod = function(methodName, args) {
    var cb = null;

    var m = argsToArray(args);

    if (m.length > 0 && typeof m[m.length - 1] == 'function')
        cb = m.pop();

    if (this._ws.readyState != 1) {
        if (cb)
            cb({type: 'org.apache.ignite.agent.AgentException', message: 'Connection is closed'});

        return
    }

    var msg = {
        mtdName: methodName,
        args: m
    };

    if (cb) {
        var reqId = this._reqCounter++;

        this._cbMap[reqId] = cb;

        msg.reqId = reqId;
    }

    this._ws.send(JSON.stringify(msg))
};

Client.prototype._rmtAuthMessage = function(msg) {
    var self = this;

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

            self._ignite = new apacheIgnite.Ignite(new AgentServer(self));
        }
    });
};

Client.prototype._rmtCallRes = function(msg) {
    var cb = this._cbMap[msg.reqId];

    if (!cb) return;

    delete this._cbMap[msg.reqId];

    if (msg.ex)
        cb(msg.ex);
    else
        cb(null, msg.res);
};

/**
 * @return {Ignite}
 */
Client.prototype.ignite = function() {
    return this._ignite;
};

function removeFromArray(arr, val) {
    var idx;

    while ((idx = arr.indexOf(val)) !== -1) {
        arr.splice(idx, 1);
    }
}

/**
 * @param args
 * @returns {Array}
 */
function argsToArray(args) {
    var res = [];

    for (var i = 0; i < args.length; i++)
        res.push(args[i])

    return res;
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
 * @return {AgentManager}
 */
exports.getAgentManager = function() {
    return manager;
};
