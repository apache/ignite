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

var _ = require('lodash');

/**
 * Creates an instance of server for Ignite
 *
 * @constructor
 * @this {AgentServer}
 * @param {Client} client Connected client
 * @param {Boolean} demo Use demo node for request
 */
function AgentServer(client, demo) {
    this._client = client;
    this._demo = !!demo;
}

/**
 * Run http request
 *
 * @this {AgentServer}
 * @param {Command} cmd Command
 * @param {callback} callback on finish
 */
AgentServer.prototype.runCommand = function(cmd, callback) {
    var params = {cmd: cmd.name()};

    _.forEach(cmd._params, function (p) {
        params[p.key] = p.value;
    });

    var body = undefined;

    var headers = undefined;

    var method = 'GET';

    if (cmd._isPost()) {
        body = cmd.postData();

        method = 'POST';

        headers = {'JSONObject': 'application/json'};
    }

    this._client.executeRest("ignite", params, this._demo, method, headers, body, callback);
};

exports.AgentServer = AgentServer;
