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
 * @param {Client} client connected client
 */
function AgentServer(client) {
    this._client = client;
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

    this._client.executeRest("ignite", params, method, headers, body, function(error, code, message) {
        if (error) {
            callback(error);
            return
        }

        if (code !== 200) {
            if (code === 401) {
                callback.call(null, "Authentication failed. Status code 401.");
            }
            else {
                callback.call(null, "Request failed. Status code " + code);
            }

            return;
        }

        var igniteResponse;

        try {
            igniteResponse = JSON.parse(message);
        }
        catch (e) {
            callback.call(null, e, null);

            return;
        }

        if (igniteResponse.successStatus) {
            callback.call(null, igniteResponse.error, null)
        }
        else {
            callback.call(null, null, igniteResponse.response);
        }
    });
};

exports.AgentServer = AgentServer;
