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

var request = require('supertest'),
    should = require('should'),
    app = require('../../app'),
    fs = require('fs'),
    https = require('https'),
    config = require('../../helpers/configuration-loader.js'),
    agentManager = require('../../agents/agent-manager');

/**
 * Create HTTP server.
 */
/**
 * Start agent server.
 */
var agentServer = https.createServer({
    key: fs.readFileSync(config.get('monitor:server:key')),
    cert: fs.readFileSync(config.get('monitor:server:cert')),
    passphrase: config.get('monitor:server:keyPassphrase')
});

agentServer.listen(config.get('monitor:server:port'));

agentManager.createManager(agentServer);

describe('request from agent', function() {
    var agent = request.agent(app);

    before(function (done) {
        this.timeout(10000);

        agent
            .post('/login')
            .send({email: 'test@test.com', password: 'test'})
            .expect(302)
            .end(function (err) {
                if (err)
                    throw err;

                setTimeout(done, 5000);
            });
    });

    it('should return topology snapshot', function(done){
        agent
            .post('/agent/topology')
            .send({})
            .end(function(err, nodes) {
                if (err) {
                    console.log(err.response.text);

                    throw err;
                }

                console.log(nodes);

                done();
            });
    });

    //it('should query result', function(done){
    //    agent
    //        .post('/agent/query')
    //        .send({
    //            username: 'nva',
    //            password: 'nva.141',
    //            host: 'localhost',
    //            port: '5432',
    //            dbName: 'ggmonitor'
    //        })
    //        .end(function(err, res) {
    //            if (err)
    //                throw err;
    //
    //            done();
    //        });
    //});
});
