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

// Load required modules.
var http = require('http');
var express = require('express');

// Parse command line arguments.
var port = process.argv[2] || 8888;
var dataDir = process.argv[3] || '.';
var rmtAddr = process.argv[4] || '10.1.10.210';

// Create client and server.
var client = http.createClient(80, rmtAddr);
var server = express.createServer();
var baseUrl = '/gg-loadtest';

client.on('error', function(err) {
    console.log('Client error: ' + err);
});

server.use(express.basicAuth('gridgain', 'gridgain2012'));

server.get(baseUrl, function(req, resp) {
    resp.sendfile(dataDir + baseUrl + '/index.html');
});

server.get(baseUrl + '/*.html', replyWithLocalFile);
server.get(baseUrl + '/*.css', replyWithLocalFile);
server.get(baseUrl + '/*.js', replyWithLocalFile);

// Get all data files from remote server.
server.get(baseUrl + '/*.csv', replyWithRemoteFile);
server.get(baseUrl + '/*.csv-*', replyWithRemoteFile);

// Ger branches list from remote server.
server.get(baseUrl + '/branches', function(req, resp) {
    req.url = baseUrl + '/'; // Change to root URL.

    replyWithRemoteFile(req, resp);
});

server.listen(port);

console.log('HTTP server started on port %s.', port);
console.log('Remote server is %s.', rmtAddr);

/*
 * Replies with a file from local file system.
 *
 * @param req Request.
 * @param resp Response.
 */
function replyWithLocalFile(req, resp) {
    resp.sendfile(dataDir + '/' + req.url);
}

/*
 * Replies with a file from remote server.
 *
 * @param req Request.
 * @param resp Response.
 */
function replyWithRemoteFile(req, resp) {
    var clntReq = client.request('GET', req.url);

    clntReq.on('response', function (clntResp) {
        clntResp.on('data', function (chunk) {
            resp.write(chunk);
        });

        clntResp.on('end', function (chunk) {
            resp.end(chunk);
        });
    });

    clntReq.on('error', function(err) {
        resp.status(404).send('Failed to request from remote server: ' + err);
    });

    clntReq.end();
}
