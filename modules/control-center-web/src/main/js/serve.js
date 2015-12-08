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

/**
 * Module dependencies.
 */
var http = require('http');
var https = require('https');
var config = require('./helpers/configuration-loader.js');
var app = require('./app');
var agentManager = require('./agents/agent-manager');

var fs = require('fs');

var debug = require('debug')('ignite-web-console:server');

/**
 * Get port from environment and store in Express.
 */
var port = config.normalizePort(config.get('server:port') || process.env.PORT || 80);

// Create HTTP server.
var server = http.createServer(app);

app.set('port', port);

/**
 * Listen on provided port, on all network interfaces.
 */
server.listen(port);
server.on('error', onError);
server.on('listening', onListening);

if (config.get('server:ssl')) {
    httpsServer = https.createServer({
        key: fs.readFileSync(config.get('server:key')),
        cert: fs.readFileSync(config.get('server:cert')),
        passphrase: config.get('server:keyPassphrase')
    }, app);

    var httpsPort = config.normalizePort(config.get('server:https-port') || 443);

    /**
     * Listen on provided port, on all network interfaces.
     */
    httpsServer.listen(httpsPort);
    httpsServer.on('error', onError);
    httpsServer.on('listening', onListening);
}

/**
 * Start agent server.
 */
var agentServer;

if (config.get('agent-server:ssl')) {
    agentServer = https.createServer({
    key: fs.readFileSync(config.get('agent-server:key')),
    cert: fs.readFileSync(config.get('agent-server:cert')),
    passphrase: config.get('agent-server:keyPassphrase')
  });
}
else {
  agentServer = http.createServer();
}

agentServer.listen(config.get('agent-server:port'));

agentManager.createManager(agentServer);

/**
 * Event listener for HTTP server "error" event.
 */
function onError(error) {
  if (error.syscall !== 'listen') {
    throw error;
  }

  var bind = typeof port === 'string'
    ? 'Pipe ' + port
    : 'Port ' + port;

  // Handle specific listen errors with friendly messages.
  switch (error.code) {
    case 'EACCES':
      console.error(bind + ' requires elevated privileges');
      process.exit(1);
      break;
    case 'EADDRINUSE':
      console.error(bind + ' is already in use');
      process.exit(1);
      break;
    default:
      throw error;
  }
}

/**
 * Event listener for HTTP server "listening" event.
 */
function onListening() {
  var addr = server.address();
  var bind = typeof addr === 'string'
    ? 'pipe ' + addr
    : 'port ' + addr.port;

  console.log('Start listening on ' + bind);
}
