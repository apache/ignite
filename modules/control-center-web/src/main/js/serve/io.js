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

// Fire me up!

function _onAuthorizeSuccess(data, accept){
    accept(null, true);
};

function _onAuthorizeFail(data, message, error, accept){
    if(error)
        throw new Error(message);
    accept(null, false);
};

module.exports = {
    implements: 'io',
    inject: ['require(socket.io)', 'require(passport.socketio)', 'require(cookie-parser)', 'http', 'settings', 'store']
};

module.exports.factory = function(socketio, passportSocketIo, cookieParser, server, settings, store) {
    const io = socketio.listen(server);

    io.use(passportSocketIo.authorize({
        cookieParser: cookieParser,
        key:         'connect.sid',             // the name of the cookie where express/connect stores its session_id
        secret:      settings.sessionSecret,    // the session_secret to parse the cookie
        store:       store,                     // we NEED to use a sessionstore. no memorystore please
        success:     _onAuthorizeSuccess,       // *optional* callback on success - read more below
        fail:        _onAuthorizeFail           // *optional* callback on fail/error - read more below
    }));

    io.sockets.on('connection', function (socket) {
        // var req = socket.client.request;

        console.log('connection');

        socket.on('agent:ping', function() {
            console.log('agent:ping');
        });

        socket.emit('agent:success', { data: 'success'});
        socket.emit('agent:error', {data: 'error'});
    });

    return io;
};
