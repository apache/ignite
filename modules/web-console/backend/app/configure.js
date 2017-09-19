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

/**
 * Module for configuration express and websocket server.
 */
module.exports = {
    implements: 'configure',
    inject: ['require(lodash)', 'require(morgan)', 'require(cookie-parser)', 'require(body-parser)',
        'require(express-session)', 'require(connect-mongo)', 'require(passport)', 'require(passport.socketio)', 'settings', 'mongo', 'middlewares:*']
};

module.exports.factory = function(_, logger, cookieParser, bodyParser, session, connectMongo, passport, passportSocketIo, settings, mongo, apis) {
    const _sessionStore = new (connectMongo(session))({mongooseConnection: mongo.connection});

    return {
        express: (app) => {
            app.use(logger('dev', {
                skip: (req, res) => res.statusCode < 400
            }));

            _.forEach(apis, (api) => app.use(api));

            app.use(cookieParser(settings.sessionSecret));

            app.use(bodyParser.json({limit: '50mb'}));
            app.use(bodyParser.urlencoded({limit: '50mb', extended: true}));

            app.use(session({
                secret: settings.sessionSecret,
                resave: false,
                saveUninitialized: true,
                unset: 'destroy',
                cookie: {
                    expires: new Date(Date.now() + settings.cookieTTL),
                    maxAge: settings.cookieTTL
                },
                store: _sessionStore
            }));

            app.use(passport.initialize());
            app.use(passport.session());

            passport.serializeUser(mongo.Account.serializeUser());
            passport.deserializeUser(mongo.Account.deserializeUser());

            passport.use(mongo.Account.createStrategy());
        },
        socketio: (io) => {
            const _onAuthorizeSuccess = (data, accept) => {
                accept(null, true);
            };

            const _onAuthorizeFail = (data, message, error, accept) => {
                accept(null, false);
            };

            io.use(passportSocketIo.authorize({
                cookieParser,
                key: 'connect.sid', // the name of the cookie where express/connect stores its session_id
                secret: settings.sessionSecret, // the session_secret to parse the cookie
                store: _sessionStore, // we NEED to use a sessionstore. no memorystore please
                success: _onAuthorizeSuccess, // *optional* callback on success - read more below
                fail: _onAuthorizeFail // *optional* callback on fail/error - read more below
            }));
        }
    };
};
