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

const fs = require('fs');
const path = require('path');

// Fire me up!

const Express = require('express');

module.exports = {
    implements: 'api-server',
    inject: ['settings', 'configure', 'routes'],
    factory(settings, configure, routes) {
        /**
         * Connected agents manager.
         */
        class ApiServer {
            /**
             * @param {Server} srv
             */
            attach(srv) {
                const app = new Express();

                configure.express(app);

                routes.register(app);

                if (settings.packaged) {
                    const staticDir = path.join(process.cwd(), 'libs/frontend');

                    try {
                        fs.accessSync(staticDir, fs.F_OK);

                        app.use('/', Express.static(staticDir));

                        app.get('*', function(req, res) {
                            res.sendFile(path.join(staticDir, 'index.html'));
                        });
                    }
                    catch (e) {
                        console.log(`Failed to find folder with frontend files: ${staticDir}`);
                    }
                }

                // Catch 404 and forward to error handler.
                app.use((req, res) => {
                    if (req.xhr)
                        return res.status(404).send({ error: 'Not Found: ' + req.originalUrl });

                    return res.sendStatus(404);
                });

                // Production error handler: no stacktraces leaked to user.
                app.use((err, req, res) => {
                    res.status(err.status || 500);

                    res.render('error', {
                        message: err.message,
                        error: {}
                    });
                });

                srv.addListener('request', app);

                return app;
            }
        }

        return new ApiServer();
    }
};
