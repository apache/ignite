/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
