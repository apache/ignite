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

module.exports = {
    implements: 'api-server',
    inject: ['require(express)', 'configure', 'routes']
};

module.exports.factory = function(Express, configure, routes) {
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

            // Catch 404 and forward to error handler.
            app.use((req, res, next) => {
                const err = new Error('Not Found: ' + req.originalUrl);

                err.status = 404;

                next(err);
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
};
