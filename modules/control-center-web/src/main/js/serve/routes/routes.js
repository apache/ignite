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
    implements: 'routes',
    inject: [
        'public-routes',
        'admin-routes',
        'profile-routes',
        'demo-routes',
        'clusters-routes',
        'domains-routes',
        'caches-routes',
        'igfs-routes',
        'notebooks-routes',
        'agent-routes',
        'ignite_modules/routes:*' // Loads all routes modules of all plugins
    ]
};

module.exports.factory = function(publicRoutes, adminRoutes, profileRoutes, demoRoutes,
    clusterRoutes, domainRoutes, cacheRoutes, igfsRoutes, notebookRoutes, agentRoutes, pluginRoutes) {
    return {
        register: (app) => {
            app.all('*', (req, res, next) => {
                req.currentUserId = () => {
                    if (!req.user)
                        return null;

                    if (req.session.viewedUser && req.user.admin)
                        return req.session.viewedUser._id;

                    return req.user._id;
                };

                next();
            });

            const _mustAuthenticated = (req, res, next) => req.isAuthenticated() ? next() : res.redirect('/');

            const _adminOnly = (req, res, next) => req.isAuthenticated() && req.user.admin ? next() : res.sendStatus(403);

            // Registering the standard routes
            app.use('/', publicRoutes);
            app.use('/admin', _mustAuthenticated, _adminOnly, adminRoutes);
            app.use('/profile', _mustAuthenticated, profileRoutes);
            app.use('/demo', _mustAuthenticated, demoRoutes);

            app.all('/configuration/*', _mustAuthenticated);

            app.use('/configuration/clusters', clusterRoutes);
            app.use('/configuration/domains', domainRoutes);
            app.use('/configuration/caches', cacheRoutes);
            app.use('/configuration/igfs', igfsRoutes);

            app.use('/notebooks', _mustAuthenticated, notebookRoutes);
            app.use('/agent', _mustAuthenticated, agentRoutes);

            // Registering the routes of all plugin modules
            for (const name in pluginRoutes) {
                if (pluginRoutes.hasOwnProperty(name))
                    pluginRoutes[name].register(app, _mustAuthenticated, _adminOnly);
            }

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
        }
    };
};
