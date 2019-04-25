/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    inject: ['routes/public', 'routes/admin', 'routes/profiles', 'routes/demo', 'routes/clusters', 'routes/domains',
        'routes/caches', 'routes/igfss', 'routes/notebooks', 'routes/downloads', 'routes/configurations', 'routes/activities']
};

module.exports.factory = function(publicRoute, adminRoute, profilesRoute, demoRoute,
    clustersRoute, domainsRoute, cachesRoute, igfssRoute, notebooksRoute, downloadsRoute, configurationsRoute, activitiesRoute) {
    return {
        register: (app) => {
            const _mustAuthenticated = (req, res, next) => {
                if (req.isAuthenticated())
                    return next();

                res.status(401).send('Access denied. You are not authorized to access this page.');
            };

            const _adminOnly = (req, res, next) => {
                if (req.isAuthenticated() && req.user.admin)
                    return next();

                res.status(401).send('Access denied. You are not authorized to access this page.');
            };

            // Registering the standard routes.
            // NOTE: Order is important!
            app.use('/api/v1/', publicRoute);
            app.use('/api/v1/admin', _mustAuthenticated, _adminOnly, adminRoute);
            app.use('/api/v1/profile', _mustAuthenticated, profilesRoute);
            app.use('/api/v1/demo', _mustAuthenticated, demoRoute);

            app.all('/api/v1/configuration/*', _mustAuthenticated);

            app.use('/api/v1/configuration/clusters', clustersRoute);
            app.use('/api/v1/configuration/domains', domainsRoute);
            app.use('/api/v1/configuration/caches', cachesRoute);
            app.use('/api/v1/configuration/igfs', igfssRoute);
            app.use('/api/v1/configuration', configurationsRoute);

            app.use('/api/v1/notebooks', _mustAuthenticated, notebooksRoute);
            app.use('/api/v1/downloads', _mustAuthenticated, downloadsRoute);
            app.use('/api/v1/activities', _mustAuthenticated, activitiesRoute);
        }
    };
};
