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
    implements: 'services/configurations',
    inject: ['require(lodash)', 'mongo', 'services/spaces', 'services/clusters', 'services/caches', 'services/domains', 'services/igfss']
};

/**
 * @param _
 * @param mongo
 * @param {SpacesService} spacesService
 * @param {ClustersService} clustersService
 * @param {CachesService} cachesService
 * @param {DomainsService} domainsService
 * @param {IgfssService} igfssService
 * @returns {ConfigurationsService}
 */
module.exports.factory = (_, mongo, spacesService, clustersService, cachesService, domainsService, igfssService) => {
    class ConfigurationsService {
        static list(userId, demo) {
            let spaces;

            return spacesService.spaces(userId, demo)
                .then((_spaces) => {
                    spaces = _spaces;

                    return spaces.map((space) => space._id);
                })
                .then((spaceIds) => Promise.all([
                    clustersService.listBySpaces(spaceIds),
                    domainsService.listBySpaces(spaceIds),
                    cachesService.listBySpaces(spaceIds),
                    igfssService.listBySpaces(spaceIds)
                ]))
                .then(([clusters, domains, caches, igfss]) => ({clusters, domains, caches, igfss, spaces}));
        }
    }

    return ConfigurationsService;
};
