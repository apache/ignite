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

import _ from 'lodash';

/**
 * @param {ng.IHttpService} $http
 */
export default function ConfigurationResourceService($http) {
    return {
        read() {
            return $http.get('/api/v1/configuration/list')
                .then(({data}) => data)
                .catch(({data}) => Promise.reject(data));
        },
        populate(data) {
            const {spaces, clusters, caches, igfss, domains} = _.cloneDeep(data);

            _.forEach(clusters, (cluster) => {
                cluster.caches = _.filter(caches, ({_id}) => _.includes(cluster.caches, _id));

                _.forEach(cluster.caches, (cache) => {
                    cache.domains = _.filter(domains, ({_id}) => _.includes(cache.domains, _id));

                    if (_.get(cache, 'nodeFilter.kind') === 'IGFS')
                        cache.nodeFilter.IGFS.instance = _.find(igfss, {_id: cache.nodeFilter.IGFS.igfs});
                });

                cluster.igfss = _.filter(igfss, ({_id}) => _.includes(cluster.igfss, _id));
            });

            return Promise.resolve({spaces, clusters, caches, igfss, domains});
        }
    };
}

ConfigurationResourceService.$inject = ['$http'];
