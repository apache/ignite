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

import _ from 'lodash';

ConfigurationResourceService.$inject = ['$http'];

export default function ConfigurationResourceService($http: ng.IHttpService) {
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
