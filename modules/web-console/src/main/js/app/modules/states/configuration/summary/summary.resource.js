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

export default ['ConfigurationSummaryResource', ['$q', '$http', ($q, $http) => {
    const api = '/api/v1/configuration/clusters/list';

    return {
        read() {
            return $http
                .post(api)
                .then(({data}) => data)
                .then(({clusters, caches, igfss}) => {
                    if (!clusters || !clusters.length)
                        return {};

                    _.forEach(clusters, (cluster) => {
                        cluster.igfss = _.filter(igfss, ({_id}) => _.includes(cluster.igfss, _id));
                        cluster.caches = _.filter(caches, ({_id}) => _.includes(cluster.caches, _id));
                    });

                    return {clusters};
                })
                .catch((err) => $q.reject(err));
        }
    };
}]];
