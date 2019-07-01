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

export default class ActivitiesData {
    static $inject = ['$http', '$state'];

    /**
     * @param {ng.IHttpService} $http
     * @param {uirouter.StateService} $state
     */
    constructor($http, $state) {
        this.$http = $http;
        this.$state = $state;
    }

    post(options = {}) {
        let { group, action } = options;

        // TODO IGNITE-5466: since upgrade to UIRouter 1, "url.source" is undefined.
        // Actions like that won't be saved to DB. Think of a better solution later.
        action = action || this.$state.$current.url.source || '';
        group = group || (action.match(/^\/([^/]+)/) || [])[1];

        return this.$http.post('/api/v1/activities/page', { group, action })
            .catch(() => {
                // No-op.
            });
    }
}
