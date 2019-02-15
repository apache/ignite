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

import {StateService} from '@uirouter/angularjs';

interface IActivityDataResponse {
    action: string,
    amount: number,
    date: string,
    group: string,
    owner: string,
    _id: string
}

export default class ActivitiesData {
    static $inject = ['$http', '$state'];

    constructor(private $http: ng.IHttpService, private $state: StateService) {}

    /**
     * Posts activity to backend, sends current state if no options specified
     */
    // For some reason, Babel loses this after destructuring, the arrow helps with that
    post = (options: {group?: string, action?: string} = {}) => {
        let { group, action } = options;

        // TODO IGNITE-5466: since upgrade to UIRouter 1, "url.source" is undefined.
        // Actions like that won't be saved to DB. Think of a better solution later.
        action = action || this.$state.$current.url.source || '';
        group = group || (action.match(/^\/([^/]+)/) || [])[1];

        return this.$http.post<IActivityDataResponse>('/api/v1/activities/page', { group, action })
            .catch(() => {
                // No-op.
            });
    }
}
