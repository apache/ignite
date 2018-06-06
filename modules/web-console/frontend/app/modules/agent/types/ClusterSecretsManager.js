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

import {ClusterSecrets} from './ClusterSecrets';

export class ClusterSecretsManager {
    /** @type {Map<String, ClusterSecrets>} */
    memoryCache = new Map();

    /**
     * @param {String} clusterId
     * @private
     */
    _has(clusterId) {
        return this.memoryCache.has(clusterId);
    }

    /**
     * @param {String} clusterId
     * @private
     */
    _get(clusterId) {
        return this.memoryCache.get(clusterId);
    }

    /**
     * @param {String} clusterId
     */
    get(clusterId) {
        if (this._has(clusterId))
            return this._get(clusterId);

        const secrets = new ClusterSecrets();

        this.put(clusterId, secrets);

        return secrets;
    }

    /**
     * @param {String} clusterId
     * @param {ClusterSecrets} secrets
     */
    put(clusterId, secrets) {
        this.memoryCache.set(clusterId, secrets);
    }

    /**
     * @param {String} clusterId
     */
    reset(clusterId) {
        const secrets = this._get(clusterId);

        secrets && secrets.resetCredentials();
    }
}
