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

import {nonEmpty} from 'app/utils/lodashMixins';

export class ClusterSecrets {
    /** @type {String} */
    user;

    /** @type {String} */
    password;

    /** @type {String} */
    sessionToken;

    constructor() {
        this.user = 'ignite';
    }

    hasCredentials() {
        return nonEmpty(this.user) && nonEmpty(this.password);
    }

    resetCredentials() {
        this.resetSessionToken();

        this.password = null;
    }

    resetSessionToken() {
        this.sessionToken = null;
    }

    /**
     * @return {{sessionToken: String}|{'user': String, 'password': String}}
     */
    getCredentials() {
        const { sessionToken } = this;

        if (sessionToken)
            return { sessionToken };

        const { user, password } = this;

        return { user, password };
    }
}
