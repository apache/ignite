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

import {nonEmpty} from 'app/utils/lodashMixins';

export class ClusterSecrets {
    /** @type {String} */
    user;

    /** @type {String} */
    password;

    /** @type {Number} */
    sessionTtl;

    /** @type {String} */
    sessionId;

    constructor() {
        this.user = 'ignite';
    }

    hasCredentials() {
        return nonEmpty(this.user) && nonEmpty(this.password);
    }

    resetCredentials() {
        this.resetSessionId();

        this.password = null;
    }

    resetSessionId() {
        this.sessionId = null;
    }

    /**
     * @return {{sessionId: String}|{user: String, password: String, sessionTtl: Number}}
     */
    asParams() {
        if (this.sessionId)
            return {sessionId: this.sessionId};

        return _.pick(this, ['user', 'password', 'sessionTtl']);
    }
}
