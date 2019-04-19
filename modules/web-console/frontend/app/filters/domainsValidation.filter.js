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

import _ from 'lodash';

/**
 * @param {ReturnType<typeof import('../services/LegacyUtils.service').default>} LegacyUtils [description]
 */
export default function factory(LegacyUtils) {
    /**
     * Filter domain models with key fields configuration.
     * @template T
     * @param {Array<T>} domains
     * @param {boolean} valid
     * @param {boolean} invalid
     */
    const filter = (domains, valid, invalid) => {
        if (valid && invalid)
            return domains;

        /** @type {Array<T>} */
        const out = [];

        _.forEach(domains, function(domain) {
            const _valid = !LegacyUtils.domainForStoreConfigured(domain) || LegacyUtils.isJavaBuiltInClass(domain.keyType) || !_.isEmpty(domain.keyFields);

            if (valid && _valid || invalid && !_valid)
                out.push(domain);
        });

        return out;
    };

    return filter;
}

factory.$inject = ['IgniteLegacyUtils'];
