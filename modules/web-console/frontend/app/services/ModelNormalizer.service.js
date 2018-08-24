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

// Service to normalize objects for dirty checks.
export default ['IgniteModelNormalizer', function() {
    /**
     * Normalize object for dirty checks.
     *
     * @param original
     * @param dest
     * @returns {*}
     */
    const normalize = (original, dest) => {
        if (_.isUndefined(original))
            return dest;

        if (_.isObject(original)) {
            _.forOwn(original, (value, key) => {
                if (/\$\$hashKey/.test(key))
                    return;

                const attr = normalize(value);

                if (!_.isNil(attr)) {
                    dest = dest || {};
                    dest[key] = attr;
                }
            });
        } else if (_.isBoolean(original) && original === true)
            dest = original;
        else if ((_.isString(original) && original.length) || _.isNumber(original))
            dest = original;
        else if (_.isArray(original) && original.length)
            dest = _.map(original, (value) => normalize(value, {}));

        return dest;
    };

    return {
        normalize,
        isEqual(prev, cur) {
            return _.isEqual(prev, normalize(cur));
        }
    };
}];
