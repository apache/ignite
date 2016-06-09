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

export default ['$cleanup', () => {
    const cleanup = (original, dist) => {
        if (_.isUndefined(original))
            return dist;

        if (_.isObject(original)) {
            _.forOwn(original, (value, key) => {
                if (/\$\$hashKey/.test(key))
                    return;

                const attr = cleanup(value);

                if (!_.isNil(attr)) {
                    dist = dist || {};
                    dist[key] = attr;
                }
            });
        } else if ((_.isString(original) && original.length) || _.isNumber(original) || _.isBoolean(original))
            dist = original;
        else if (_.isArray(original) && original.length)
            dist = _.map(original, (value) => cleanup(value, {}));

        return dist;
    };

    return cleanup;
}];
