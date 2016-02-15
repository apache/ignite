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

import angular from 'angular';

const isArray = angular.isArray;
const isDefined = angular.isDefined;
const isNumber = angular.isNumber;
const isObject = angular.isObject;
const isString = angular.isString;
const isUndefined = angular.isUndefined;
const isBoolean = (val) => typeof val === 'boolean';

export default ['$cleanup', () => {
    const cleanup = (original, dist) => {
        if (isUndefined(original))
            return dist;

        if (isObject(original)) {
            let key;

            for (key in original) {
                if (original.hasOwnProperty(key)) {
                    const attr = cleanup(original[key]);

                    if (isDefined(attr)) {
                        dist = dist || {};
                        dist[key] = attr;
                    }
                }
            }
        } else if ((isString(original) && original.length) || isNumber(original) || isBoolean(original))
            dist = original;
        else if (isArray(original) && original.length) {
            dist = [];

            let i = 0;
            const l = original.length;

            for (; i < l; i++)
                dist[i] = cleanup(original[i], {});
        }

        return dist;
    };

    return cleanup;
}];
