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

export default class ArtifactVersionChecker {
    /**
     * Compare two numbers.
     *
     * @param a {Number} First number to compare
     * @param b {Number} Second number to compare.
     * @return {Number} 1 when a is greater then b, -1 when b is greater then a, 0 when a and b is equal.
     */
    static _numberComparator(a, b) {
        return a > b ? 1 : a < b ? -1 : 0;
    }

    /**
     * Compare to version.
     *
     * @param {Object} a first compared version.
     * @param {Object} b second compared version.
     * @returns {Number} 1 if a > b, 0 if versions equals, -1 if a < b
     */
    static _compare(a, b) {
        for (let i = 0; i < a.length && i < b.length; i++) {
            const res = this._numberComparator(a[i], b[i]);

            if (res !== 0)
                return res;
        }

        return 0;
    }

    /**
     * Tries to parse JDBC driver version.
     *
     * @param {String} ver - String representation of version.
     * @returns {Number[]} - Array of version parts.
     */
    static _parse(ver) {
        return _.map(ver.split(/[.-]/), (v) => {
            return v.startsWith('jre') ? parseInt(v.substring(3), 10) : parseInt(v, 10);
        });
    }

    /**
     * Stay only latest versions of the same dependencies.
     *
     * @param deps Array of dependencies.
     */
    static latestVersions(deps) {
        return _.map(_.values(_.groupBy(_.uniqWith(deps, _.isEqual), (dep) => dep.groupId + dep.artifactId)), (arr) => {
            if (arr.length > 1) {
                try {
                    return _.reduce(arr, (resDep, dep) => {
                        if (this._compare(this._parse(dep.version), this._parse(resDep.version)) > 0)
                            return dep;

                        return resDep;
                    });
                }
                catch (err) {
                    return _.last(_.sortBy(arr, 'version'));
                }
            }

            return arr[0];
        });
    }
}
