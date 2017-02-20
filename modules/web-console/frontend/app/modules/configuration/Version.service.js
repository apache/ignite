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

/**
 * Utility service for version parsing and comparing
 */
const VERSION_MATCHER = /(\d+)\.(\d+)\.(\d+)([-.]([^0123456789][^-]+)(-SNAPSHOT)?)?(-(\d+))?(-([\da-f]+))?/i;

const numberComparator = (a, b) => a > b ? 1 : a < b ? -1 : 0;

export default class IgniteVersion {
    /** Current product version. */
    static ignite = '1.8.0';

    /**
     * Tries to parse product version from it's string representation.
     *
     * @param {String} ver - String representation of version.
     * @returns {{major: Number, minor: Number, maintenance: Number, stage: String, revTs: Number, revHash: String}} - Object that contains product version fields.
     */
    parse(ver) {
        // Development or built from source ZIP.
        ver = ver.replace(/(-DEV|-n\/a)$/i, '');

        const [, major, minor, maintenance, stage, ...chunks] = ver.match(VERSION_MATCHER);

        return {
            major: parseInt(major, 10),
            minor: parseInt(minor, 10),
            maintenance: parseInt(maintenance, 10),
            stage: (stage || '').substring(1),
            revTs: chunks[2] ? parseInt(chunks[3], 10) : 0,
            revHash: chunks[4] ? chunks[5] : null
        };
    }

    /**
     * Compare to version.
     * @param a {String} first compared version.
     * @param b {String} second compared version.
     * @returns {Number} 1 if a > b, 0 if versions equals, -1 if a < b
     */
    compare(a, b) {
        const pa = this.parse(a);
        const pb = this.parse(b);

        let res = numberComparator(pa.major, pb.major);

        if (res !== 0)
            return res;

        res = numberComparator(pa.minor, pb.minor);

        if (res !== 0)
            return res;

        res = numberComparator(pa.maintenance, pb.maintenance);

        if (res !== 0)
            return res;

        return numberComparator(pa.revTs, pb.revTs);
    }

    /**
     * Check if node version in range
     * @param {String} nodeVer Node version.
     * @param {Array.<String>} ranges Version ranges to compare with.
     * @returns {Boolean} `True` if node version is equal or greater than specified range.
     */
    includes(nodeVer, ...ranges) {
        return !!_.find(ranges, ([after, before]) =>
            this.compare(nodeVer, after) >= 0 && (_.isNil(before) || this.compare(nodeVer, before) < 0)
        );
    }

    /**
     * Check if node version is newer or same
     * @param {String} nodeVer Node version.
     * @param {String} sinceVer Version to compare with.
     * @returns {Boolean} `True` if node version is equal or greater than specified version.
     */
    since(nodeVer, sinceVer) {
        return this.includes(nodeVer, [sinceVer]);
    }

    /**
     * Check whether node version before than specified version.
     * @param {String} nodeVer Node version.
     * @param {String} sinceVer Version to compare with.
     * @return {Boolean} `True` if node version before than specified version.
     */
    before(nodeVer, sinceVer) {
        return !this.since(nodeVer, sinceVer);
    }
}
