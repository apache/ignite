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

import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import _ from 'lodash';

/**
 * Utility service for version parsing and comparing
 */
const VERSION_MATCHER = /(\d+)\.(\d+)\.(\d+)([-.]([^0123456789][^-]+)(-SNAPSHOT)?)?(-(\d+))?(-([\da-f]+))?/i;

/**
 * Tries to parse product version from it's string representation.
 *
 * @param {String} ver - String representation of version.
 * @returns {{major: Number, minor: Number, maintenance: Number, stage: String, revTs: Number, revHash: String}} - Object that contains product version fields.
 */
const parse = (ver) => {
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
};

const numberComparator = (a, b) => a > b ? 1 : a < b ? -1 : 0;

/**
 * Compare to version.
 * @param a {Object} first compared version.
 * @param b {Object} second compared version.
 * @returns {Number} 1 if a > b, 0 if versions equals, -1 if a < b
 */
const compare = (a, b) => {
    let res = numberComparator(a.major, b.major);

    if (res !== 0)
        return res;

    res = numberComparator(a.minor, b.minor);

    if (res !== 0)
        return res;

    res = numberComparator(a.maintenance, b.maintenance);

    if (res !== 0)
        return res;

    return numberComparator(a.stage, b.stage);
};

export default class IgniteVersion {
    constructor() {
        this.webConsole = '2.7.0';

        this.supportedVersions = [
            {
                label: 'Ignite 2.7',
                ignite: '2.7.0'
            },
            {
                label: 'Ignite 2.6',
                ignite: '2.6.0'
            },
            {
                label: 'Ignite 2.5',
                ignite: '2.5.0'
            },
            {
                label: 'Ignite 2.4',
                ignite: '2.4.0'
            },
            {
                label: 'Ignite 2.3',
                ignite: '2.3.0'
            },
            {
                label: 'Ignite 2.1',
                ignite: '2.2.0'
            },
            {
                label: 'Ignite 2.0',
                ignite: '2.0.0'
            },
            {
                label: 'Ignite 1.x',
                ignite: '1.9.0'
            }
        ];

        /** Current product version. */
        let current = _.head(this.supportedVersions);

        try {
            const ignite = localStorage.configurationVersion;

            const restored = _.find(this.supportedVersions, {ignite});

            if (restored)
                current = restored;
        }
        catch (ignored) {
            // No-op.
        }

        this.currentSbj = new BehaviorSubject(current);

        this.currentSbj.subscribe({
            next: (ver) => {
                try {
                    localStorage.setItem('configurationVersion', ver.ignite);
                }
                catch (ignored) {
                    // No-op.
                }
            }
        });
    }

    /**
     * @return {String} Current Ignite version.
     */
    get current() {
        return this.currentSbj.getValue().ignite;
    }

    /**
     * Check if version in range.
     *
     * @param {String} target Target version.
     * @param {String | Array.<String>} ranges Version ranges to compare with.
     * @returns {Boolean} `True` if version is equal or greater than specified range.
     */
    since(target, ...ranges) {
        const targetVer = parse(target);

        return !!_.find(ranges, (range) => {
            if (_.isArray(range)) {
                const [after, before] = range;

                return compare(targetVer, parse(after)) >= 0 &&
                    (_.isNil(before) || compare(targetVer, parse(before)) < 0);
            }

            return compare(targetVer, parse(range)) >= 0;
        });
    }

    /**
     * Check whether version before than specified version.
     *
     * @param {String} target Target version.
     * @param {String} ranges Version ranges to compare with.
     * @return {Boolean} `True` if version before than specified version.
     */
    before(target, ...ranges) {
        return !this.since(target, ...ranges);
    }

    /**
     * Check if current version in specified range.
     *
     * @param {String|Array.<String>} ranges Version ranges to compare with.
     * @returns {Boolean} `True` if configuration version is equal or greater than specified range.
     */
    available(...ranges) {
        return this.since(this.current, ...ranges);
    }
}
