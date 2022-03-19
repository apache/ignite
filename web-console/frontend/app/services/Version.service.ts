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

import { BehaviorSubject } from 'rxjs';
import _ from 'lodash';

/**
 * Utility service for version parsing and comparing
 */
const VERSION_MATCHER = /(\d+)\.(\d+)\.(\d+)([-.]([^0123456789][^-]+)(-SNAPSHOT)?)?(-(\d+))?(-([\da-f]+))?/i;

type ComparisonNumbers = -1|0|1

const numberComparator = <T>(a: T, b: T): ComparisonNumbers => a > b ? 1 : a < b ? -1 : 0;

interface ParsedVersion {
    major: number,
    minor: number,
    maintenance: number,
    stage: string,
    revTs: number,
    revHash?: string
}

type VersionInfo = {label: string, ignite: string}

export default class IgniteVersion {
    // @ts-ignore
    webConsole: string = WEB_CONSOLE_VERSION;
    supportedVersions: VersionInfo[];
    currentSbj: BehaviorSubject<VersionInfo>;
    constructor() {
        this.supportedVersions = [
            {
                label: 'Ignite 2.9+',
                ignite: '2.9.0'
            },
            {
                label: 'Ignite 2.8',
                ignite: '2.8.0'
            },
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
                label: 'Ignite 2.2',
                ignite: '2.2.0'
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
     * @return Current Ignite version.
     */
    get current() {
        return this.currentSbj.getValue().ignite;
    }

    /**
     * Check if version in range.
     *
     * @param target Target version.
     * @param ranges Version ranges to compare with.
     * @returns `True` if version is equal or greater than specified range.
     */
    since(target: string, ...ranges: (string|string[])[]): boolean {
        if (!target)
            return false;

        const targetVer = this.parse(target);

        return !!_.find(ranges, (range) => {
            if (_.isArray(range)) {
                const [after, before] = range;

                return this.compare(targetVer, this.parse(after)) >= 0 &&
                    (_.isNil(before) || this.compare(targetVer, this.parse(before)) < 0);
            }

            return this.compare(targetVer, this.parse(range)) >= 0;
        });
    }

    /**
     * Check whether version before than specified version.
     *
     * @param target Target version.
     * @param ranges Version ranges to compare with.
     * @return `True` if version before than specified version.
     */
    before(target: string, ...ranges: (string|string[])[]): boolean {
        return !this.since(target, ...ranges);
    }

    /**
     * Check if current version in specified range.
     *
     * @param ranges Version ranges to compare with.
     * @returns `True` if configuration version is equal or greater than specified range.
     */
    available(...ranges: (string|string[])[]): boolean {
        return this.since(this.current, ...ranges);
    }

    /**
     * Tries to parse product version from it's string representation.
     */
    parse(ver: string): ParsedVersion {
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
     */
    compare(a: ParsedVersion, b: ParsedVersion): ComparisonNumbers {
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
    }

}
