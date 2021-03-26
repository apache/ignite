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

import uniq from 'lodash/uniq';
import map from 'lodash/map';
import reduce from 'lodash/reduce';
import isObject from 'lodash/isObject';
import merge from 'lodash/merge';
import includes from 'lodash/includes';

export class JavaTypesNonEnum {
    static $inject = ['IgniteClusterDefaults', 'IgniteCacheDefaults', 'JavaTypes'];

    enumClasses: any;
    shortEnumClasses: any;

    constructor(clusterDflts, cacheDflts, JavaTypes) {
        this.enumClasses = uniq(this._enumClassesAcc(merge(clusterDflts, cacheDflts), []));
        this.shortEnumClasses = map(this.enumClasses, (cls) => JavaTypes.shortClassName(cls));
    }

    /**
     * Check if class name is non enum class in Ignite configuration.
     *
     * @param clsName
     * @return {boolean}
     */
    nonEnum(clsName: string): boolean {
        return !includes(this.shortEnumClasses, clsName) && !includes(this.enumClasses, clsName);
    }

    /**
     * Collects recursive enum classes.
     *
     * @param root Root object.
     * @param classes Collected classes.
     */
    private _enumClassesAcc(root, classes): Array<string> {
        return reduce(root, (acc, val, key) => {
            if (key === 'clsName')
                acc.push(val);
            else if (isObject(val))
                this._enumClassesAcc(val, acc);

            return acc;
        }, classes);
    }
}
