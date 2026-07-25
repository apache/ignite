

import uniq from 'lodash/uniq';
import map from 'lodash/map';
import reduce from 'lodash/reduce';
import isObject from 'lodash/isObject';
import merge from 'lodash/merge';
import includes from 'lodash/includes';

export class JavaTypesNonEnum {
    static $inject = ['IgniteClusterDefaults', 'IgniteCacheDefaults', 'IgniteIGFSDefaults', 'JavaTypes'];

    enumClasses: any;
    shortEnumClasses: any;

    constructor(clusterDflts, cacheDflts, igfsDflts, JavaTypes) {
        this.enumClasses = uniq(this._enumClassesAcc(merge(clusterDflts, cacheDflts, igfsDflts), []));
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
