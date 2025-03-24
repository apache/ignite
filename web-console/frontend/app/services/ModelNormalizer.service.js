

import _ from 'lodash';

// Service to normalize objects for dirty checks.
export default function() {
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
}
