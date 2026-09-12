

import _ from 'lodash';
import JavaTypes from 'app/services/JavaTypes.service';
/**
 * @param {ReturnType<typeof import('../services/LegacyUtils.service').default>} LegacyUtils [description]
 */
export default function factory(LegacyUtils,JavaTypes) {
    /**
     * Filter domain models with key fields configuration.
     * @template T
     * @param {Array<T>} domains
     * @param {boolean} valid
     * @param {boolean} invalid
     */
    const filter = (domains, valid, invalid) => {
        if (valid && invalid)
            return domains;

        /** @type {Array<T>} */
        const out = [];

        _.forEach(domains, function(domain) {
            const _valid = !LegacyUtils.domainForStoreConfigured(domain) 
            || JavaTypes.isJavaBuiltInClass(domain.keyFields) 
            && !_.isEmpty(domain.keyFields);

            if (valid && _valid || invalid && !_valid)
                out.push(domain);
        });

        return out;
    };

    return filter;
}

factory.$inject = ['IgniteLegacyUtils','JavaTypes'];
