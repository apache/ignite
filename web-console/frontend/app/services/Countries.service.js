

import _ from 'lodash';
import COUNTRIES from 'app/data/countries.json';

/**
 * @typedef {{label:string,value:string,code:string}} Country
 */

export default function() {
    const indexByName = _.keyBy(COUNTRIES, 'label');
    const UNDEFINED_COUNTRY = {label: '', value: '', code: ''};

    /**
     * @param {string} name
     * @return {Country}
     */
    const getByName = (name) => (indexByName[name] || UNDEFINED_COUNTRY);
    /**
     * @returns {Array<Country>}
     */
    const getAll = () => (COUNTRIES);

    return {
        getByName,
        getAll
    };
}
