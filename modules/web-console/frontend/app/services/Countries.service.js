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
