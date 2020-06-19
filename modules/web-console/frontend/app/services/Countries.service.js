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
