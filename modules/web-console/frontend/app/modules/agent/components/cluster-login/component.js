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

import template from './template.pug';

export const component = {
    name: 'clusterLogin',
    bindings: {
        onLogin: '=',
        onHide: '='
    },
    controller: class {
        sesTtls = [
            {label: '5 min', value: 300000},
            {label: '10 min', value: 600000},
            {label: '15 min', value: 900000},
            {label: '20 min', value: 1200000},
            {label: '25 min', value: 1500000},
            {label: '30 min', value: 1800000}
        ];

        constructor() {
            this.data = {
                sesTtl: _.last(this.sesTtls).value
            };
        }

        login() {
            if (this.form.$invalid)
                return;

            this.onLogin(this.data);
        }
    },
    template
};
