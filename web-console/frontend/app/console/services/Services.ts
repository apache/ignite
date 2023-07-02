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

import get from 'lodash/get';
import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';

import {CacheModes, AtomicityModes, ShortCache} from 'app/configuration/types';
import {Menu} from 'app/types';

export default class Services {
    static $inject = ['$http'];

    serviceModes: Menu<CacheModes> = [
        {value: 'NodeSingleton', label: 'NodeSingleton'},
        {value: 'ClusterSingleton', label: 'ClusterSingleton'},
        {value: 'KeyAffinitySingleton', label: 'KeyAffinitySingleton'},
        {value: 'Multiple', label: 'Multiple'}
    ];

    atomicityModes: Menu<AtomicityModes> = [
        {value: 'ATOMIC', label: 'ATOMIC'},
        {value: 'TRANSACTIONAL', label: 'TRANSACTIONAL'},
        {value: 'TRANSACTIONAL_SNAPSHOT', label: 'TRANSACTIONAL_SNAPSHOT'}
    ];

    constructor(private $http: ng.IHttpService) {}

    getBackupsCount(serviceName){
        return 1;
    }
}
