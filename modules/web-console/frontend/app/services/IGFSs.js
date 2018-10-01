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

import ObjectID from 'bson-objectid';
import omit from 'lodash/fp/omit';
import get from 'lodash/get';

export default class IGFSs {
    static $inject = ['$http'];

    igfsModes = [
        {value: 'PRIMARY', label: 'PRIMARY'},
        {value: 'PROXY', label: 'PROXY'},
        {value: 'DUAL_SYNC', label: 'DUAL_SYNC'},
        {value: 'DUAL_ASYNC', label: 'DUAL_ASYNC'}
    ];

    /**
     * @param {ng.IHttpService} $http
     */
    constructor($http) {
        this.$http = $http;
    }

    /**
     * @param {string} igfsID
     */
    getIGFS(igfsID) {
        return this.$http.get(`/api/v1/configuration/igfs/${igfsID}`);
    }

    getBlankIGFS() {
        return {
            _id: ObjectID.generate(),
            ipcEndpointEnabled: true,
            fragmentizerEnabled: true,
            colocateMetadata: true,
            relaxedConsistency: true
        };
    }

    affinnityGroupSize = {
        default: 512,
        min: 1
    };

    defaultMode = {
        values: [
            {value: 'PRIMARY', label: 'PRIMARY'},
            {value: 'PROXY', label: 'PROXY'},
            {value: 'DUAL_SYNC', label: 'DUAL_SYNC'},
            {value: 'DUAL_ASYNC', label: 'DUAL_ASYNC'}
        ],
        default: 'DUAL_ASYNC'
    };

    secondaryFileSystemEnabled = {
        requiredWhenIGFSProxyMode: (igfs) => {
            if (get(igfs, 'defaultMode') === 'PROXY')
                return get(igfs, 'secondaryFileSystemEnabled') === true;

            return true;
        },
        requiredWhenPathModeProxyMode: (igfs) => {
            if (get(igfs, 'pathModes', []).some((pm) => pm.mode === 'PROXY'))
                return get(igfs, 'secondaryFileSystemEnabled') === true;

            return true;
        }
    };

    normalize = omit(['__v', 'space', 'clusters']);
}
