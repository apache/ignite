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

import templateUrl from './template.tpl.pug';
import {CancellationError} from 'app/errors/CancellationError';

const DFLT_CREDS = {user: '', password: ''};

export default class ClusterCredentials {
    static $inject = ['$modal', '$q'];

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ng.IQService} $q
     */
    constructor($modal, $q) {
        this.$modal = $modal;

        this.$q = $q;

        this.credentials = {
            user: '',
            password: ''
        };
    }

    /**
     * @return {Promise<{user: string, password: string}>}
     */
    askCredentials(cluster) {
        if (!cluster)
            return Promise.resolve(DFLT_CREDS);

        if (!cluster.needsCredentials())
            return Promise.resolve(cluster.credentials());

        return this.$q((resolve, reject) => {
            this.$modal({
                templateUrl,
                backdrop: true,
                onBeforeHide: () => reject(new CancellationError()),
                controller: [
                    '$scope', ($scope) => {
                        $scope.credentials = this.credentials;

                        $scope.cancel = () => {
                            reject(new CancellationError());

                            $scope.$hide();
                        };

                        $scope.login = () => {
                            resolve(this.credentials);

                            $scope.$hide();
                        };
                    }]
            });
        });
    }
}
