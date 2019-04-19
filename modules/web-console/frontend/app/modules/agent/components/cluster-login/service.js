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

import {CancellationError} from 'app/errors/CancellationError';

export default class ClusterLoginService {
    static $inject = ['$modal', '$q'];

    deferred;

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ng.IQService} $q
     */
    constructor($modal, $q) {
        this.$modal = $modal;
        this.$q = $q;
    }

    /**
     * @param {import('../../types/ClusterSecrets').ClusterSecrets} baseSecrets
     * @returns {ng.IPromise<import('../../types/ClusterSecrets').ClusterSecrets>}
     */
    askCredentials(baseSecrets) {
        if (this.deferred)
            return this.deferred.promise;

        this.deferred = this.$q.defer();

        const self = this;

        const modal = this.$modal({
            template: `
                <cluster-login
                    secrets='$ctrl.secrets'
                    on-login='$ctrl.onLogin()'
                    on-hide='$ctrl.onHide()'
                ></cluster-login>
            `,
            controller: [function() {
                this.secrets = _.clone(baseSecrets);

                this.onLogin = () => {
                    self.deferred.resolve(this.secrets);
                };

                this.onHide = () => {
                    self.deferred.reject(new CancellationError());
                };
            }],
            controllerAs: '$ctrl',
            backdrop: 'static',
            show: true
        });

        return modal.$promise
            .then(() => this.deferred.promise)
            .finally(() => {
                this.deferred = null;

                modal.hide();
            });
    }

    cancel() {
        if (this.deferred)
            this.deferred.reject(new CancellationError());
    }
}
