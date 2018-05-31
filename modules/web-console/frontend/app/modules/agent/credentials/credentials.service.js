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

import template from './template.pug';

import {CancellationError} from 'app/errors/CancellationError';

const DFLT_CREDS = {user: '', password: ''};

class controller {
    static $inject = ['cluster', 'deferred'];

    /**
     * @param cluster
     * @param deferred
     */
    constructor(cluster, deferred) {
        this.cluster = cluster;
        this.deferred = deferred;

        this.data = {};
    }

    login() {
        this.cluster.setCredentials(this.data);
        this.deferred.resolve(this.data);
    }
}

export default class ModalCredentials {
    static $inject = ['$modal', '$q'];

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param $q
     */
    constructor($modal, $q) {
        this.$modal = $modal;
        this.$q = $q;
    }

    /**
     * @return {Promise<{user: string, password: string}>}
     */
    askCredentials(cluster) {
        if (!cluster)
            return Promise.resolve(DFLT_CREDS);

        if (!cluster.needsCredentials())
            return Promise.resolve(cluster.credentials());

        const deferred = this.$q.defer();

        const modal = this.$modal({
            template,
            resolve: {
                cluster: () => cluster,
                deferred: () => deferred
            },
            controller,
            controllerAs: '$ctrl',
            show: true
        });

        const modalHide = modal.hide;

        modal.hide = () => deferred.reject(new CancellationError());

        return deferred.promise
            .finally(modalHide);
    }
}
