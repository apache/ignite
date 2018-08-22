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

import controller from './controller';
import templateUrl from './template.tpl.pug';
import {CancellationError} from 'app/errors/CancellationError';

export default class UserNotificationsService {
    static $inject = ['$http', '$modal', '$q', 'IgniteMessages'];

    /** @type {ng.IQService} */
    $q;

    constructor($http, $modal, $q, Messages) {
        Object.assign(this, {$http, $modal, $q, Messages});

        this.message = null;
        this.isShown = false;
    }

    set notification(notification) {
        this.message = _.get(notification, 'message');
        this.isShown = _.get(notification, 'isShown');
    }

    editor() {
        const deferred = this.$q.defer();

        const modal = this.$modal({
            templateUrl,
            resolve: {
                deferred: () => deferred,
                message: () => this.message,
                isShown: () => this.isShown
            },
            controller,
            controllerAs: '$ctrl'
        });

        const modalHide = modal.hide;

        modal.hide = () => deferred.reject(new CancellationError());

        return deferred.promise
            .finally(modalHide)
            .then(({ message, isShown }) => {
                this.$http.put('/api/v1/admin/notifications', { message, isShown })
                    .catch(this.Messages.showError);
            });
    }
}
