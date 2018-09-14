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

import templateUrl from 'views/templates/confirm.tpl.pug';
import {CancellationError} from 'app/errors/CancellationError';

export class Confirm {
    static $inject = ['$modal', '$q'];
    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ng.IQService} $q
     */
    constructor($modal, $q) {
        this.$modal = $modal;
        this.$q = $q;
    }
    /**
     * @param {string} content - Confirmation text/html content
     * @param {boolean} yesNo - Show "Yes/No" buttons instead of "Config"
     * @return {ng.IPromise}
     */
    confirm(content = 'Confirm?', yesNo = false) {
        return this.$q((resolve, reject) => {
            this.$modal({
                templateUrl,
                backdrop: true,
                onBeforeHide: () => reject(new CancellationError()),
                controller: ['$scope', function($scope) {
                    $scope.yesNo = yesNo;
                    $scope.content = content;
                    $scope.confirmCancel = $scope.confirmNo = () => {
                        reject(new CancellationError());
                        $scope.$hide();
                    };
                    $scope.confirmYes = () => {
                        resolve();
                        $scope.$hide();
                    };
                }]
            });
        });
    }
}

// Confirm popup service.
export default ['IgniteConfirm', ['$rootScope', '$q', '$modal', '$animate', function($root, $q, $modal, $animate) {
    const scope = $root.$new();

    const modal = $modal({templateUrl, scope, show: false, backdrop: true});

    const _hide = () => {
        $animate.enabled(modal.$element, false);

        modal.hide();
    };

    let deferred;

    scope.confirmYes = () => {
        _hide();

        deferred.resolve(true);
    };

    scope.confirmNo = () => {
        _hide();

        deferred.resolve(false);
    };

    scope.confirmCancel = () => {
        _hide();

        deferred.reject(new CancellationError());
    };

    /**
     *
     * @param {String } content
     * @param {Boolean} [yesNo]
     * @returns {Promise}
     */
    modal.confirm = (content, yesNo) => {
        scope.content = content || 'Confirm?';
        scope.yesNo = !!yesNo;

        deferred = $q.defer();

        modal.$promise.then(modal.show);

        return deferred.promise;
    };

    return modal;
}]];
