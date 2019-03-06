/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

/**
 * Confirm popup service.
 * @deprecated Use Confirm instead
 * @param {ng.IRootScopeService} $root
 * @param {ng.IQService} $q
 * @param {mgcrea.ngStrap.modal.IModalService} $modal
 * @param {ng.animate.IAnimateService} $animate
 */
export default function IgniteConfirm($root, $q, $modal, $animate) {
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
}

IgniteConfirm.$inject = ['$rootScope', '$q', '$modal', '$animate'];
