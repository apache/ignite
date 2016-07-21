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

// Confirm popup service.
export default ['IgniteConfirm', ['$rootScope', '$q', '$modal', '$animate', ($root, $q, $modal, $animate) => {
    const scope = $root.$new();

    const modal = $modal({templateUrl: '/templates/confirm.html', scope, placement: 'center', show: false});

    let deferred;

    const _hide = (animate) => {
        $animate.enabled(modal.$element, animate);

        modal.hide();
    };

    scope.confirmYes = () => {
        _hide(scope.animate);

        deferred.resolve(true);
    };

    scope.confirmNo = () => {
        _hide(scope.animate);

        deferred.resolve(false);
    };

    scope.confirmCancel = () => {
        _hide(true);

        deferred.reject('cancelled');
    };

    /**
     *
     * @param {String } content
     * @param {Boolean} [yesNo]
     * @param {Boolean} [animate]
     * @returns {Promise}
     */
    modal.confirm = (content, yesNo, animate) => {
        scope.animate = !!animate;
        scope.content = content || 'Confirm?';
        scope.yesNo = !!yesNo;

        deferred = $q.defer();

        modal.$promise.then(modal.show);

        return deferred.promise;
    };

    return modal;
}]];
