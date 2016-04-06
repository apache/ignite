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
export default ['$confirm', ['$modal', '$rootScope', '$q', ($modal, $root, $q) => {
    const scope = $root.$new();

    const modal = $modal({templateUrl: '/templates/confirm.html', scope, placement: 'center', show: false});

    let deferred;

    scope.confirmYes = () => {
        deferred.resolve(true);

        modal.hide();
    };

    scope.confirmNo = () => {
        deferred.resolve(false);

        modal.hide();
    };

    scope.confirmCancel = () => {
        deferred.reject('cancelled');

        modal.hide();
    };

    /**
     *
     * @param {String } content
     * @param {Boolean} yesNo
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
