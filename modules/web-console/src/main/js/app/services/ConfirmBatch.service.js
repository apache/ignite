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

// Service for confirm or skip several steps.
export default ['IgniteConfirmBatch', ['$rootScope', '$q', '$modal', ($root, $q, $modal) => {
    const scope = $root.$new();

    scope.confirmModal = $modal({
        templateUrl: '/templates/batch-confirm.html',
        scope,
        placement: 'center',
        show: false,
        backdrop: 'static',
        keyboard: false
    });

    const _done = (cancel) => {
        scope.confirmModal.hide();

        if (cancel)
            scope.deferred.reject('cancelled');
        else
            scope.deferred.resolve();
    };

    const _nextElement = (skip) => {
        scope.items[scope.curIx++].skip = skip;

        if (scope.curIx < scope.items.length)
            scope.content = scope.contentGenerator(scope.items[scope.curIx]);
        else
            _done();
    };

    scope.cancel = () => {
        _done(true);
    };

    scope.skip = (applyToAll) => {
        if (applyToAll) {
            for (let i = scope.curIx; i < scope.items.length; i++)
                scope.items[i].skip = true;

            _done();
        }
        else
            _nextElement(true);
    };

    scope.overwrite = (applyToAll) => {
        if (applyToAll)
            _done();
        else
            _nextElement(false);
    };

    return {
        /**
         * Show confirm all dialog.
         *
         * @param confirmMessageFn Function to generate a confirm message.
         * @param itemsToConfirm Array of element to process by confirm.
         */
        confirm(confirmMessageFn, itemsToConfirm) {
            scope.deferred = $q.defer();

            scope.contentGenerator = confirmMessageFn;

            scope.items = itemsToConfirm;
            scope.curIx = 0;
            scope.content = (scope.items && scope.items.length > 0) ? scope.contentGenerator(scope.items[0]) : null;

            scope.confirmModal.$promise.then(scope.confirmModal.show);

            return scope.deferred.promise;
        }
    };
}]];
