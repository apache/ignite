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

export default class ScanFilter {
    static $inject = ['$rootScope', '$q', '$modal'];

    constructor($root, $q, $modal) {
        this.deferred = null;
        this.$q = $q;

        const scope = $root.$new();

        scope.ui = {};

        scope.ok = () => {
            this.deferred.resolve({filter: scope.ui.filter, caseSensitive: !!scope.ui.caseSensitive});

            this.modal.hide();
        };

        scope.$hide = () => {
            this.modal.hide();

            this.deferred.reject();
        };

        this.modal = $modal({templateUrl: '/scan-filter-input.html', scope, placement: 'center', show: false});
    }

    open() {
        this.deferred = this.$q.defer();

        this.modal.$promise.then(this.modal.show);

        return this.deferred.promise;
    }
}
