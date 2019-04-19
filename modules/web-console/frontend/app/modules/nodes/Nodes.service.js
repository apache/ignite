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

import nodesDialogTplUrl from './nodes-dialog.tpl.pug';

const DEFAULT_OPTIONS = {
    grid: {
        multiSelect: false
    }
};

class Nodes {
    static $inject = ['$q', '$modal'];

    /**
     * @param {ng.IQService} $q
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     */
    constructor($q, $modal) {
        this.$q = $q;
        this.$modal = $modal;
    }

    selectNode(nodes, cacheName, options = DEFAULT_OPTIONS) {
        const { $q, $modal } = this;
        const defer = $q.defer();
        options.target = cacheName;

        const modalInstance = $modal({
            templateUrl: nodesDialogTplUrl,
            show: true,
            resolve: {
                nodes: () => nodes || [],
                options: () => options
            },
            controller: 'nodesDialogController',
            controllerAs: '$ctrl'
        });

        modalInstance.$scope.$ok = (data) => {
            defer.resolve(data);
            modalInstance.$scope.$hide();
        };

        modalInstance.$scope.$cancel = () => {
            defer.reject();
            modalInstance.$scope.$hide();
        };

        return defer.promise;
    }
}

export default Nodes;
