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

// Controller that load notebooks in navigation bar .
export default ['notebooks', [
    '$rootScope', '$scope', '$modal', '$state', '$http', 'IgniteMessages',
    ($root, $scope, $modal, $state, $http, Messages) => {
        $root.notebooks = [];

        // Pre-fetch modal dialogs.
        const _notebookNewModal = $modal({scope: $scope, templateUrl: '/sql/notebook-new.html', show: false});

        $root.rebuildDropdown = function() {
            $scope.notebookDropdown = [
                {text: 'Create new notebook', click: 'inputNotebookName()'},
                {divider: true}
            ];

            _.forEach($root.notebooks, (notebook) => $scope.notebookDropdown.push({
                text: notebook.name,
                sref: 'base.sql.notebook({noteId:"' + notebook._id + '"})'
            }));
        };

        $root.reloadNotebooks = function() {
            // When landing on the page, get clusters and show them.
            $http.post('/api/v1/notebooks/list')
                .success((data) => {
                    $root.notebooks = data;

                    $root.rebuildDropdown();
                })
                .error(Messages.showError);
        };

        $root.inputNotebookName = () => {
            _notebookNewModal.$promise.then(_notebookNewModal.show);
        };

        $root.createNewNotebook = (name) => {
            $http.post('/api/v1/notebooks/new', {name})
                .success((noteId) => {
                    _notebookNewModal.hide();

                    $root.reloadNotebooks();

                    $state.go('base.sql.notebook', {noteId});
                })
                .error(Messages.showError);
        };

        $root.reloadNotebooks();

    }
]];
