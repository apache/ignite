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

import templateUrl from 'views/sql/notebook-new.tpl.pug';

// Controller that load notebooks in navigation bar .
export default ['$scope', '$modal', '$state', 'IgniteMessages', 'IgniteNotebook',
    (scope, $modal, $state, Messages, Notebook) => {
        // Pre-fetch modal dialogs.
        const nameModal = $modal({scope, templateUrl, show: false});

        scope.create = (name) => {
            return Notebook.create(name)
                .then((notebook) => {
                    nameModal.hide();

                    $state.go('base.sql.notebook', {noteId: notebook._id});
                })
                .catch(Messages.showError);
        };

        scope.createNotebook = () => nameModal.$promise.then(nameModal.show);

        Notebook.read()
            .then((notebooks) => {
                scope.$watchCollection(() => notebooks, (changed) => {
                    if (_.isEmpty(changed))
                        return scope.notebooks = [];

                    scope.notebooks = [
                        {text: 'Create new notebook', click: scope.createNotebook},
                        {divider: true}
                    ];

                    _.forEach(changed, (notebook) => scope.notebooks.push({
                        data: notebook,
                        action: {
                            icon: 'fa-trash',
                            click: (item) => Notebook.remove(item)
                        },
                        text: notebook.name,
                        sref: `base.sql.notebook({noteId:"${notebook._id}"})`
                    }));
                });
            })
            .catch(Messages.showError);
    }
];
