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

import angular from 'angular';

angular
    .module('ignite-console.QueryNotebooks', [

    ])
    .provider('QueryNotebooks', function() {
        const _demoNotebook = {
            name: 'SQL demo',
            paragraphs: [
                {
                    name: 'Query with refresh rate',
                    cacheName: 'CarCache',
                    pageSize: 50,
                    query: 'SELECT count(*)\nFROM "CarCache".Car',
                    result: 'bar',
                    timeLineSpan: '1',
                    rate: {
                        value: 3,
                        unit: 1000,
                        installed: true
                    }
                },
                {
                    name: 'Simple query',
                    cacheName: 'CarCache',
                    pageSize: 50,
                    query: 'SELECT * FROM "CarCache".Car',
                    result: 'table',
                    timeLineSpan: '1',
                    rate: {
                        value: 30,
                        unit: 1000,
                        installed: false
                    }
                },
                {
                    name: 'Query with aggregates',
                    cacheName: 'CarCache',
                    pageSize: 50,
                    query: 'SELECT p.name, count(*) AS cnt\nFROM "ParkingCache".Parking p\nINNER JOIN "CarCache".Car c\n  ON (p.id) = (c.parkingId)\nGROUP BY P.NAME',
                    result: 'table',
                    timeLineSpan: '1',
                    rate: {
                        value: 30,
                        unit: 1000,
                        installed: false
                    }
                }
            ],
            expandedParagraphs: [0, 1, 2]
        };

        this.$get = ['$q', '$http', '$rootScope', ($q, $http, $root) => {
            return {
                read(noteId) {
                    if ($root.IgniteDemoMode)
                        return $q.when(angular.copy(_demoNotebook));

                    return $http.post('/api/v1/notebooks/get', {noteId})
                        .then(({data}) => data)
                        .catch(({data}) => $q.reject(data));
                },
                save(notebook) {
                    if ($root.IgniteDemoMode)
                        return $q.when();

                    return $http.post('/api/v1/notebooks/save', notebook)
                        .then(({data}) => data)
                        .catch(({data}) => $q.reject(data));
                },
                remove(notebook) {
                    if ($root.IgniteDemoMode)
                        return $q.reject(`Removing "${notebook.name}" notebook is not supported.`);

                    return $http.post('/api/v1/notebooks/remove', {_id: notebook._id})
                        .then(() => {
                            const idx = _.findIndex($root.notebooks, (item) => {
                                return item._id === notebook._id;
                            });

                            if (idx >= 0) {
                                $root.notebooks.splice(idx, 1);

                                $root.rebuildDropdown();

                                if (idx < $root.notebooks.length)
                                    return $root.notebooks[idx];
                            }

                            if ($root.notebooks.length > 0)
                                return $root.notebooks[$root.notebooks.length - 1];
                        })
                        .catch(({data}) => $q.reject(data));
                }
            };
        }];
    });
