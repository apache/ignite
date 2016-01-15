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
                    name: 'Simple query',
                    cacheName: 'CarCache',
                    pageSize: 50,
                    query: 'SELECT * FROM "CarCache".Car',
                    result: 'table',
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
                    rate: {
                        value: 30,
                        unit: 1000,
                        installed: false
                    }
                },
                {
                    name: 'Query with refresh rate',
                    cacheName: 'CarCache',
                    pageSize: 50,
                    query: 'SELECT * FROM "CarCache".Car',
                    result: 'table',
                    rate: {
                        value: 5,
                        unit: 1000,
                        installed: true
                    }
                }
            ],
            expandedParagraphs: [0, 1, 2]
        };

        this.$get = ['$q', '$http', ($q, $http) => {
            return {
                read(demo, noteId) {
                    if (demo)
                        return $q.when(angular.copy(_demoNotebook));

                    return $http.post('/api/v1/notebooks/get', {noteId})
                        .then(({data}) => {
                            return data;
                        });
                },
                save(demo, notebook) {
                    if (demo)
                        return $http.post('/api/v1/notebooks/save', notebook);

                    return $q.when();
                },
                remove(demo, nodeId) {
                }
            };
        }];
    });
