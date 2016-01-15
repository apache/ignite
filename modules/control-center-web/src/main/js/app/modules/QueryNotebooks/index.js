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
                    query: 'SELECT * FROM Car',
                    result: 'table',
                    queryArgs: {
                        type: 'QUERY',
                        query: 'SELECT * FROM Car',
                        pageSize: 50,
                        cacheName: 'CarCache'
                    },
                    rate: {
                        value: 1,
                        unit: 60000,
                        installed: false
                    }
                },
                {
                    name: 'Query with aggregates',
                    cacheName: 'CarCache',
                    pageSize: 50,
                    query: 'SELECT * FROM Car',
                    result: 'table',
                    queryArgs: {
                        type: 'QUERY',
                        query: 'SELECT * FROM Car',
                        pageSize: 50,
                        cacheName: 'CarCache'
                    },
                    rate: {
                        value: 1,
                        unit: 60000,
                        installed: false
                    }
                },
                {
                    name: 'Query with refresh rate',
                    cacheName: 'CarCache',
                    pageSize: 50,
                    query: 'SELECT * FROM Car',
                    result: 'table',
                    queryArgs: {
                        type: 'QUERY',
                        query: 'SELECT * FROM Car',
                        pageSize: 50,
                        cacheName: 'CarCache'
                    },
                    rate: {
                        value: 1,
                        unit: 60000,
                        installed: false
                    }
                }
            ],
            expandedParagraphs: [0, 1, 2]
        };

        this.$get = ['$q', '$http', ($q, $http) => {
            return {
                read(demo, noteId) {
                    if (demo) {
                        return $http.post('/api/v1/agent/demo/sql/start')
                            .then(() => {
                                return angular.copy(_demoNotebook);
                            });
                    }

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
