/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the License); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {spy, stub} from 'sinon';

import Controller from './controller';

const mocks = () => new Map([
    ['$scope', {}],
    ['uiGridConstants', {
        dataChange: {
            COLUMN: 'COLUMN'
        }
    }]
]);

const apiMock = () => ({
    grid: {
        options: {
            columnDefs: []
        },
        registerDataChangeCallback: spy(),
        processColumnsProcessors: spy((v) => Promise.resolve(v)),
        setVisibleColumns: spy((v) => Promise.resolve(v)),
        redrawInPlace: spy((v) => Promise.resolve(v)),
        refreshCanvas: spy((v) => Promise.resolve(v)),
        scrollTo: spy()
    },
    grouping: {
        on: {
            groupingChanged: spy()
        },
        getGrouping: stub().returns({grouping: []})
    }
});

suite('grid-column-selector component controller', () => {
    test('$onChanges', () => {
        const c = new Controller(...mocks().values());
        c.applyValues = spy(c.applyValues.bind(c));
        const api = apiMock();
        c.gridApi = api;
        c.$onChanges({gridApi: {currentValue: api}});

        assert.equal(c.applyValues.callCount, 1, 'calls applyValues');
        assert.isFunction(
            c.gridApi.grid.registerDataChangeCallback.lastCall.args[0]
        );

        c.gridApi.grid.registerDataChangeCallback.lastCall.args[0]();

        assert.equal(
            c.applyValues.callCount,
            2,
            'registers applyValues as data change callback'
        );
        assert.deepEqual(
            c.gridApi.grid.registerDataChangeCallback.lastCall.args[1],
            [c.uiGridConstants.dataChange.COLUMN],
            'registers data change callback for COLUMN'
        );
        assert.equal(
            c.gridApi.grouping.on.groupingChanged.lastCall.args[0],
            c.$scope,
            'registers grouping change callback with correct $scope'
        );

        c.gridApi.grouping.on.groupingChanged.lastCall.args[1]();

        assert.equal(
            c.applyValues.callCount,
            3,
            'registers applyValues as grouping change callback'
        );
    });
    test('applyValues', () => {
        const c = new Controller(...mocks().values());
        const mock = {
            getState: stub().returns({}),
            makeMenu: stub().returns({}),
            getSelectedColumns: stub().returns({}),
            setSelectedColumns: spy()
        };
        c.applyValues.call(mock);

        assert.equal(
            mock.state,
            mock.getState.lastCall.returnValue,
            'assigns getState return value as this.state'
        );
        assert.equal(
            mock.columnsMenu,
            mock.makeMenu.lastCall.returnValue,
            'assigns makeMenu return value as this.columnsMenu'
        );
        assert.equal(
            mock.selectedColumns,
            mock.getSelectedColumns.lastCall.returnValue,
            'assigns getSelectedColumns return value as this.selectedColumns'
        );
        assert.equal(
            mock.setSelectedColumns.callCount,
            1,
            'calls setSelectedColumns once'
        );
    });
    test('getSelectedColumns, using categories', () => {
        const c = new Controller(...mocks().values());
        c.state = {
            categories: [
                {isVisible: false},
                {isVisible: true, isInMenu: false},
                {isVisible: true, isInMenu: true, item: {categoryDisplayName: '1'}},
                {isVisible: true, isInMenu: true, item: {displayName: '2'}},
                {isVisible: true, isInMenu: true, item: {name: '3'}}
            ],
            columns: []
        };

        assert.deepEqual(
            c.getSelectedColumns(),
            ['1', '2', '3'],
            'returns correct value, prefers categories over columns'
        );
    });
    test('getSelectedColumns, using columnDefs', () => {
        const c = new Controller(...mocks().values());
        c.state = {
            categories: [
            ],
            columns: [
                {isVisible: false},
                {isVisible: true, isInMenu: false},
                {isVisible: true, isInMenu: true, item: {categoryDisplayName: '1'}},
                {isVisible: true, isInMenu: true, item: {displayName: '2'}},
                {isVisible: true, isInMenu: true, item: {name: '3'}}
            ]
        };

        assert.deepEqual(
            c.getSelectedColumns(),
            ['1', '2', '3'],
            'returns correct value, uses columns if there are no categories'
        );
    });
    test('makeMenu, using categories', () => {
        const c = new Controller(...mocks().values());
        c.state = {
            categories: [
                {isVisible: false},
                {isVisible: true, isInMenu: false},
                {isVisible: true, isInMenu: true, item: {categoryDisplayName: '1'}},
                {isVisible: true, isInMenu: true, item: {displayName: '2'}},
                {isVisible: true, isInMenu: true, item: {name: '3'}}
            ],
            columns: []
        };

        assert.deepEqual(
            c.makeMenu(),
            [
                {item: {categoryDisplayName: '1'}, name: '1'},
                {item: {displayName: '2'}, name: '2'},
                {item: {name: '3'}, name: '3'}
            ],
            'returns correct value, prefers categories over columns'
        );
    });
    test('makeMenu, using columns', () => {
        const c = new Controller(...mocks().values());
        c.state = {
            categories: [],
            columns: [
                {isVisible: false},
                {isVisible: true, isInMenu: false},
                {isVisible: true, isInMenu: true, item: {categoryDisplayName: '1'}},
                {isVisible: true, isInMenu: true, item: {displayName: '2'}},
                {isVisible: true, isInMenu: true, item: {name: '3'}}
            ]
        };

        assert.deepEqual(
            c.makeMenu(),
            [
                {item: {categoryDisplayName: '1'}, name: '1'},
                {item: {displayName: '2'}, name: '2'},
                {item: {name: '3'}, name: '3'}
            ],
            'returns correct value, uses columns if there are no categories'
        );
    });
    test('getState', () => {
        const c = new Controller(...mocks().values());
        c.gridApi = apiMock();
        c.gridApi.grouping.getGrouping = () => ({grouping: [{colName: 'a', categoryDisplayName: 'A'}]});
        c.gridApi.grid.options.columnDefs = [
            {visible: false, name: 'a', categoryDisplayName: 'A'},
            {visible: true, name: 'a1', categoryDisplayName: 'A'},
            {visible: true, name: 'a2', categoryDisplayName: 'A'},
            {visible: true, name: 'b1', categoryDisplayName: 'B'},
            {visible: true, name: 'b2', categoryDisplayName: 'B'},
            {visible: true, name: 'b3', categoryDisplayName: 'B'},
            {visible: true, name: 'c1', categoryDisplayName: 'C'},
            {visible: true, name: 'c2', categoryDisplayName: 'C', enableHiding: false},
            {visible: false, name: 'c3', categoryDisplayName: 'C'}
        ];
        c.gridApi.grid.options.categories = [
            {categoryDisplayName: 'A', enableHiding: false, visible: true},
            {categoryDisplayName: 'B', enableHiding: true, visible: false},
            {categoryDisplayName: 'C', enableHiding: true, visible: true}
        ];

        assert.deepEqual(
            c.getState(),
            {
                categories: [{
                    isInMenu: false,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'A',
                        enableHiding: false,
                        visible: true
                    }
                }, {
                    isInMenu: true,
                    isVisible: false,
                    item: {
                        categoryDisplayName: 'B',
                        enableHiding: true,
                        visible: false
                    }
                }, {
                    isInMenu: true,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'C',
                        enableHiding: true,
                        visible: true
                    }
                }],
                columns: [{
                    isInMenu: true,
                    isVisible: false,
                    item: {
                        categoryDisplayName: 'A',
                        name: 'a',
                        visible: false
                    }
                }, {
                    isInMenu: true,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'A',
                        name: 'a1',
                        visible: true
                    }
                }, {
                    isInMenu: true,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'A',
                        name: 'a2',
                        visible: true
                    }
                }, {
                    isInMenu: true,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'B',
                        name: 'b1',
                        visible: true
                    }
                }, {
                    isInMenu: true,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'B',
                        name: 'b2',
                        visible: true
                    }
                }, {
                    isInMenu: true,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'B',
                        name: 'b3',
                        visible: true
                    }
                }, {
                    isInMenu: true,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'C',
                        name: 'c1',
                        visible: true
                    }
                }, {
                    isInMenu: false,
                    isVisible: true,
                    item: {
                        categoryDisplayName: 'C',
                        name: 'c2',
                        visible: true,
                        enableHiding: false
                    }
                }, {
                    isInMenu: true,
                    isVisible: false,
                    item: {
                        categoryDisplayName: 'C',
                        name: 'c3',
                        visible: false
                    }
                }]
            },
            'returns correct value'
        );
    });
    test('findScrollToNext', () => {
        assert.deepEqual(
            Controller.prototype.findScrollToNext([1, 2, 3], [1, 2]),
            3,
            `returns new item if it's in selected collection end`
        );
        assert.deepEqual(
            Controller.prototype.findScrollToNext([1, 2], [1, 3]),
            2,
            `returns new item if it's in selected collection middle`
        );
        assert.deepEqual(
            Controller.prototype.findScrollToNext([1, 2, 3], [2, 3]),
            1,
            `returns new item if it's in selected collection start`
        );
        assert.equal(
            Controller.prototype.findScrollToNext([1, 2, 3, 4, 5], [1]),
            void 0,
            `returns nothing if there's more than one new item`
        );
        assert.equal(
            Controller.prototype.findScrollToNext([1, 2], [1, 2, 3]),
            void 0,
            `returns nothing if items were removed`
        );
    });
    test('setSelectedColumns', () => {
        const c = new Controller(...mocks().values());
        const api = c.gridApi = apiMock();
        c.refreshColumns = stub().returns(({then: (f) => f()}));
        c.gridApi.grid.options.columnDefs = [
            {name: 'a', visible: false, categoryDisplayName: 'A'},
            {name: 'a1', visible: false, categoryDisplayName: 'A'},
            {name: 'b', visible: true, categoryDisplayName: 'B'}
        ];
        c.gridApi.grid.options.categories = [
            {categoryDisplayName: 'A', visible: false},
            {categoryDisplayName: 'B'}
        ];
        c.$onChanges({gridApi: {currentValue: api}});
        c.selectedColumns = ['A', 'B'];
        c.setSelectedColumns();

        assert.equal(
            c.refreshColumns.callCount,
            2,
            'calls refreshColumns'
        );
        assert.deepEqual(
            c.gridApi.grid.options.categories,
            [
                {categoryDisplayName: 'A', visible: true},
                {categoryDisplayName: 'B', visible: true}
            ],
            'changes category visibility'
        );
        assert.deepEqual(
            c.gridApi.grid.options.columnDefs,
            [
                {name: 'a', visible: true, categoryDisplayName: 'A'},
                {name: 'a1', visible: true, categoryDisplayName: 'A'},
                {name: 'b', visible: true, categoryDisplayName: 'B'}
            ],
            'changes column visibility'
        );
        assert.deepEqual(
            c.gridApi.grid.scrollTo.lastCall.args,
            [null, {name: 'a1', visible: true, categoryDisplayName: 'A'}],
            'scrolls to last added column'
        );
    });
    test('refreshColumns', () => {
        const c = new Controller(...mocks().values());
        c.gridApi = apiMock();
        c.gridApi.grid.columns = [1, 2, 3];

        return c.refreshColumns().then(() => {
            assert.equal(
                c.gridApi.grid.processColumnsProcessors.lastCall.args[0],
                c.gridApi.grid.columns,
                'calls processColumnsProcessors with correct args'
            );
            assert.equal(
                c.gridApi.grid.setVisibleColumns.lastCall.args[0],
                c.gridApi.grid.columns,
                'calls setVisibleColumns with correct args'
            );
            assert.equal(
                c.gridApi.grid.redrawInPlace.callCount,
                1,
                'calls redrawInPlace'
            );
            assert.equal(
                c.gridApi.grid.refreshCanvas.lastCall.args[0],
                true,
                'calls refreshCanvas with correct args'
            );
        });
    });
});
