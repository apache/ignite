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

import _ from 'lodash';

import columnDefs from './column-defs';
import categories from './categories';

import headerTemplate from 'app/primitives/ui-grid-header/index.tpl.pug';

const rowTemplate = `<div
  ng-repeat="(colRenderIndex, col) in colContainer.renderedColumns track by col.uid"
  ui-grid-one-bind-id-grid="rowRenderIndex + '-' + col.uid + '-cell'"
  class="ui-grid-cell"
  ng-class="{ 'ui-grid-row-header-cell': col.isRowHeader }"
  role="{{col.isRowHeader ? 'rowheader' : 'gridcell'}}"
  ui-grid-cell/>`;

const treeAggregationFinalizerFn = function(agg) {
    return agg.rendered = agg.value;
};

export default class IgniteListOfRegisteredUsersCtrl {
    static $inject = ['$scope', '$state', '$filter', 'User', 'uiGridGroupingConstants', 'uiGridPinningConstants', 'IgniteAdminData', 'IgniteNotebookData', 'IgniteConfirm', 'IgniteActivitiesUserDialog'];

    constructor($scope, $state, $filter, User, uiGridGroupingConstants, uiGridPinningConstants, AdminData, NotebookData, Confirm, ActivitiesUserDialog) {
        this.$state = $state;
        this.AdminData = AdminData;
        this.ActivitiesDialogFactory = ActivitiesUserDialog;
        this.Confirm = Confirm;
        this.User = User;
        this.NotebookData = NotebookData;

        const dtFilter = $filter('date');

        this.groupBy = 'user';

        this.selected = [];

        this.params = {
            startDate: new Date(),
            endDate: new Date()
        };

        this.uiGridPinningConstants = uiGridPinningConstants;
        this.uiGridGroupingConstants = uiGridGroupingConstants;

        User.read().then((user) => this.user = user);

        const companiesExcludeFilter = (renderableRows) => {
            if (_.isNil(this.params.companiesExclude))
                return renderableRows;

            _.forEach(renderableRows, (row) => {
                row.visible = _.isEmpty(this.params.companiesExclude) ||
                    row.entity.company.toLowerCase().indexOf(this.params.companiesExclude.toLowerCase()) === -1;
            });

            return renderableRows;
        };

        this.actionOptions = [
            {
                action: 'Become this user',
                click: () => this.becomeUser(),
                available: true
            },
            {
                action: 'Revoke admin',
                click: () => this.toggleAdmin(),
                available: true
            },
            {
                action: 'Grant admin',
                click: () => this.toggleAdmin(),
                available: false
            },
            {
                action: 'Add user',
                sref: '.createUser',
                available: true
            },
            {
                action: 'Remove user',
                click: () => this.removeUser(),
                available: true
            },
            {
                action: 'Activity detail',
                click: () => this.showActivities(),
                available: true
            }
        ];

        this._userGridOptions = {
            columnDefs,
            categories
        };

        this.gridOptions = {
            data: [],

            columnDefs,
            categories,

            treeRowHeaderAlwaysVisible: true,
            headerTemplate,
            columnVirtualizationThreshold: 30,
            rowTemplate,
            rowHeight: 46,
            selectWithCheckboxOnly: true,
            suppressRemoveSort: false,
            enableFiltering: true,
            enableSelectAll: true,
            enableRowSelection: true,
            enableFullRowSelection: true,
            enableColumnMenus: false,
            multiSelect: false,
            modifierKeysToMultiSelect: true,
            noUnselect: false,
            fastWatch: true,
            exporterSuppressColumns: ['actions'],
            exporterCsvColumnSeparator: ';',
            rowIdentity: (row) => row._id,
            getRowIdentity: (row) => row._id,
            onRegisterApi: (api) => {
                this.gridApi = api;

                api.selection.on.rowSelectionChanged($scope, this._updateSelected.bind(this));
                api.selection.on.rowSelectionChangedBatch($scope, this._updateSelected.bind(this));

                api.core.on.filterChanged($scope, this._filteredRows.bind(this));
                api.core.on.rowsVisibleChanged($scope, this._filteredRows.bind(this));

                api.grid.registerRowsProcessor(companiesExcludeFilter, 50);

                $scope.$watch(() => this.gridApi.grid.getVisibleRows().length, (rows) => this.adjustHeight(rows));
                $scope.$watch(() => this.params.companiesExclude, () => this.gridApi.grid.refreshRows());
            }
        };

        /**
         * @param {{startDate: number, endDate: number}} params
         */
        const reloadUsers = (params) => {
            AdminData.loadUsers(params)
                .then((data) => {
                    this.gridOptions.data = data;

                    this.companies = _.values(_.groupBy(data, 'company'));
                    this.countries = _.values(_.groupBy(data, 'countryCode'));

                    this._refreshRows();
                });
        };

        const filterDates = _.debounce(() => {
            const sdt = this.params.startDate;
            const edt = this.params.endDate;

            this.exporterCsvFilename = `web_console_users_${dtFilter(sdt, 'yyyy_MM')}.csv`;

            const startDate = Date.UTC(sdt.getFullYear(), sdt.getMonth(), 1);
            const endDate = Date.UTC(edt.getFullYear(), edt.getMonth() + 1, 1);

            reloadUsers({ startDate, endDate });
        }, 250);

        $scope.$on('userCreated', filterDates);
        $scope.$watch(() => this.params.startDate, filterDates);
        $scope.$watch(() => this.params.endDate, filterDates);
    }

    adjustHeight(rows) {
        // Add header height.
        const height = Math.min(rows, 11) * 48 + 78;

        this.gridApi.grid.element.css('height', height + 'px');

        this.gridApi.core.handleWindowResize();
    }

    _filteredRows() {
        const filtered = _.filter(this.gridApi.grid.rows, ({ visible}) => visible);

        this.filteredRows = _.map(filtered, 'entity');
    }

    _updateSelected() {
        const ids = this.gridApi.selection.legacyGetSelectedRows().map(({ _id }) => _id).sort();

        if (!_.isEqual(ids, this.selected))
            this.selected = ids;

        if (ids.length) {
            const user = this.gridApi.selection.legacyGetSelectedRows()[0];
            const other = this.user._id !== user._id;

            this.actionOptions[0].available = other; // Become this user.
            this.actionOptions[1].available = other && user.admin; // Revoke admin.
            this.actionOptions[2].available = other && !user.admin; // Grant admin.
            this.actionOptions[4].available = other; // Remove user.
            this.actionOptions[5].available = true; // Activity detail.
        }
        else {
            this.actionOptions[0].available = false; // Become this user.
            this.actionOptions[1].available = false; // Revoke admin.
            this.actionOptions[2].available = false; // Grant admin.
            this.actionOptions[4].available = false; // Remove user.
            this.actionOptions[5].available = false; // Activity detail.
        }
    }

    _refreshRows() {
        if (this.gridApi) {
            this.gridApi.grid.refreshRows()
                .then(() => this._updateSelected());
        }
    }

    becomeUser() {
        const user = this.gridApi.selection.legacyGetSelectedRows()[0];

        this.AdminData.becomeUser(user._id)
            .then(() => this.User.load())
            .then(() => this.$state.go('default-state'))
            .then(() => this.NotebookData.load());
    }

    toggleAdmin() {
        if (!this.gridApi)
            return;

        const user = this.gridApi.selection.legacyGetSelectedRows()[0];

        if (user.adminChanging)
            return;

        user.adminChanging = true;

        this.AdminData.toggleAdmin(user)
            .finally(() => {
                this._updateSelected();

                user.adminChanging = false;
            });
    }

    removeUser() {
        const user = this.gridApi.selection.legacyGetSelectedRows()[0];

        this.Confirm.confirm(`Are you sure you want to remove user: "${user.userName}"?`)
            .then(() => this.AdminData.removeUser(user))
            .then(() => {
                const i = _.findIndex(this.gridOptions.data, (u) => u._id === user._id);

                if (i >= 0) {
                    this.gridOptions.data.splice(i, 1);
                    this.gridApi.selection.clearSelectedRows();
                }

                this.adjustHeight(this.gridOptions.data.length);

                return this._refreshRows();
            });
    }

    showActivities() {
        const user = this.gridApi.selection.legacyGetSelectedRows()[0];

        return new this.ActivitiesDialogFactory({ user });
    }

    groupByUser() {
        this.groupBy = 'user';

        this.gridApi.grouping.clearGrouping();
        this.gridApi.selection.clearSelectedRows();

        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'company'}), (col) => {
            this.gridApi.pinning.pinColumn(col, this.uiGridPinningConstants.container.NONE);
        });

        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'country'}), (col) => {
            this.gridApi.pinning.pinColumn(col, this.uiGridPinningConstants.container.NONE);
        });

        this.gridOptions.categories = categories;
    }

    groupByCompany() {
        this.groupBy = 'company';

        this.gridApi.grouping.clearGrouping();
        this.gridApi.selection.clearSelectedRows();

        _.forEach(this.gridApi.grid.columns, (col) => {
            col.enableSorting = true;

            if (col.colDef.type !== 'number')
                return;

            this.gridApi.grouping.aggregateColumn(col.colDef.name, this.uiGridGroupingConstants.aggregation.SUM);
            col.customTreeAggregationFinalizerFn = treeAggregationFinalizerFn;
        });

        this.gridApi.grouping.aggregateColumn('user', this.uiGridGroupingConstants.aggregation.COUNT);
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'user'}), (col) => {
            col.customTreeAggregationFinalizerFn = treeAggregationFinalizerFn;
        });

        this.gridApi.grouping.aggregateColumn('lastactivity', this.uiGridGroupingConstants.aggregation.MAX);
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'lastactivity'}), (col) => {
            col.customTreeAggregationFinalizerFn = treeAggregationFinalizerFn;
        });

        this.gridApi.grouping.groupColumn('company');
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'company'}), (col) => {
            col.customTreeAggregationFinalizerFn = (agg) => agg.rendered = agg.groupVal;
        });

        // Pinning left company.
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'company'}), (col) => {
            this.gridApi.pinning.pinColumn(col, this.uiGridPinningConstants.container.LEFT);
        });

        // Unpinning country.
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'country'}), (col) => {
            this.gridApi.pinning.pinColumn(col, this.uiGridPinningConstants.container.NONE);
        });

        const _categories = _.cloneDeep(categories);
        // Cut company category.
        const company = _categories.splice(3, 1)[0];
        company.selectable = false;

        // Add company as first column.
        _categories.unshift(company);
        this.gridOptions.categories = _categories;
    }

    groupByCountry() {
        this.groupBy = 'country';

        this.gridApi.grouping.clearGrouping();
        this.gridApi.selection.clearSelectedRows();

        _.forEach(this.gridApi.grid.columns, (col) => {
            col.enableSorting = true;

            if (col.colDef.type !== 'number')
                return;

            this.gridApi.grouping.aggregateColumn(col.colDef.name, this.uiGridGroupingConstants.aggregation.SUM);
            col.customTreeAggregationFinalizerFn = treeAggregationFinalizerFn;
        });

        this.gridApi.grouping.aggregateColumn('user', this.uiGridGroupingConstants.aggregation.COUNT);
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'user'}), (col) => {
            col.customTreeAggregationFinalizerFn = treeAggregationFinalizerFn;
        });

        this.gridApi.grouping.aggregateColumn('lastactivity', this.uiGridGroupingConstants.aggregation.MAX);
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'lastactivity'}), (col) => {
            col.customTreeAggregationFinalizerFn = treeAggregationFinalizerFn;
        });

        this.gridApi.grouping.groupColumn('country');
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'country'}), (col) => {
            col.customTreeAggregationFinalizerFn = (agg) => agg.rendered = agg.groupVal;
        });

        // Pinning left country.
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'country'}), (col) => {
            this.gridApi.pinning.pinColumn(col, this.uiGridPinningConstants.container.LEFT);
        });

        // Unpinning country.
        _.forEach(_.filter(this.gridApi.grid.columns, {name: 'company'}), (col) => {
            this.gridApi.pinning.pinColumn(col, this.uiGridPinningConstants.container.NONE);
        });

        const _categories = _.cloneDeep(categories);
        // Cut company category.
        const country = _categories.splice(4, 1)[0];
        country.selectable = false;

        // Add company as first column.
        _categories.unshift(country);
        this.gridOptions.categories = _categories;
    }
}
