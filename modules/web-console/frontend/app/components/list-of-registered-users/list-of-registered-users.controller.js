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

import headerTemplate from 'app/primitives/ui-grid-header/index.tpl.pug';

import columnDefs from './list-of-registered-users.column-defs';
import categories from './list-of-registered-users.categories';

const rowTemplate = `<div
  ng-repeat="(colRenderIndex, col) in colContainer.renderedColumns track by col.uid"
  ui-grid-one-bind-id-grid="rowRenderIndex + '-' + col.uid + '-cell'"
  class="ui-grid-cell"
  ng-class="{ 'ui-grid-row-header-cell': col.isRowHeader }"
  role="{{col.isRowHeader ? 'rowheader' : 'gridcell'}}"
  ui-grid-cell/>`;

export default class IgniteListOfRegisteredUsersCtrl {
    static $inject = ['$scope', '$state', '$filter', 'User', 'uiGridGroupingConstants', 'IgniteAdminData', 'IgniteNotebookData', 'IgniteConfirm', 'IgniteActivitiesUserDialog'];

    constructor($scope, $state, $filter, User, uiGridGroupingConstants, AdminData, NotebookData, Confirm, ActivitiesUserDialog) {
        const $ctrl = this;

        const dtFilter = $filter('date');

        $ctrl.groupBy = 'user';

        $ctrl.selected = [];

        $ctrl.params = {
            startDate: new Date(),
            endDate: new Date()
        };

        $ctrl.uiGridGroupingConstants = uiGridGroupingConstants;

        User.read().then((user) => $ctrl.user = user);

        const becomeUser = () => {
            const user = this.gridApi.selection.getSelectedRows()[0];

            AdminData.becomeUser(user._id)
                .then(() => User.load())
                .then(() => $state.go('base.configuration.clusters'))
                .then(() => NotebookData.load());
        };

        const removeUser = () => {
            const user = this.gridApi.selection.getSelectedRows()[0];

            Confirm.confirm(`Are you sure you want to remove user: "${user.userName}"?`)
                .then(() => AdminData.removeUser(user))
                .then(() => {
                    const i = _.findIndex($ctrl.gridOptions.data, (u) => u._id === user._id);

                    if (i >= 0)
                        $ctrl.gridOptions.data.splice(i, 1);
                })
                .then(() => $ctrl.adjustHeight($ctrl.gridOptions.data.length));
        };

        const toggleAdmin = () => {
            const user = this.gridApi.selection.getSelectedRows()[0];

            if (user.adminChanging)
                return;

            user.adminChanging = true;

            AdminData.toggleAdmin(user)
                .then(() => user.admin = !user.admin)
                .finally(() => user.adminChanging = false);
        };

        const showActivities = () => {
            const user = this.gridApi.selection.getSelectedRows()[0];

            return new ActivitiesUserDialog({ user });
        };

        const companiesExcludeFilter = (renderableRows) => {
            if (_.isNil($ctrl.params.companiesExclude))
                return renderableRows;

            _.forEach(renderableRows, (row) => {
                row.visible = _.isEmpty($ctrl.params.companiesExclude) ||
                    row.entity.company.toLowerCase().indexOf($ctrl.params.companiesExclude.toLowerCase()) === -1;
            });

            return renderableRows;
        };

        $ctrl.actionOptions = [
            {
                action: 'Become this user',
                click: becomeUser.bind(this),
                available: true
            },
            {
                action: 'Revoke admin',
                click: toggleAdmin.bind(this),
                available: true
            },
            {
                action: 'Grant admin',
                click: toggleAdmin.bind(this),
                available: false
            },
            {
                action: 'Remove user',
                click: removeUser.bind(this),
                available: true
            },
            {
                action: 'Activity detail',
                click: showActivities.bind(this),
                available: true
            }
        ];

        $ctrl._userGridOptions = {
            columnDefs,
            categories
        };

        $ctrl.gridOptions = {
            data: [],

            columnDefs,
            categories,

            headerTemplate,
            columnVirtualizationThreshold: 30,
            rowTemplate,
            rowHeight: 46,
            selectWithCheckboxOnly: true,
            selectionRowHeaderWidth: 52,
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
            onRegisterApi: (api) => {
                $ctrl.gridApi = api;

                api.selection.on.rowSelectionChanged($scope, $ctrl._updateSelected.bind($ctrl));
                api.selection.on.rowSelectionChangedBatch($scope, $ctrl._updateSelected.bind($ctrl));

                api.core.on.filterChanged($scope, $ctrl._filteredRows.bind($ctrl));
                api.core.on.rowsVisibleChanged($scope, $ctrl._filteredRows.bind($ctrl));

                api.grid.registerRowsProcessor(companiesExcludeFilter, 50);

                $scope.$watch(() => $ctrl.gridApi.grid.getVisibleRows().length, (rows) => $ctrl.adjustHeight(rows));
                $scope.$watch(() => $ctrl.params.companiesExclude, () => $ctrl.gridApi.grid.refreshRows());
            }
        };

        /**
         * @param {{startDate: number, endDate: number}} params
         */
        const reloadUsers = (params) => {
            AdminData.loadUsers(params)
                .then((data) => $ctrl.gridOptions.data = data)
                .then((data) => {
                    this.gridApi.grid.refresh();

                    this.companies = _.values(_.groupBy(data, (b) => b.company.toLowerCase()));
                    this.countries = _.values(_.groupBy(data, (b) => b.countryCode));

                    return data;
                });
        };

        const filterDates = (sdt, edt) => {
            $ctrl.gridOptions.exporterCsvFilename = `web_console_users_${dtFilter(sdt, 'yyyy_MM')}.csv`;

            const startDate = Date.UTC(sdt.getFullYear(), sdt.getMonth(), 1);
            const endDate = Date.UTC(edt.getFullYear(), edt.getMonth() + 1, 1);

            reloadUsers({ startDate, endDate });
        };

        $scope.$watch(() => $ctrl.params.startDate, (sdt) => filterDates(sdt, $ctrl.params.endDate));
        $scope.$watch(() => $ctrl.params.endDate, (edt) => filterDates($ctrl.params.startDate, edt));
    }

    adjustHeight(rows) {
        // Add header height.
        const height = Math.min(rows, 20) * 48 + 78;

        this.gridApi.grid.element.css('height', height + 'px');

        this.gridApi.core.handleWindowResize();
    }

    _filteredRows() {
        const filtered = _.filter(this.gridApi.grid.rows, ({ visible}) => visible);
        const entities = _.map(filtered, 'entity');

        this.filteredRows = entities;
    }

    _updateSelected() {
        const ids = this.gridApi.selection.getSelectedRows().map(({ _id }) => _id).sort();

        if (ids.length) {
            const user = this.gridApi.selection.getSelectedRows()[0];
            const other = this.user._id !== user._id;

            this.actionOptions[1].available = other && user.admin;
            this.actionOptions[2].available = other && !user.admin;

            this.actionOptions[0].available = other;
            this.actionOptions[3].available = other;
        }

        if (!_.isEqual(ids, this.selected))
            this.selected = ids;
    }

    _enableColumns(_categories, visible) {
        _.forEach(_categories, (cat) => {
            cat.visible = visible;

            _.forEach(this.gridOptions.columnDefs, (col) => {
                if (col.categoryDisplayName === cat.name)
                    col.visible = visible;
            });
        });

        // Check to all selected columns.
        this.gridOptions.selectedAll = true;
        _.forEach(this._selectableColumns(), ({ visible }) => this.gridOptions.selectedAll = visible);

        // Workaround for this.gridApi.grid.refresh() didn't return promise.
        this.gridApi.grid.processColumnsProcessors(this.gridApi.grid.columns)
            .then((renderableColumns) => this.gridApi.grid.setVisibleColumns(renderableColumns))
            .then(() => this.gridApi.grid.redrawInPlace())
            .then(() => this.gridApi.grid.refreshCanvas(true))
            .then(() => {
                if (visible) {
                    const categoryDisplayName = _.last(_categories).name;

                    const col = _.findLast(this.gridOptions.columnDefs, {categoryDisplayName});

                    this.gridApi.grid.scrollTo(null, col);
                }
            });
    }

    _selectableColumns() {
        return _.filter(this.gridOptions.categories, (cat) => cat.selectable);
    }

    toggleColumns(category, visible) {
        this._enableColumns([category], visible);
    }

    selectAllColumns() {
        this._enableColumns(this._selectableColumns(), true);
    }

    clearAllColumns() {
        this._enableColumns(this._selectableColumns(), false);
    }

    exportCsv() {
        this.gridApi.exporter.csvExport('visible', 'visible');
    }

    groupByUser() {
        this.groupBy = 'user';

        this.gridApi.grouping.clearGrouping();
        this.gridOptions.categories = this._userGridOptions.categories;
        this.gridOptions.columnDefs = this._userGridOptions.columnDefs;
    }

    groupByCompany() {
        this.groupBy = 'company';

        this.gridApi.grouping.clearGrouping();

        this.gridApi.grouping.groupColumn('company');
        this.gridApi.grouping.aggregateColumn('user', this.uiGridGroupingConstants.aggregation.COUNT);

        if (this._companyGridOptions) {
            this.gridOptions.categories = this._companyGridOptions.categories;
            this.gridOptions.columnDefs = this._companyGridOptions.columnDefs;

            return;
        }

        const _categories = _.cloneDeep(categories);
        const _columnDefs = _.cloneDeep(columnDefs);

        // Cut company category.
        const company = _categories.splice(3, 1)[0];
        company.selectable = false;

        // Hide Actions category.
        _categories.splice(0, 1);

        _.forEach(_.filter(_columnDefs, {displayName: 'Actions'}), (col) => {
            col.visible = false;
        });

        // Add company as first column.
        _categories.unshift(company);

        _.forEach(_columnDefs, (col) => {
            col.enableSorting = true;

            if (col.type !== 'number')
                return;

            col.treeAggregationType = this.uiGridGroupingConstants.aggregation.SUM;
            col.customTreeAggregationFinalizerFn = (agg) => agg.rendered = agg.value;
        });

        // Set grouping to last activity column.
        const lastactivity = _.find(_columnDefs, { name: 'lastactivity' });

        if (_.nonNil(lastactivity)) {
            lastactivity.treeAggregationType = this.uiGridGroupingConstants.aggregation.MAX;
            lastactivity.customTreeAggregationFinalizerFn = (agg) => agg.rendered = agg.value;
        }

        this._companyGridOptions = {
            categories: this.gridOptions.categories = _categories,
            columnDefs: this.gridOptions.columnDefs = _columnDefs
        };
    }

    groupByCountry() {
        this.groupBy = 'country';

        this.gridApi.grouping.clearGrouping();
        this.gridApi.grouping.groupColumn('country');
        this.gridApi.grouping.aggregateColumn('user', this.uiGridGroupingConstants.aggregation.COUNT);

        if (this._countryGridOptions) {
            this.gridOptions.categories = this._countryGridOptions.categories;
            this.gridOptions.columnDefs = this._countryGridOptions.columnDefs;

            return;
        }

        const _categories = _.cloneDeep(categories);
        const _columnDefs = _.cloneDeep(columnDefs);

        // Cut country category.
        const country = _categories.splice(4, 1)[0];
        country.selectable = false;

        // Hide Actions category.
        _categories.splice(0, 1);

        _.forEach(_.filter(_columnDefs, {displayName: 'Actions'}), (col) => {
            col.visible = false;
        });

        // Add company as first column.
        _categories.unshift(country);

        _.forEach(_columnDefs, (col) => {
            col.enableSorting = true;

            if (col.type !== 'number')
                return;

            col.treeAggregationType = this.uiGridGroupingConstants.aggregation.SUM;
            col.customTreeAggregationFinalizerFn = (agg) => agg.rendered = agg.value;
        });

        // Set grouping to last activity column.
        const lastactivity = _.find(_columnDefs, { name: 'lastactivity' });

        if (_.nonNil(lastactivity)) {
            lastactivity.treeAggregationType = this.uiGridGroupingConstants.aggregation.MAX;
            lastactivity.customTreeAggregationFinalizerFn = (agg) => agg.rendered = agg.value;
        }

        this._countryGridOptions = {
            categories: this.gridOptions.categories = _categories,
            columnDefs: this.gridOptions.columnDefs = _columnDefs
        };
    }
}
