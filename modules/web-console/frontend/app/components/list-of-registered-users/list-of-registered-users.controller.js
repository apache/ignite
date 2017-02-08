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

import headerTemplate from 'app/components/ui-grid-header/ui-grid-header.jade';

import columnDefs from './list-of-registered-users.column-defs';
import categories from './list-of-registered-users.categories';

export default class IgniteListOfRegisteredUsersCtrl {
    static $inject = ['$scope', '$state', '$templateCache', 'User', 'uiGridConstants', 'IgniteAdminData', 'IgniteNotebookData', 'IgniteConfirm', 'IgniteActivitiesUserDialog'];

    constructor($scope, $state, $templateCache, User, uiGridConstants, AdminData, NotebookData, Confirm, ActivitiesUserDialog) {
        const $ctrl = this;

        const companySelectOptions = [];
        const countrySelectOptions = [];

        $ctrl.params = {
            startDate: new Date()
        };

        $ctrl.params.startDate.setDate(1);
        $ctrl.params.startDate.setHours(0, 0, 0, 0);

        const columnCompany = _.find(columnDefs, { displayName: 'Company' });
        const columnCountry = _.find(columnDefs, { displayName: 'Country' });

        columnCompany.filter = {
            selectOptions: companySelectOptions,
            type: uiGridConstants.filter.SELECT,
            condition: uiGridConstants.filter.EXACT
        };

        columnCountry.filter = {
            selectOptions: countrySelectOptions,
            type: uiGridConstants.filter.SELECT,
            condition: uiGridConstants.filter.EXACT
        };

        const becomeUser = (user) => {
            AdminData.becomeUser(user._id)
                .then(() => User.load())
                .then(() => $state.go('base.configuration.clusters'))
                .then(() => NotebookData.load());
        };

        const removeUser = (user) => {
            Confirm.confirm(`Are you sure you want to remove user: "${user.userName}"?`)
                .then(() => AdminData.removeUser(user))
                .then(() => {
                    const i = _.findIndex($ctrl.gridOptions.data, (u) => u._id === user._id);

                    if (i >= 0)
                        $ctrl.gridOptions.data.splice(i, 1);
                })
                .then(() => $ctrl.adjustHeight($ctrl.gridOptions.data.length));
        };

        const toggleAdmin = (user) => {
            if (user.adminChanging)
                return;

            user.adminChanging = true;

            AdminData.toggleAdmin(user)
                .then(() => user.admin = !user.admin)
                .finally(() => user.adminChanging = false);
        };

        const showActivities = (user) => {
            return new ActivitiesUserDialog({ user, params: $ctrl.params });
        };

        $ctrl.gridOptions = {
            data: [],
            columnVirtualizationThreshold: 30,
            columnDefs,
            categories,
            headerTemplate: $templateCache.get(headerTemplate),
            enableFiltering: true,
            enableRowSelection: false,
            enableRowHeaderSelection: false,
            enableColumnMenus: false,
            multiSelect: false,
            modifierKeysToMultiSelect: true,
            noUnselect: true,
            fastWatch: true,
            onRegisterApi: (api) => {
                $ctrl.gridApi = api;

                api.becomeUser = becomeUser;
                api.removeUser = removeUser;
                api.toggleAdmin = toggleAdmin;
                api.showActivities = showActivities;
            }
        };

        const usersToFilterOptions = (column) => {
            return _.sortBy(
                _.map(
                    _.groupBy($ctrl.gridOptions.data, (usr) => {
                        const fld = usr[column];

                        return _.isNil(fld) ? fld : fld.toUpperCase();
                    }),
                    (arr, value) => ({label: `${_.head(arr)[column] || 'Not set'} (${arr.length})`, value})
                ),
                'value');
        };

        /**
         * @param {{startDate: Date, endDate: Date}} params
         */
        const reloadUsers = (params) => {
            AdminData.loadUsers(params)
                .then((data) => $ctrl.gridOptions.data = data)
                .then((data) => {
                    companySelectOptions.push(...usersToFilterOptions('company'));
                    countrySelectOptions.push(...usersToFilterOptions('countryCode'));

                    this.gridApi.grid.refresh();

                    return data;
                })
                .then((data) => $ctrl.adjustHeight(data.length));
        };

        $scope.$watch(() => $ctrl.params.startDate, () => {
            const endDate = new Date($ctrl.params.startDate);

            endDate.setMonth(endDate.getMonth() + 1);

            $ctrl.params.endDate = endDate;

            reloadUsers($ctrl.params);
        });
    }

    adjustHeight(rows) {
        const height = Math.min(rows, 20) * 30 + 75;

        // Remove header height.
        this.gridApi.grid.element.css('height', height + 'px');

        this.gridApi.core.handleWindowResize();
    }

    _enableColumns(_categories, visible) {
        _.forEach(_categories, (cat) => {
            cat.visible = visible;

            _.forEach(this.gridOptions.columnDefs, (col) => {
                if (col.categoryDisplayName === cat.name)
                    col.visible = visible;
            });
        });

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
        this.gridApi.exporter.csvExport('all', 'visible');
    }
}
