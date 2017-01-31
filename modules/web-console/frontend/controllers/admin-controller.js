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

const ICON_SORT = '<span ui-grid-one-bind-id-grid="col.uid + \'-sortdir-text\'" ui-grid-visible="col.sort.direction" aria-label="Sort Descending"><i ng-class="{ \'ui-grid-icon-up-dir\': col.sort.direction == asc, \'ui-grid-icon-down-dir\': col.sort.direction == desc, \'ui-grid-icon-blank\': !col.sort.direction }" title="" aria-hidden="true"></i></span>';

const CLUSTER_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-sitemap'></i>${ICON_SORT}</div>`;
const MODEL_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-object-group'></i>${ICON_SORT}</div>`;
const CACHE_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-database'></i>${ICON_SORT}</div>`;
const IGFS_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-folder-o'></i>${ICON_SORT}</div>`;

const ACTIONS_TEMPLATE = `
<div class='text-center ui-grid-cell-actions'>
    <a class='btn btn-default dropdown-toggle' bs-dropdown='' ng-show='row.entity._id != $root.user._id' data-placement='bottom-right' data-container='.panel'>
        <i class='fa fa-gear'></i>&nbsp;
        <span class='caret'></span>
    </a>
    <ul class='dropdown-menu' role='menu'>
        <li>
            <a ng-click='grid.api.becomeUser(row.entity)'>Become this user</a>
        </li>
        <li>
            <a ng-click='grid.api.toggleAdmin(row.entity)' ng-if='row.entity.admin && row.entity._id !== $root.user._id'>Revoke admin</a>
            <a ng-click='grid.api.toggleAdmin(row.entity)' ng-if='!row.entity.admin && row.entity._id !== $root.user._id'>Grant admin</a>
        </li>
        <li>
            <a ng-click='grid.api.removeUser(row.entity)'>Remove user</a>
        </li>
</div>`;

const EMAIL_TEMPLATE = '<div class="ui-grid-cell-contents"><a ng-href="mailto:{{ COL_FIELD }}">{{ COL_FIELD }}</a></div>';

// Controller for Admin screen.
export default ['adminController', [
    '$rootScope', '$scope', '$http', '$q', '$state', '$filter', 'uiGridConstants', 'IgniteMessages', 'IgniteConfirm', 'User', 'IgniteNotebookData', 'IgniteCountries',
    ($rootScope, $scope, $http, $q, $state, $filter, uiGridConstants, Messages, Confirm, User, Notebook, Countries) => {
        $scope.users = null;

        const companySelectOptions = [];
        const countrySelectOptions = [];

        const COLUMNS_DEFS = [
            {displayName: 'Actions', cellTemplate: ACTIONS_TEMPLATE, field: 'test', minWidth: 80, width: 80, enableFiltering: false, enableSorting: false},
            {displayName: 'User', field: 'userName', minWidth: 65, enableFiltering: true, filter: { placeholder: 'Filter by name...' }},
            {displayName: 'Email', field: 'email', cellTemplate: EMAIL_TEMPLATE, minWidth: 160, enableFiltering: true, filter: { placeholder: 'Filter by email...' }},
            {displayName: 'Company', field: 'company', minWidth: 160, filter: {
                selectOptions: companySelectOptions, type: uiGridConstants.filter.SELECT, condition: uiGridConstants.filter.EXACT }
            },
            {displayName: 'Country', field: 'countryCode', minWidth: 80, filter: {
                selectOptions: countrySelectOptions, type: uiGridConstants.filter.SELECT, condition: uiGridConstants.filter.EXACT }
            },
            {displayName: 'Last login', field: 'lastLogin', cellFilter: 'date:"medium"', minWidth: 175, width: 175, enableFiltering: false, sort: { direction: 'desc', priority: 0 }},
            {displayName: 'Clusters count', headerCellTemplate: CLUSTER_HEADER_TEMPLATE, field: '_clusters', type: 'number', headerTooltip: 'Clusters count', minWidth: 50, width: 50, enableFiltering: false},
            {displayName: 'Models count', headerCellTemplate: MODEL_HEADER_TEMPLATE, field: '_models', type: 'number', headerTooltip: 'Models count', minWidth: 50, width: 50, enableFiltering: false},
            {displayName: 'Caches count', headerCellTemplate: CACHE_HEADER_TEMPLATE, field: '_caches', type: 'number', headerTooltip: 'Caches count', minWidth: 50, width: 50, enableFiltering: false},
            {displayName: 'IGFS count', headerCellTemplate: IGFS_HEADER_TEMPLATE, field: '_igfs', type: 'number', headerTooltip: 'IGFS count', minWidth: 50, width: 50, enableFiltering: false}
        ];

        const ctrl = $scope.ctrl = {};

        const becomeUser = function(user) {
            $http.get('/api/v1/admin/become', { params: {viewedUserId: user._id}})
                .then(() => User.load())
                .then(() => $state.go('base.configuration.clusters'))
                .then(() => Notebook.load())
                .catch(Messages.showError);
        };

        const removeUser = (user) => {
            Confirm.confirm(`Are you sure you want to remove user: "${user.userName}"?`)
                .then(() => {
                    $http.post('/api/v1/admin/remove', {userId: user._id})
                        .then(() => {
                            const i = _.findIndex($scope.users, (u) => u._id === user._id);

                            if (i >= 0)
                                $scope.users.splice(i, 1);

                            Messages.showInfo(`User has been removed: "${user.userName}"`);
                        })
                        .catch(({data, status}) => {
                            if (status === 503)
                                Messages.showInfo(data);
                            else
                                Messages.showError('Failed to remove user: ', data);
                        });
                });
        };

        const toggleAdmin = (user) => {
            if (user.adminChanging)
                return;

            user.adminChanging = true;

            $http.post('/api/v1/admin/save', {userId: user._id, adminFlag: !user.admin})
                .then(() => {
                    user.admin = !user.admin;

                    Messages.showInfo(`Admin right was successfully toggled for user: "${user.userName}"`);
                })
                .catch((res) => {
                    Messages.showError('Failed to toggle admin right for user: ', res);
                })
                .finally(() => user.adminChanging = false);
        };


        ctrl.gridOptions = {
            data: [],
            columnVirtualizationThreshold: 30,
            columnDefs: COLUMNS_DEFS,
            categories: [
                {name: 'Actions', visible: true, selectable: true},
                {name: 'User', visible: true, selectable: true},
                {name: 'Email', visible: true, selectable: true},
                {name: 'Company', visible: true, selectable: true},
                {name: 'Country', visible: true, selectable: true},
                {name: 'Last login', visible: true, selectable: true},

                {name: 'Clusters count', visible: true, selectable: true},
                {name: 'Models count', visible: true, selectable: true},
                {name: 'Caches count', visible: true, selectable: true},
                {name: 'IGFS count', visible: true, selectable: true}
            ],
            enableFiltering: true,
            enableRowSelection: false,
            enableRowHeaderSelection: false,
            enableColumnMenus: false,
            multiSelect: false,
            modifierKeysToMultiSelect: true,
            noUnselect: true,
            flatEntityAccess: true,
            fastWatch: true,
            onRegisterApi: (api) => {
                ctrl.gridApi = api;

                api.becomeUser = becomeUser;
                api.removeUser = removeUser;
                api.toggleAdmin = toggleAdmin;
            }
        };

        /**
         * Set grid height.
         *
         * @param {Number} rows Rows count.
         * @private
         */
        const adjustHeight = (rows) => {
            const height = Math.min(rows, 20) * 30 + 75;

            // Remove header height.
            ctrl.gridApi.grid.element.css('height', height + 'px');

            ctrl.gridApi.core.handleWindowResize();
        };

        const usersToFilterOptions = (column) => {
            return _.sortBy(
                _.map(
                    _.groupBy($scope.users, (usr) => {
                        const fld = usr[column];

                        return _.isNil(fld) ? fld : fld.toUpperCase();
                    }),
                    (arr, value) => ({label: `${_.head(arr)[column] || 'Not set'} (${arr.length})`, value})
                ),
                'value');
        };

        const _reloadUsers = () => {
            $http.post('/api/v1/admin/list')
                .then(({ data }) => {
                    $scope.users = data;

                    companySelectOptions.length = 0;
                    countrySelectOptions.length = 0;

                    _.forEach($scope.users, (user) => {
                        user.userName = user.firstName + ' ' + user.lastName;
                        user.countryCode = Countries.getByName(user.country).code;

                        user._clusters = user.counters.clusters;
                        user._models = user.counters.models;
                        user._caches = user.counters.caches;
                        user._igfs = user.counters.igfs;
                    });

                    companySelectOptions.push(...usersToFilterOptions('company'));
                    countrySelectOptions.push(...usersToFilterOptions('countryCode'));

                    $scope.ctrl.gridOptions.data = data;

                    adjustHeight(data.length);
                })
                .catch(Messages.showError);
        };

        _reloadUsers();

        const _enableColumns = (categories, visible) => {
            _.forEach(categories, (cat) => {
                cat.visible = visible;

                _.forEach(ctrl.gridOptions.columnDefs, (col) => {
                    if (col.displayName === cat.name)
                        col.visible = visible;
                });
            });

            ctrl.gridApi.grid.refresh();
        };

        const _selectableColumns = () => _.filter(ctrl.gridOptions.categories, (cat) => cat.selectable);

        ctrl.toggleColumns = (category, visible) => _enableColumns([category], visible);
        ctrl.selectAllColumns = () => _enableColumns(_selectableColumns(), true);
        ctrl.clearAllColumns = () => _enableColumns(_selectableColumns(), false);
    }
]];
