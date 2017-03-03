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

const USER_TEMPLATE = '<div class="ui-grid-cell-contents"><i class="pull-left" ng-class="row.entity.admin ? \'icon-admin\' : \'icon-user\'"></i>{{ COL_FIELD }}</div>';

const CLUSTER_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-sitemap'></i>${ICON_SORT}</div>`;
const MODEL_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-object-group'></i>${ICON_SORT}</div>`;
const CACHE_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-database'></i>${ICON_SORT}</div>`;
const IGFS_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-folder-o'></i>${ICON_SORT}</div>`;

const ACTIONS_TEMPLATE = `
<div class='text-center ui-grid-cell-actions'>
    <a class='btn btn-default dropdown-toggle' bs-dropdown='' data-placement='bottom-right' data-container='.panel'>
        <i class='fa fa-gear'></i>&nbsp;
        <span class='caret'></span>
    </a>
    <ul class='dropdown-menu' role='menu'>
        <li ng-show='row.entity._id != $root.user._id'>
            <a ng-click='grid.api.becomeUser(row.entity)'>Become this user</a>
        </li>
        <li ng-show='row.entity._id != $root.user._id'>
            <a ng-click='grid.api.toggleAdmin(row.entity)' ng-if='row.entity.admin && row.entity._id !== $root.user._id'>Revoke admin</a>
            <a ng-click='grid.api.toggleAdmin(row.entity)' ng-if='!row.entity.admin && row.entity._id !== $root.user._id'>Grant admin</a>
        </li>
        <li ng-show='row.entity._id != $root.user._id'>
            <a ng-click='grid.api.removeUser(row.entity)'>Remove user</a>
        </li>
        <li>
            <a ng-click='grid.api.showActivities(row.entity)'>Activity detail</a>
        </li>
</div>`;

const EMAIL_TEMPLATE = '<div class="ui-grid-cell-contents"><a ng-href="mailto:{{ COL_FIELD }}">{{ COL_FIELD }}</a></div>';

export default [
    {displayName: 'Actions', categoryDisplayName: 'Actions', cellTemplate: ACTIONS_TEMPLATE, field: 'actions', minWidth: 65, width: 65, enableFiltering: false, enableSorting: false, pinnedLeft: true},
    {displayName: 'User', categoryDisplayName: 'User', field: 'userName', cellTemplate: USER_TEMPLATE, minWidth: 160, enableFiltering: true, filter: { placeholder: 'Filter by name...' }, pinnedLeft: true},
    {displayName: 'Email', categoryDisplayName: 'Email', field: 'email', cellTemplate: EMAIL_TEMPLATE, minWidth: 160, enableFiltering: true, filter: { placeholder: 'Filter by email...' }},
    {displayName: 'Company', categoryDisplayName: 'Company', field: 'company', minWidth: 160, enableFiltering: true},
    {displayName: 'Country', categoryDisplayName: 'Country', field: 'countryCode', minWidth: 80, enableFiltering: true},
    {displayName: 'Last login', categoryDisplayName: 'Last login', field: 'lastLogin', cellFilter: 'date:"M/d/yy HH:mm"', minWidth: 105, width: 105, enableFiltering: false, visible: false},
    {displayName: 'Last activity', categoryDisplayName: 'Last activity', field: 'lastActivity', cellFilter: 'date:"M/d/yy HH:mm"', minWidth: 105, width: 105, enableFiltering: false, visible: true, sort: { direction: 'desc', priority: 0 }},
    // Configurations
    {displayName: 'Clusters count', categoryDisplayName: 'Configurations', headerCellTemplate: CLUSTER_HEADER_TEMPLATE, field: 'counters.clusters', type: 'number', headerTooltip: 'Clusters count', minWidth: 50, width: 50, enableFiltering: false, visible: false},
    {displayName: 'Models count', categoryDisplayName: 'Configurations', headerCellTemplate: MODEL_HEADER_TEMPLATE, field: 'counters.models', type: 'number', headerTooltip: 'Models count', minWidth: 50, width: 50, enableFiltering: false, visible: false},
    {displayName: 'Caches count', categoryDisplayName: 'Configurations', headerCellTemplate: CACHE_HEADER_TEMPLATE, field: 'counters.caches', type: 'number', headerTooltip: 'Caches count', minWidth: 50, width: 50, enableFiltering: false, visible: false},
    {displayName: 'IGFS count', categoryDisplayName: 'Configurations', headerCellTemplate: IGFS_HEADER_TEMPLATE, field: 'counters.igfs', type: 'number', headerTooltip: 'IGFS count', minWidth: 50, width: 50, enableFiltering: false, visible: false},
    // Activities Total
    {displayName: 'Cfg', categoryDisplayName: 'Total activities', field: 'activitiesTotal["configuration"] || 0', type: 'number', headerTooltip: 'Total count of configuration usages', minWidth: 50, width: 50, enableFiltering: false},
    {displayName: 'Qry', categoryDisplayName: 'Total activities', field: 'activitiesTotal["queries"] || 0', type: 'number', headerTooltip: 'Total count of queries usages', minWidth: 50, width: 50, enableFiltering: false},
    {displayName: 'Demo', categoryDisplayName: 'Total activities', field: 'activitiesTotal["demo"] || 0', type: 'number', headerTooltip: 'Total count of demo startup', minWidth: 60, width: 60, enableFiltering: false},
    {displayName: 'Dnld', categoryDisplayName: 'Total activities', field: 'activitiesDetail["/agent/download"] || 0', type: 'number', headerTooltip: 'Total count of agent downloads', minWidth: 55, width: 55, enableFiltering: false},
    {displayName: 'Starts', categoryDisplayName: 'Total activities', field: 'activitiesDetail["/agent/start"] || 0', type: 'number', headerTooltip: 'Total count of agent startup', minWidth: 60, width: 60, enableFiltering: false},
    // Activities Configuration
    {displayName: 'Clusters', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/clusters"] || 0', type: 'number', headerTooltip: 'Configuration clusters', minWidth: 50, width: 80, enableFiltering: false, visible: false},
    {displayName: 'Model', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/domains"] || 0', type: 'number', headerTooltip: 'Configuration model', minWidth: 50, width: 80, enableFiltering: false, visible: false},
    {displayName: 'Caches', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/caches"] || 0', type: 'number', headerTooltip: 'Configuration caches', minWidth: 50, width: 80, enableFiltering: false, visible: false},
    {displayName: 'IGFS', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/igfs"] || 0', type: 'number', headerTooltip: 'Configuration IGFS', minWidth: 50, width: 80, enableFiltering: false, visible: false},
    {displayName: 'Summary', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/summary"] || 0', type: 'number', headerTooltip: 'Configuration summary', minWidth: 50, width: 80, enableFiltering: false, visible: false},
    // Activities Queries
    {displayName: 'Execute', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/execute"] || 0', type: 'number', headerTooltip: 'Query executions', minWidth: 50, width: 80, enableFiltering: false, visible: false},
    {displayName: 'Explain', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/explain"] || 0', type: 'number', headerTooltip: 'Query explain executions', minWidth: 50, width: 80, enableFiltering: false, visible: false},
    {displayName: 'Scan', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/scan"] || 0', type: 'number', headerTooltip: 'Scan query executions', minWidth: 50, width: 80, enableFiltering: false, visible: false}
];
