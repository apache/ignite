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

const USER_TEMPLATE = '<div class="ui-grid-cell-contents"><i class="pull-left" ng-class="row.entity.admin ? \'icon-admin\' : \'icon-user\'"></i>&nbsp;{{ COL_FIELD }}</div>';

const CLUSTER_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='icon-cluster'></i>${ICON_SORT}</div>`;
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
    {name: 'actions', displayName: 'Actions', categoryDisplayName: 'Actions', cellTemplate: ACTIONS_TEMPLATE, field: 'actions', minWidth: 70, width: 70, enableFiltering: false, enableSorting: false, visible: false},
    {name: 'user', displayName: 'User', categoryDisplayName: 'User', field: 'userName', cellTemplate: USER_TEMPLATE, minWidth: 160, enableFiltering: true, pinnedLeft: true, filter: { placeholder: 'Filter by name...' }},
    {name: 'email', displayName: 'Email', categoryDisplayName: 'Email', field: 'email', cellTemplate: EMAIL_TEMPLATE, minWidth: 160, enableFiltering: false, filter: { placeholder: 'Filter by email...' }},
    {name: 'company', displayName: 'Company', categoryDisplayName: 'Company', field: 'company', minWidth: 180, enableFiltering: true, filter: { placeholder: 'Filter by company...' }},
    {name: 'country', displayName: 'Country', categoryDisplayName: 'Country', field: 'countryCode', minWidth: 160, enableFiltering: true, filter: { placeholder: 'Filter by country...' }},
    {name: 'lastlogin', displayName: 'Last login', categoryDisplayName: 'Last login', field: 'lastLogin', cellFilter: 'date:"M/d/yy HH:mm"', minWidth: 120, width: 120, enableFiltering: false, visible: false},
    {name: 'lastactivity', displayName: 'Last activity', categoryDisplayName: 'Last activity', field: 'lastActivity', cellFilter: 'date:"M/d/yy HH:mm"', minWidth: 130, width: 130, enableFiltering: false, visible: true, sort: { direction: 'desc', priority: 0 }},
    // Configurations
    {name: 'cfg_clusters', displayName: 'Clusters count', categoryDisplayName: 'Configurations', headerCellTemplate: CLUSTER_HEADER_TEMPLATE, field: 'counters.clusters', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Clusters count', minWidth: 65, width: 65, enableFiltering: false, visible: false},
    {name: 'cfg_models', displayName: 'Models count', categoryDisplayName: 'Configurations', headerCellTemplate: MODEL_HEADER_TEMPLATE, field: 'counters.models', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Models count', minWidth: 65, width: 65, enableFiltering: false, visible: false},
    {name: 'cfg_caches', displayName: 'Caches count', categoryDisplayName: 'Configurations', headerCellTemplate: CACHE_HEADER_TEMPLATE, field: 'counters.caches', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Caches count', minWidth: 65, width: 65, enableFiltering: false, visible: false},
    {name: 'cfg_igfs', displayName: 'IGFS count', categoryDisplayName: 'Configurations', headerCellTemplate: IGFS_HEADER_TEMPLATE, field: 'counters.igfs', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'IGFS count', minWidth: 65, width: 65, enableFiltering: false, visible: false},
    // Activities Total
    {name: 'cfg', displayName: 'Cfg', categoryDisplayName: 'Total activities', field: 'activitiesTotal["configuration"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of configuration usages', minWidth: 70, width: 70, enableFiltering: false},
    {name: 'qry', displayName: 'Qry', categoryDisplayName: 'Total activities', field: 'activitiesTotal["queries"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of queries usages', minWidth: 70, width: 70, enableFiltering: false},
    {name: 'demo', displayName: 'Demo', categoryDisplayName: 'Total activities', field: 'activitiesTotal["demo"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of demo startup', minWidth: 85, width: 85, enableFiltering: false},
    {name: 'dnld', displayName: 'Dnld', categoryDisplayName: 'Total activities', field: 'activitiesDetail["/agent/download"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of agent downloads', minWidth: 80, width: 80, enableFiltering: false},
    {name: 'starts', displayName: 'Starts', categoryDisplayName: 'Total activities', field: 'activitiesDetail["/agent/start"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of agent startup', minWidth: 87, width: 87, enableFiltering: false},
    // Activities Configuration
    {name: 'clusters', displayName: 'Clusters', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/clusters"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 100, enableFiltering: false, visible: false},
    {name: 'model', displayName: 'Model', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/domains"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration model', minWidth: 87, width: 87, enableFiltering: false, visible: false},
    {name: 'caches', displayName: 'Caches', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/caches"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration caches', minWidth: 96, width: 96, enableFiltering: false, visible: false},
    {name: 'igfs', displayName: 'IGFS', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/igfs"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration IGFS', minWidth: 85, width: 85, enableFiltering: false, visible: false},
    {name: 'summary', displayName: 'Summary', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/summary"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration summary', minWidth: 111, width: 111, enableFiltering: false, visible: false},
    // Activities Queries
    {name: 'execute', displayName: 'Execute', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/execute"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Query executions', minWidth: 98, width: 98, enableFiltering: false, visible: false},
    {name: 'explain', displayName: 'Explain', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/explain"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Query explain executions', minWidth: 95, width: 95, enableFiltering: false, visible: false},
    {name: 'scan', displayName: 'Scan', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/scan"] || 0', type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Scan query executions', minWidth: 80, width: 80, enableFiltering: false, visible: false}
];
