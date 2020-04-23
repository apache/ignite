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

const USER_TEMPLATE = '<div class="ui-grid-cell-contents user-cell">' +
    '<i class="pull-left" ng-class="row.entity.admin ? \'icon-admin\' : \'icon-user\'"></i>&nbsp;<label bs-tooltip data-title="{{ COL_FIELD }}">{{ COL_FIELD }}</label></div>';

const CLUSTER_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='icon-cluster'></i>${ICON_SORT}</div>`;
const MODEL_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-object-group'></i>${ICON_SORT}</div>`;
const CACHE_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-database'></i>${ICON_SORT}</div>`;
const IGFS_HEADER_TEMPLATE = `<div class='ui-grid-cell-contents' bs-tooltip data-title='{{ col.headerTooltip(col) }}' data-placement='top'><i class='fa fa-folder-o'></i>${ICON_SORT}</div>`;

const EMAIL_TEMPLATE = '<div class="ui-grid-cell-contents"><a bs-tooltip data-title="{{ COL_FIELD }}" ng-href="mailto:{{ COL_FIELD }}">{{ COL_FIELD }}</a></div>';
const DATE_WITH_TITLE = '<div class="ui-grid-cell-contents"><label bs-tooltip data-title="{{ COL_FIELD | date:\'M/d/yy HH:mm\' }}">{{ COL_FIELD | date:"M/d/yy HH:mm" }}</label></div>';
const VALUE_WITH_TITLE = '<div class="ui-grid-cell-contents"><label bs-tooltip data-title="{{ COL_FIELD }}">{{ COL_FIELD }}</label></div>';

export default [
    {name: 'user', enableHiding: false, displayName: 'User', categoryDisplayName: 'User', field: 'userName', cellTemplate: USER_TEMPLATE, minWidth: 160, enableFiltering: true, pinnedLeft: true, filter: { placeholder: 'Filter by name...' }},
    {name: 'email', displayName: 'Email', categoryDisplayName: 'Email', field: 'email', cellTemplate: EMAIL_TEMPLATE, minWidth: 160, width: 220, enableFiltering: true, filter: { placeholder: 'Filter by email...' }},
    {name: 'activated', displayName: 'Activated', categoryDisplayName: 'Activated', field: 'activated', width: 220, enableFiltering: true, filter: { placeholder: 'Filter by activation...' }, visible: false},
    {name: 'company', displayName: 'Company', categoryDisplayName: 'Company', field: 'company', cellTemplate: VALUE_WITH_TITLE, minWidth: 180, enableFiltering: true, filter: { placeholder: 'Filter by company...' }},
    {name: 'country', displayName: 'Country', categoryDisplayName: 'Country', field: 'countryCode', cellTemplate: VALUE_WITH_TITLE, minWidth: 160, enableFiltering: true, filter: { placeholder: 'Filter by country...' }},
    {name: 'lastlogin', displayName: 'Last login', categoryDisplayName: 'Last login', field: 'lastLogin', cellTemplate: DATE_WITH_TITLE, minWidth: 135, width: 135, enableFiltering: false, visible: false},
    {name: 'lastactivity', displayName: 'Last activity', categoryDisplayName: 'Last activity', field: 'lastActivity', cellTemplate: DATE_WITH_TITLE, minWidth: 135, width: 145, enableFiltering: false, visible: true, sort: { direction: 'desc', priority: 0 }},
    // Configurations
    {name: 'cfg_clusters', displayName: 'Clusters count', categoryDisplayName: 'Configurations', headerCellTemplate: CLUSTER_HEADER_TEMPLATE, field: 'counters.clusters', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Clusters count', minWidth: 65, width: 65, enableFiltering: false, visible: false},
    {name: 'cfg_models', displayName: 'Models count', categoryDisplayName: 'Configurations', headerCellTemplate: MODEL_HEADER_TEMPLATE, field: 'counters.models', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Models count', minWidth: 65, width: 65, enableFiltering: false, visible: false},
    {name: 'cfg_caches', displayName: 'Caches count', categoryDisplayName: 'Configurations', headerCellTemplate: CACHE_HEADER_TEMPLATE, field: 'counters.caches', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Caches count', minWidth: 65, width: 65, enableFiltering: false, visible: false},
    {name: 'cfg_igfs', displayName: 'IGFS count', categoryDisplayName: 'Configurations', headerCellTemplate: IGFS_HEADER_TEMPLATE, field: 'counters.igfs', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'IGFS count', minWidth: 65, width: 65, enableFiltering: false, visible: false},
    // Activities Total
    {name: 'cfg', displayName: 'Cfg', categoryDisplayName: 'Total activities', field: 'activitiesTotal["configuration"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of configuration usages', minWidth: 70, width: 70, enableFiltering: false},
    {name: 'qry', displayName: 'Qry', categoryDisplayName: 'Total activities', field: 'activitiesTotal["queries"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of queries usages', minWidth: 70, width: 70, enableFiltering: false},
    {name: 'demo', displayName: 'Demo', categoryDisplayName: 'Total activities', field: 'activitiesTotal["demo"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of demo startup', minWidth: 85, width: 85, enableFiltering: false},
    {name: 'dnld', displayName: 'Dnld', categoryDisplayName: 'Total activities', field: 'activitiesDetail["/agent/download"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of agent downloads', minWidth: 80, width: 80, enableFiltering: false},
    {name: 'starts', displayName: 'Starts', categoryDisplayName: 'Total activities', field: 'activitiesDetail["/agent/start"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Total count of agent startup', minWidth: 87, width: 87, enableFiltering: false},
    // Activities Configuration
    {name: 'clusters', displayName: 'Clusters', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.overview"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 100, enableFiltering: false, visible: false},
    {name: 'clusterBasic', displayName: 'Basic', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.edit.basic"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 100, enableFiltering: false, visible: false},
    {name: 'clusterBasicNew', displayName: 'Basic create', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/new/basic"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 150, enableFiltering: false, visible: false},
    {name: 'clusterAdvancedNew', displayName: 'Adv. Cluster create', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["/configuration/new/advanced/cluster"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 170, enableFiltering: false, visible: false},
    {name: 'clusterAdvancedCluster', displayName: 'Adv. Cluster edit', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.edit.advanced.cluster"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 150, enableFiltering: false, visible: false},
    {name: 'clusterAdvancedCaches', displayName: 'Adv. Caches', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.edit.advanced.caches"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 150, enableFiltering: false, visible: false},
    {name: 'clusterAdvancedCache', displayName: 'Adv. Cache edit', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.edit.advanced.caches.cache"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 150, enableFiltering: false, visible: false},
    {name: 'clusterAdvancedModels', displayName: 'Adv. Models', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.edit.advanced.models"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 150, enableFiltering: false, visible: false},
    {name: 'clusterAdvancedModel', displayName: 'Adv. Model edit', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.edit.advanced.models.model"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 150, enableFiltering: false, visible: false},
    {name: 'clusterAdvancedIGFSs', displayName: 'Adv. IGFSs', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.edit.advanced.igfs"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 150, enableFiltering: false, visible: false},
    {name: 'clusterAdvancedIGFS', displayName: 'Adv. IGFS edit', categoryDisplayName: 'Configuration\'s activities', field: 'activitiesDetail["base.configuration.edit.advanced.igfs.igfs"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Configuration clusters', minWidth: 100, width: 150, enableFiltering: false, visible: false},
    // Activities Queries
    {name: 'execute', displayName: 'Execute', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/execute"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Query executions', minWidth: 98, width: 98, enableFiltering: false, visible: false},
    {name: 'explain', displayName: 'Explain', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/explain"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Query explain executions', minWidth: 95, width: 95, enableFiltering: false, visible: false},
    {name: 'scan', displayName: 'Scan', categoryDisplayName: 'Queries\' activities', field: 'activitiesDetail["/queries/scan"] || 0', cellTemplate: VALUE_WITH_TITLE, type: 'number', cellClass: 'ui-grid-number-cell', headerTooltip: 'Scan query executions', minWidth: 80, width: 80, enableFiltering: false, visible: false}
];
