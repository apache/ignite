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

export default [
    {
        name: 'name',
        displayName: 'Cluster',
        field: 'name',
        cellTemplate: `
            <div class='ui-grid-cell-contents'>
                <cluster-security-icon
                    secured='row.entity.secured'
                ></cluster-security-icon>
                {{ COL_FIELD }}
            </div>
        `,
        width: 240,
        minWidth: 240
    },
    {
        name: 'nids',
        displayName: 'Number of Nodes',
        field: 'nids.length',
        cellClass: 'ui-grid-number-cell',
        width: 160,
        minWidth: 160
    },
    {
        name: 'status',
        displayName: 'Status',
        field: 'active',
        cellTemplate: `
            <div class='ui-grid-cell-contents ui-grid-cell--status'>
                <connected-clusters-cell-status
                    status='COL_FIELD'
                ></connected-clusters-cell-status>
                <connected-clusters-cell-logout
                    ng-if='row.entity.secured && grid.appScope.$ctrl.agentMgr.hasCredentials(row.entity.id)'
                    cluster-id='row.entity.id'
                ></connected-clusters-cell-logout>
            </div>
        `,
        minWidth: 140
    }
];
