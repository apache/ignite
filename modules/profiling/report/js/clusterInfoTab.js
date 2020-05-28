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

$('#nodesTable').bootstrapTable({
    pagination: true,
    search: true,
    columns: [{
        field: 'name',
        title: 'Node name',
        sortable: true
    }, {
        field: 'verson',
        title: 'Node verson',
        sortable: true
    }, {
        field: 'nodeId',
        title: 'Node ID',
        sortable: true
    }],
    data: prepareNodesTableData(),
    sortName: 'name',
    sortOrder: 'desc'
})

function prepareNodesTableData() {
    var data = [];

    $.each(REPORT_DATA.clusterInfo.nodes, function (nodeId, node) {
        data.push({
            "nodeId": nodeId,
            "name": node.name,
            "verson": node.verson
        });
    });

    return data;
}

$('#cachesTable').bootstrapTable({
    pagination: true,
    search: true,
    columns: [{
        field: 'cacheName',
        title: 'Cache name',
        sortable: true
    }, {
        field: 'groupName',
        title: 'Group name',
        sortable: true
    }, {
        field: 'startTime',
        title: 'Start time',
        sortable: true
    }, {
        field: 'userCache',
        title: 'User cache',
        sortable: true
    }, {
        field: 'cacheId',
        title: 'Cache ID',
        sortable: true
    }],
    data: prepareCachesTableData(),
    sortName: 'cacheName',
    sortOrder: 'desc'
})

function prepareCachesTableData() {
    var data = [];

    $.each(REPORT_DATA.clusterInfo.caches, function (cacheId, cache) {
        data.push({
            "cacheId": cacheId,
            "startTime": new Date(cache.startTime),
            "cacheName": cache.cacheName,
            "groupName": cache.groupName,
            "userCache": cache.userCache
        });
    });

    return data;
}
