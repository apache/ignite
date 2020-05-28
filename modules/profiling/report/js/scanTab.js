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

$('#scanStatisticsTable').bootstrapTable({
    pagination: true,
    search: true,
    columns: [{
        field: 'cacheName',
        title: 'Cache name',
        sortable: true
    }, {
        field: 'count',
        title: 'Executions',
        sortable: true
    }, {
        field: 'duration',
        title: 'Total duration, ms',
        sortable: true
    }, {
        field: 'logicalReads',
        title: 'Logical reads',
        sortable: true
    }, {
        field: 'physicalReads',
        title: 'Physical reads',
        sortable: true
    }, {
        field: 'failures',
        title: 'Failures count',
        sortable: true
    }],
    data: prepareScanTableData(),
    sortName: 'duration',
    sortOrder: 'desc'
})

function prepareScanTableData() {
    var data = [];

    $.each(REPORT_DATA.scan, function (cacheName, sqlData) {
        data.push({
            "cacheName": cacheName,
            "count": numberWithCommas(sqlData["count"]),
            "duration": numberWithCommas(sqlData["duration"]),
            "logicalReads": numberWithCommas(sqlData["logicalReads"]),
            "physicalReads": numberWithCommas(sqlData["physicalReads"]),
            "failures": numberWithCommas(sqlData["failures"])
        });
    });

    return data;
}

$('#topSlowScanTable').bootstrapTable({
    pagination: true,
    search: true,
    columns: [{
        field: 'text',
        title: 'Query text',
        sortable: true
    }, {
        field: 'duration',
        title: 'Duration, ms',
        sortable: true,
        sortOrder: 'desc'
    }, {
        field: 'startTime',
        title: 'Start time',
        sortable: true
    }, {
        field: 'nodeId',
        title: 'Originating node id',
        sortable: true
    }, {
        field: 'logicalReads',
        title: 'Logical reads',
        sortable: true
    }, {
        field: 'physicalReads',
        title: 'Physical reads',
        sortable: true
    }, {
        field: 'success',
        title: 'Success',
        sortable: true
    }],
    data: prepareSlowScanTableData(),
    sortName: 'duration',
    sortOrder: 'desc'
})

function prepareSlowScanTableData() {
    var data = [];

    $.each(REPORT_DATA.topSlowScan, function (key, sqlData) {
        data.push({
            text: sqlData["text"],
            duration: numberWithCommas(sqlData["duration"]),
            startTime: new Date(sqlData["startTime"]),
            nodeId: sqlData["nodeId"],
            logicalReads: numberWithCommas(sqlData["logicalReads"]),
            physicalReads: numberWithCommas(sqlData["physicalReads"]),
            success: sqlData["success"]
        });
    });

    return data;
}
